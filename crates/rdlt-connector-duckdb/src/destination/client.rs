//! THE duckdb-rs boundary: the shared database handle, per-session
//! setup replay, and the error-classification rulebook. Library types
//! stop at this module's edge.
//!
//! One `Connection` is opened per destination and every session CLONES
//! from it — two independent `Connection::open`s on the same file are
//! two database instances, and the second cannot see the first's
//! un-checkpointed catalog. A cloned connection inherits neither
//! session-scoped `SET`s nor `LOAD`s, so the recorded setup statements
//! replay on every clone; the replay is what configures the clone that
//! performs the load at all — every `SET` and `LOAD` would otherwise
//! bind only to the idle builder connection.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use duckdb::Connection;
use rdlt_connector_sdk::spi::DestinationError;

/// Canonical paths held open READ-WRITE by this process, one entry per
/// live [`Db`]. Read-only oracles (`testhook`) open the library
/// directly and are deliberately exempt — a read-only open replays the
/// WAL without touching it.
fn open_paths() -> &'static Mutex<HashSet<PathBuf>> {
    static OPEN: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
    OPEN.get_or_init(|| Mutex::new(HashSet::new()))
}

/// One registry claim, released on drop. Every clone of a [`Db`]
/// shares the one claim through its `Arc`, so the path frees when the
/// LAST clone goes — a sequential re-open stays legal.
#[derive(Debug)]
struct Claim(PathBuf);

impl Drop for Claim {
    fn drop(&mut self) {
        if let Ok(mut open) = open_paths().lock() {
            open.remove(&self.0);
        }
    }
}

/// The double-open refusal (031 review N2, measured): a second
/// read-write instance in this process replays AND TRUNCATES the live
/// instance's WAL at open, silently swallowing every commit the first
/// makes afterwards.
fn double_open_refusal(path: &Path) -> DestinationError {
    DestinationError::fatal(format!(
        "duckdb database `{}` is already open in this process — a second \
         instance cannot see the first's writes and a read-write open \
         would truncate its WAL; share the one destination",
        path.display()
    ))
}

/// One setup statement, recorded for replay-per-clone.
#[derive(Debug, Clone)]
enum Setup {
    /// `SET {key}='{value}'` — value escaped by `'` doubling; the key
    /// passed the bare-identifier gate at validation.
    Setting { key: String, value: String },
    /// `LOAD {name}` — name passed the bare-identifier gate.
    Extension { name: String },
}

impl Setup {
    fn render(&self) -> String {
        match self {
            Setup::Setting { key, value } => {
                format!("SET {key}='{}'", value.replace('\'', "''"))
            }
            Setup::Extension { name } => format!("LOAD {name}"),
        }
    }

    fn describe(&self) -> String {
        match self {
            Setup::Setting { key, .. } => format!("duckdb setting `{key}`"),
            Setup::Extension { name } => format!("duckdb extension `{name}`"),
        }
    }
}

/// The shared database instance.
#[derive(Clone)]
pub(crate) struct Db {
    conn: Arc<Mutex<Connection>>,
    setup: Arc<Vec<Setup>>,
    /// Held for its Drop alone: the registry entry that refuses a
    /// second in-process open of the same file.
    _claim: Arc<Claim>,
}

impl std::fmt::Debug for Db {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Db").finish_non_exhaustive()
    }
}

impl Db {
    /// Open (or create) the database file and apply every setup
    /// statement EAGERLY — a bad key or value errors here, at
    /// connect, not later inside a session.
    pub(crate) fn connect(
        path: &std::path::Path,
        settings: impl IntoIterator<Item = (String, String)>,
        extensions: impl IntoIterator<Item = String>,
    ) -> Result<Self, DestinationError> {
        // The WHOLE check-open-claim sequence holds the registry lock:
        // the second read-write `Connection::open` is itself the
        // WAL-truncating act, so the refusal must come before it —
        // and without the lock held ACROSS the open, a second connect
        // could pass the check, sleep, and open long after the first
        // claimed and committed (the TOCTOU the 031 /code-review round
        // traced). Opens are rare and take milliseconds; serializing
        // them is free. `Claim::drop` takes this same lock, but no
        // claim is ever dropped while `connect` holds it.
        let claim = {
            let mut open = open_paths()
                .lock()
                .map_err(|_| DestinationError::fatal("open-path registry poisoned"))?;
            // The file may not exist yet (first ever open) — then
            // nobody can hold it and the pre-check is moot; the
            // post-open canonicalize below is the definitive spelling.
            if let Ok(canonical) = path.canonicalize()
                && open.contains(&canonical)
            {
                return Err(double_open_refusal(path));
            }
            // Transient here reaches the engine's retry budget only
            // through the SESSION path; through `assemble` it survives
            // as rendered text in the ConfigError (pinned). An
            // I/O-pressured file is the environment's problem; a file
            // another PROCESS holds is the operator's (D-042-2, the
            // lock-family fatal in `classify`) — either way not a
            // config defect to rewrite.
            let conn = Connection::open(path).map_err(classify)?;
            // Canonicalize AFTER the open — the open creates the file,
            // and only the canonical spelling makes two documents
            // naming one file through different paths collide.
            let canonical = path.canonicalize().map_err(|e| {
                DestinationError::fatal(format!("cannot canonicalize `{}`: {e}", path.display()))
            })?;
            if !open.insert(canonical.clone()) {
                return Err(double_open_refusal(path));
            }
            (conn, Arc::new(Claim(canonical)))
        };
        let (conn, claim) = claim;
        // Settings are recorded (and applied) BEFORE extensions
        // deliberately: probed benign for bundled builds, and it keeps
        // limits like memory_limit in force before any LOAD runs.
        let mut setup = Vec::new();
        for (key, value) in settings {
            setup.push(Setup::Setting { key, value });
        }
        for name in extensions {
            setup.push(Setup::Extension { name });
        }
        for statement in &setup {
            conn.execute_batch(&statement.render())
                .map_err(|e| DestinationError::fatal(format!("{}: {e}", statement.describe())))?;
        }
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            setup: Arc::new(setup),
            _claim: claim,
        })
    }

    /// A NEW session connection: cloned from the shared instance with
    /// the recorded setup replayed.
    pub(crate) fn session(&self) -> Result<Connection, DestinationError> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| DestinationError::fatal("connection poisoned"))?
            .try_clone()
            .map_err(classify)?;
        for statement in self.setup.iter() {
            conn.execute_batch(&statement.render())
                .map_err(|e| DestinationError::fatal(format!("{}: {e}", statement.describe())))?;
        }
        Ok(conn)
    }
}

/// The classification rulebook. DuckDB's C API reports NO structured
/// error category (the crate's probe pins `ErrorCode::Unknown`), so
/// the transient key is a stable message prefix: `"IO Error"` covers
/// resource pressure — which heals on retry, because the message text
/// names the operating condition, not the operation. Everything else
/// is fatal.
///
/// A CARVE-OUT under that prefix (037 US6 / Task 19, S5) catches TWO
/// sub-cases that are deterministic instead: a missing parent
/// directory or a permission-denied path never heals on retry either —
/// no amount of retry budget opens a path that structurally cannot
/// open — so a run that treated them as transient would retry forever
/// and report a false "still trying" instead of failing loud.
///
/// Both render through the SAME open-failure template in duckdb
/// 1.5.x's own C++ source (`local_file_system.cpp`):
/// `"Cannot open file \"%s\": %s"`, with `strerror(errno)` filling the
/// `%s`. That ONE template also carries every OTHER `open()` errno —
/// `EMFILE`/`ENFILE` ("Too many open files"), create-time `ENOSPC`
/// ("No space left on device"), `EINTR`, and more — all of which DO
/// heal on retry, so the template alone cannot be the key: the carve
/// requires the template fragment `"Cannot open file"` AND one of the
/// two SUFFIXES actually probed (`test_probes.rs`,
/// `probe_deterministic_io_message_spellings` +
/// `..._permission_denied`): `"No such file or directory"` or
/// `"Permission denied"`. An errno this crate has not measured through
/// the template — even one that IS genuinely deterministic, like a
/// second `EEXIST`-shaped race — deliberately STAYS transient: a
/// retry-forever on an unmeasured deterministic case is the recorded
/// lesser evil against fatal-ing a healable one on a guess. Extend the
/// suffix list only from a new probe pin, never from reasoning about
/// what `strerror` might say.
///
/// The lock-conflict family (D-042-2) is a SECOND fatal carve, its own
/// template — `"Could not set lock on file \"%s\": %s"` — measured
/// live from a second process's read-write open (042 Task 6; the spawn
/// suite's cross-process cell re-measures it on every run):
/// `IO Error: Could not set lock on file "…": Conflicting lock is held
/// in <program> (PID …) by user …. See also
/// https://duckdb.org/docs/stable/connect/concurrency`. FATAL, because
/// a lock conflict is deterministic from inside one run: a second
/// read-write open of a single-writer file is an operator error (two
/// processes pointed at one database), and the holder keeps the lock
/// for its whole life — a retry budget spent against it reports a
/// false "still trying" where the honest answer is "share the one
/// destination". The same measurement showed a READ-ONLY open from a
/// second process is refused with the SAME message while a read-write
/// holder lives, so no sub-case of the template heals on retry either.
/// The template's `%s` tail is USUALLY `AdditionalProcessInfo` naming
/// the PID holding the lock, not `strerror`; the source only falls
/// back to `strerror(errno)` there if the diagnostic `fcntl(F_GETLK)`
/// call itself fails. Either filling can in principle contain either
/// open-carve suffix string, which is exactly why the open carve does
/// not key on the suffixes ALONE — it requires the open-template
/// fragment `"Cannot open file"` too, which the lock template never
/// renders.
///
/// Classification is UNIFORM across the whole load path (031 review
/// S2/A3): the receipt probe, the load-committed probe, and
/// `read_state`'s read arm all route here too — those reads are
/// idempotent, so a locked-file IO error there deserves the retry
/// budget, not a run abort. Only `read_state`'s serde-parse arm stays
/// unconditionally fatal (a corrupt document never heals on retry).
pub(crate) fn classify(e: duckdb::Error) -> DestinationError {
    if let duckdb::Error::DuckDBFailure(_, Some(message)) = &e
        && message.starts_with("IO Error")
    {
        // Deterministic sub-cases never heal on retry; both the
        // template fragment AND the suffix are probe-pinned
        // (test_probes), not guessed — an unmeasured suffix through
        // the same template (EMFILE, ENOSPC, EINTR, ...) stays
        // transient rather than being fatal-ed on a guess.
        const OPEN_TEMPLATE: &str = "Cannot open file";
        const MEASURED_SUFFIXES: &[&str] = &["No such file or directory", "Permission denied"];
        if message.contains(OPEN_TEMPLATE) && MEASURED_SUFFIXES.iter().any(|s| message.contains(s))
        {
            return DestinationError::fatal(e.to_string());
        }
        // The lock-conflict family is FATAL — the owner ruling
        // D-042-2, its full weighing carried here so the trade is
        // auditable in place:
        //
        // - MEASURED, not guessed: the fragment is the template's own
        //   head (the tail names the holding PID and varies), and the
        //   042 T6 probes measured that duckdb's cross-process lock
        //   refuses even a READ-ONLY open while a read-write holder
        //   lives — so a conflict means a live holder, full stop.
        // - THE ASYMMETRY that decided it: a holder normally keeps the
        //   lock for its whole life, and under a transient
        //   classification the engine retries the WHOLE run five times
        //   against a lock that will not clear — minutes of doomed
        //   re-extraction ending in the same failure, with the real
        //   cause (two writers configured onto one store) buried under
        //   retry noise.
        // - THE BRIEF-HOLDER CASE was weighed and DECLINED by the
        //   owner: a holder in its final milliseconds of teardown (a
        //   previous run exiting, an operator's CLI session closing)
        //   would heal on retry, but distinguishing it from a durable
        //   holder is not possible from the message, and trading a
        //   loud immediate refusal for sometimes-heals means the
        //   two-writers misconfiguration is discovered five retries
        //   late. Orchestrators serialize runs; the refusal names the
        //   conflict at first contact.
        const LOCK_TEMPLATE: &str = "Could not set lock on file";
        if message.contains(LOCK_TEMPLATE) {
            return DestinationError::fatal(e.to_string());
        }
        return DestinationError::transient(e.to_string());
    }
    DestinationError::fatal(e.to_string())
}

/// The duplicate-merge-key diagnosis key, checked on the LIBRARY
/// error BEFORE wrapping — a message that merely mentions violations
/// (a table name, a quoted value) can never be misdiagnosed.
pub(crate) fn is_constraint_violation(e: &duckdb::Error) -> bool {
    matches!(e, duckdb::Error::DuckDBFailure(_, Some(message))
        if message.starts_with("Constraint Error"))
}

/// The index-dependency refusal key (Task 15 probe, pinned live):
/// `ALTER … SET DATA TYPE` refuses outright — a CONTAINS check, not a
/// prefix, because the library's own wording leads with "Catalog
/// Error" — whenever ANY index, unique or plain, still depends on the
/// column being widened. The pre-ALTER drop (`load.rs`) clears the
/// UNIQUE arbiter out of the way before this can fire; a PLAIN index
/// (identity, `delete_insert`/`scd2` key columns, `merge_scope`) is
/// NOT pre-dropped (037 US5 fix round 2, F4 — a narrower, deliberate
/// scope), so this classifier is what turns that raw catalog error
/// into named advice instead.
pub(crate) fn is_index_dependency_error(e: &duckdb::Error) -> bool {
    matches!(e, duckdb::Error::DuckDBFailure(_, Some(message))
        if message.contains("an index depends on it!"))
}

/// The diagnosis for [`is_index_dependency_error`]: names the failing
/// statement (which already carries the table and column — no separate
/// parse needed) and the remedy, with the service's own wording kept
/// inside rather than discarded.
pub(crate) fn index_dependency_diagnosis(statement: &str, cause: &str) -> String {
    format!(
        "a cross-run widen is blocked by an index: `{statement}` — an index still \
         depends on this column (a plain identity/merge-key/scope index, not the \
         upsert arbiter, which this destination already clears itself); drop the \
         index manually and re-run so ensure recreates it after the widen, or leave \
         the column's type as it was if the index is load-bearing: {cause}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The replay-per-clone invariant, pinned at its seam: a cloned
    /// session connection inherits neither SETs nor LOADs from the
    /// builder connection, so `session()` must replay the recorded
    /// setup — this is the connection that actually writes.
    #[test]
    fn a_session_connection_carries_the_recorded_settings() {
        let dir = tempfile::tempdir().expect("dir");
        let db = Db::connect(
            &dir.path().join("x.duckdb"),
            [("threads".to_owned(), "1".to_owned())],
            [],
        )
        .expect("connect");
        let session = db.session().expect("clone + replay");
        let live: String = session
            .query_row("SELECT current_setting('threads')::VARCHAR", [], |row| {
                row.get(0)
            })
            .expect("query");
        assert_eq!(live, "1", "the setting reached the CLONED connection");
    }

    /// A bad setting value errors at CONNECT (eager application), with
    /// the frozen frame naming the key.
    #[test]
    fn a_bad_setting_value_errors_at_connect() {
        let dir = tempfile::tempdir().expect("dir");
        let err = Db::connect(
            &dir.path().join("x.duckdb"),
            [("threads".to_owned(), "zero".to_owned())],
            [],
        )
        .expect_err("refused")
        .to_string();
        assert!(err.contains("duckdb setting `threads`"), "{err}");
    }

    /// The classifier wiring, pinned directly on constructed library
    /// errors: an `IO Error`-prefixed failure is TRANSIENT, anything
    /// else is FATAL — EXCEPT the deterministic carve-out (037 US6 /
    /// Task 19, S5), which routes fatal even under the `IO Error`
    /// prefix. The commit path (receipt probe, load-committed probe,
    /// `read_state`'s read arm) relies on exactly this split.
    #[test]
    fn classify_splits_io_errors_transient_everything_else_fatal() {
        let failure = |message: &str| {
            duckdb::Error::DuckDBFailure(duckdb::ffi::Error::new(1), Some(message.to_owned()))
        };
        // The lock-conflict family (D-042-2), pinned on the message
        // MEASURED live from a second process's open (042 Task 6; the
        // spawn suite's cross-process cell re-measures it forever):
        // FATAL, because a second read-write open of a single-writer
        // file is an operator error — the holder keeps the lock for
        // its whole life, so no retry budget ever heals it.
        assert!(
            matches!(
                classify(failure(
                    "IO Error: Could not set lock on file \
                     \"/var/tmp/rdlt-tests/lockprobe/x.duckdb\": Conflicting lock is held in \
                     /var/home/netf/Repos/rapidbyte/rdlt/target/debug/examples/lock_probe \
                     (PID 1093008) by user netf. See also \
                     https://duckdb.org/docs/stable/connect/concurrency"
                )),
                DestinationError::Fatal(_)
            ),
            "a cross-process lock conflict is an operator error, never heal-on-retry"
        );
        assert!(
            matches!(
                classify(failure("Binder Error: no such column")),
                DestinationError::Fatal(_)
            ),
            "everything else stays fatal"
        );
        // The deterministic carve-out — one case per fragment
        // probe-pinned in test_probes.rs, plus the lock case above
        // proving the carve-out does NOT swallow the rulebook's
        // reason for the "IO Error" prefix existing at all.
        assert!(
            matches!(
                classify(failure(
                    "IO Error: Cannot open file \"/no/such/dir/x.duckdb\": \
                     No such file or directory"
                )),
                DestinationError::Fatal(_)
            ),
            "a missing parent directory never heals on retry"
        );
        assert!(
            matches!(
                classify(failure(
                    "IO Error: Cannot open file \"/no/perm/x.duckdb\": Permission denied"
                )),
                DestinationError::Fatal(_)
            ),
            "a permission-denied path never heals on retry"
        );
        // The template-vs-suffix guard, directly: OTHER errnos through
        // the SAME "Cannot open file" template are healable and must
        // stay transient — an unmeasured suffix never gets fatal-ed on
        // a guess (fix round 1, S5).
        assert!(
            matches!(
                classify(failure(
                    "IO Error: Cannot open file \"/x.duckdb\": Too many open files"
                )),
                DestinationError::Transient(_)
            ),
            "EMFILE/ENFILE through the open template heals on retry"
        );
        assert!(
            matches!(
                classify(failure(
                    "IO Error: Cannot open file \"/x.duckdb\": No space left on device"
                )),
                DestinationError::Transient(_)
            ),
            "create-time ENOSPC through the open template heals on retry"
        );
    }

    /// The index-dependency classifier, pinned against the EXACT
    /// library error shape probed live (Task 15): a `Catalog Error`
    /// wording, not a prefix match, so `is_index_dependency_error`
    /// must check CONTAINS. The diagnosis names the statement (which
    /// already carries table + column) and keeps the service's cause.
    #[test]
    fn index_dependency_errors_are_classified_and_diagnosed() {
        let e = duckdb::Error::DuckDBFailure(
            duckdb::ffi::Error::new(1),
            Some(
                "Catalog Error: Cannot change the type of this column: an index \
                 depends on it!"
                    .to_owned(),
            ),
        );
        assert!(is_index_dependency_error(&e));
        assert!(!is_index_dependency_error(&duckdb::Error::DuckDBFailure(
            duckdb::ffi::Error::new(1),
            Some("Binder Error: no such column".to_owned())
        )));

        let diagnosis = index_dependency_diagnosis(
            "ALTER TABLE \"t\" ALTER COLUMN \"id\" SET DATA TYPE VARCHAR",
            &e.to_string(),
        );
        assert!(
            diagnosis.contains("ALTER TABLE \"t\" ALTER COLUMN \"id\""),
            "{diagnosis}"
        );
        assert!(diagnosis.contains("drop the index manually"), "{diagnosis}");
        assert!(diagnosis.contains("an index depends on it!"), "{diagnosis}");
    }

    /// The in-process double-open guard at the `Db` seam: a second
    /// connect on the SAME canonical path is refused typed-fatal, and
    /// dropping the first instance makes a sequential re-open legal
    /// again.
    #[test]
    fn a_second_in_process_connect_is_refused_until_the_first_drops() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("one.duckdb");
        let first = Db::connect(&path, [], []).expect("first open");
        let err = Db::connect(&path, [], [])
            .expect_err("second live instance refused")
            .to_string();
        assert!(err.contains("already open in this process"), "{err}");
        drop(first);
        Db::connect(&path, [], []).expect("sequential re-open is legal");
    }
}
