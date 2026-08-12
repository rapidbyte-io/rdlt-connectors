//! The ONE driver boundary.
//!
//! `oracle` is SYNCHRONOUS and its handles are not safe to hold
//! across an await, so the connection lives on a dedicated thread and
//! this module is the only place that knows it. Callers hand it a
//! closure and await the result.
//!
//! WHAT THIS BOUNDARY NO LONGER NEEDS (plan.md T005). Against the
//! pure-Rust driver it also carried a poison rule (any errored
//! statement killed the connection), page-size derivation from the
//! session data unit, and a connection recycler to stay under the
//! server's open-cursor limit. None of that survives the switch: an
//! errored statement leaves the connection usable, one cursor streams
//! a whole result set, and the driver closes its own cursors.

use std::sync::mpsc;

use rdlt_connector_sdk::spi::SourceError;

use super::config::Config;

/// A unit of work for the connection's own thread.
type Job = Box<dyn FnOnce(&mut oracle::Connection) + Send>;

/// An Oracle connection, reachable from async code.
///
/// Dropping this closes the channel, which ends the thread, which
/// drops the connection — and the driver sends a real logoff rather
/// than abandoning the session for the server to reap.
pub(crate) struct Client {
    jobs: Option<mpsc::Sender<Job>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Client {
    /// Connect, with every failure typed by its ORA code.
    pub(crate) async fn connect(config: &Config) -> Result<Self, SourceError> {
        let connect_string = config.connect_string();
        let user = config.user.clone();
        let password = config.password.reveal().to_owned();
        let cache = config.tuning.statement_cache;
        let call_timeout = std::time::Duration::from_millis(config.tuning.read_timeout_ms);
        let context = format!("connecting to `{connect_string}`");

        let (jobs, inbox) = mpsc::channel::<Job>();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();

        let thread = std::thread::Builder::new()
            .name("rdlt-oracle".into())
            .spawn(move || {
                let conn = oracle::Connection::connect(&user, &password, &connect_string);
                let mut conn = match conn {
                    Ok(conn) => conn,
                    Err(e) => {
                        let _ = ready_tx.send(Err(classify(&context, &e)));
                        return;
                    }
                };
                // Pin the session to UTC so a TIMESTAMP WITH LOCAL
                // TIME ZONE does not depend on the zone of whatever
                // machine happens to be running rdlt.
                if let Err(e) = conn.execute("ALTER SESSION SET TIME_ZONE='UTC'", &[]) {
                    let _ = ready_tx.send(Err(classify("pinning the session time zone", &e)));
                    return;
                }
                let _ = conn.set_stmt_cache_size(cache);
                // The statement budget, enforced by the CLIENT
                // library: a round trip that outlives it is aborted
                // rather than waited on forever. Without this the
                // knob was documented and inert, and a hung query
                // parked a job on this thread with nothing to
                // interrupt it.
                if let Err(e) = conn.set_call_timeout(Some(call_timeout)) {
                    let _ = ready_tx.send(Err(classify("setting the statement timeout", &e)));
                    return;
                }
                if ready_tx.send(Ok(())).is_err() {
                    return;
                }
                // One job at a time, in order, on the one thread that
                // owns the connection.
                while let Ok(job) = inbox.recv() {
                    // A panicking job must not take the thread with
                    // it. Unwinding out of this loop killed the
                    // connection, dropped the reply channel, and the
                    // caller then saw "the thread dropped the job" —
                    // a TRANSIENT, so the engine retried a genuine
                    // bug five times while the panic message went
                    // only to stderr, and `close()` never ran.
                    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        job(&mut conn);
                    }))
                    .is_err()
                    {
                        // The reply channel is already gone (the
                        // job's sender dropped while unwinding), so
                        // the caller learns of it as a dropped job —
                        // but the connection survives for the next
                        // one, and the logoff below still runs.
                        continue;
                    }
                }
                let _ = conn.close();
            })
            .map_err(|e| SourceError::fatal(format!("starting the oracle thread: {e}")))?;

        match ready_rx.await {
            Ok(Ok(())) => Ok(Self {
                jobs: Some(jobs),
                thread: Some(thread),
            }),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(SourceError::transient(
                "the oracle connection thread ended before it reported readiness",
            )),
        }
    }

    /// Run `job` on the connection's thread and await its result.
    pub(crate) async fn with<T, F>(&self, job: F) -> Result<T, SourceError>
    where
        T: Send + 'static,
        F: FnOnce(&mut oracle::Connection) -> Result<T, SourceError> + Send + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let jobs = self
            .jobs
            .as_ref()
            .ok_or_else(|| SourceError::transient("the oracle connection is closing"))?;
        jobs.send(Box::new(move |conn| {
            let _ = tx.send(job(conn));
        }))
        .map_err(|_| SourceError::transient("the oracle connection thread has ended"))?;
        rx.await
            .map_err(|_| SourceError::transient("the oracle connection thread dropped the job"))?
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        // Closing the channel ends the loop; joining lets the logoff
        // finish rather than racing process exit.
        drop(self.jobs.take());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// The ODPI-C load-time failure family: the driver could not load a
/// USABLE Oracle Client library into this process. Verified against
/// the vendored ODPI-C source (`odpi/src/dpiErrorMessages.h`):
/// 1047 cannot locate the library, 1049 a symbol is missing from it
/// (a broken or bitness-mismatched install), 1050 and 1079 the
/// library is too old, 1072 its version is unsupported. POLARITY:
/// any failure OUTSIDE this family — a network refusal, an ORA code,
/// a call timeout — means a client WAS found and loaded, which is
/// exactly what the probe asks.
const DPI_CLIENT_LOAD_FAMILY: &[i32] = &[1047, 1049, 1050, 1072, 1079];

/// Is a USABLE Oracle Client library loadable in THIS process?
///
/// ODPI-C compiles from vendored source, so the BUILD needs nothing —
/// but the first connection dlopens Oracle's client (libclntsh) at
/// RUNTIME, and a process without a usable one dies inside the driver
/// with an error no caller upstream can type. Probed by attempting a
/// connection to an address nothing answers: the driver reports a
/// load-time client failure (the `DPI_CLIENT_LOAD_FAMILY` codes just
/// above, through the crate's structured `Error::dpi_code` accessor —
/// no ORA code exists at that layer) differently from a network
/// failure, and any OTHER outcome means a client was found and loaded.
///
/// The connector binary calls this BEFORE the protocol handshake so an
/// unusable client is a typed refusal on stderr, never an opaque death
/// mid-serve; test fixtures call it to skip live cells rather than
/// fail them.
pub fn client_available() -> bool {
    match oracle::Connection::connect("x", "x", "//127.0.0.1:1/NOPE") {
        Ok(_) => true,
        Err(e) => !e
            .dpi_code()
            .is_some_and(|code| DPI_CLIENT_LOAD_FAMILY.contains(&code)),
    }
}

/// The classification rulebook, keyed on the STRUCTURED ORA code.
///
/// Unknown codes default FATAL: retrying an undiagnosed failure hides
/// it. The transient family is only what the environment heals on its
/// own.
///
/// CREDENTIALS ARE NOT TRANSIENT, deliberately. Oracle's default
/// profile locks an account after ten failed logins and the engine
/// attempts a run five times, so retrying a rotated password would
/// lock the user out for every other consumer of it within two runs.
pub(crate) fn classify(context: &str, e: &oracle::Error) -> SourceError {
    let Some(db) = e.db_error() else {
        return SourceError::fatal(format!("{context}: {e}"));
    };
    let code = db.code();
    if TRANSIENT_ORA.contains(&code) {
        SourceError::transient(format!("{context}: {e}"))
    } else if AUTH_ORA.contains(&code) {
        SourceError::fatal(format!("{context}: {e} — check `user` and `password`"))
    } else {
        SourceError::fatal(format!("{context}: {e}"))
    }
}

/// Timeouts, listener and connection loss, instance startup, session
/// limits, and snapshot staleness for a fresh statement.
const TRANSIENT_ORA: &[i32] = &[
    18,    // maximum number of sessions exceeded
    1033,  // initialization or shutdown in progress
    1089,  // immediate shutdown in progress
    1555,  // snapshot too old (a fresh statement retries clean)
    3113,  // end-of-file on communication channel
    3114,  // not connected to ORACLE
    12170, // connect timeout
    12514, // listener does not know of the service (a PDB still registering)
    12528, // listener: all appropriate instances are blocking
    12541, // no listener
];

/// Credential failures — fatal, and named so the operator fixes the
/// secret rather than hunting the network.
const AUTH_ORA: &[i32] = &[
    1004,  // default username feature not supported
    1017,  // invalid username/password
    28000, // the account is locked (what retrying causes)
    28001, // the password has expired
];

/// Quote a possibly QUALIFIED table name, one part at a time.
///
/// `HR.EMPLOYEES` is the ordinary shape of an Oracle estate — an app
/// schema read by a separate read-only user — and quoting the whole
/// string would produce the single identifier `"HR.EMPLOYEES"`, which
/// no table has ever been called.
///
/// A part already written in double quotes is taken VERBATIM (minus
/// the quotes). That is the only way to name a table created as
/// `"lower_t"`: unquoted Oracle identifiers fold upper-case, so
/// without the escape hatch a case-sensitive table is unreachable.
pub(crate) fn quote_table(table: &str) -> String {
    table
        .split('.')
        .map(|part| {
            let part = part.trim();
            match part.strip_prefix('"').and_then(|p| p.strip_suffix('"')) {
                // Already escaped by whoever wrote it: re-doubling
                // would turn `"a""b"` into a different identifier.
                Some(verbatim) => format!("\"{verbatim}\""),
                None => quote_upper(part),
            }
        })
        .collect::<Vec<_>>()
        .join(".")
}

/// One identifier, upper-cased (Oracle's own folding rule) and
/// quoted, with any embedded quote doubled so it cannot end the
/// identifier early.
pub(crate) fn quote_upper(ident: &str) -> String {
    format!("\"{}\"", ident.to_uppercase().replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qualified_names_quote_each_part() {
        assert_eq!(quote_table("employees"), "\"EMPLOYEES\"");
        assert_eq!(quote_table("hr.employees"), "\"HR\".\"EMPLOYEES\"");
        assert_eq!(quote_table(" hr . employees "), "\"HR\".\"EMPLOYEES\"");
    }

    /// A quoted part is the escape hatch for a case-sensitive table.
    #[test]
    fn a_quoted_part_is_taken_verbatim() {
        assert_eq!(quote_table("\"lower_t\""), "\"lower_t\"");
        assert_eq!(quote_table("hr.\"lower_t\""), "\"HR\".\"lower_t\"");
    }

    /// The identifier gate: an embedded quote must not be able to end
    /// the identifier and start SQL of its own.
    #[test]
    fn embedded_quotes_are_doubled() {
        assert_eq!(quote_table("a\"b"), "\"A\"\"B\"");
        assert_eq!(quote_table("\"a\"\"b\""), "\"a\"\"b\"");
    }
}
