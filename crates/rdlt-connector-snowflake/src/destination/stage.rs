//! Parts, and their journey: built on local disk, uploaded to storage
//! the service provides, loaded by one COPY per table, removed when the
//! unit settles.
//!
//! One part exists locally at a time — written, uploaded, deleted — so
//! peak local usage is a single part, not the unit or the dataset. Two
//! service facts anchor the design, both measured (023): **an upload
//! does not end an open transaction** (so staging happens inside the
//! unit), and **dropping the staging object does** (so teardown never
//! drops it). A third would corrupt loads silently without the guard
//! here: a multi-file upload reports overall SUCCESS while a row says
//! one file failed — every row is inspected, and any failure abandons
//! the unit.

use std::path::{Path, PathBuf};

use rdlt_connector_sdk::spi::DestinationError;
use rdlt_connector_sdk::spi::core::{crash_point, naming::ident_hash};

use super::client::Executor;

/// The staging object for a pipeline: rdlt's shared prefix, an `int_`
/// marker separating it from the merge stage TABLES in the same
/// listing, and the pipeline hash. Deterministic, so every load of the
/// pipeline finds the same object; pipeline-scoped, so two pipelines
/// sharing a schema cannot redefine each other's.
pub(super) fn stage_object_name(pipeline: &str) -> String {
    format!(
        "{}int_{}",
        rdlt_connector_sqlcore::names::STAGE_PREFIX,
        ident_hash(pipeline, 8)
    )
}

/// One staged part: the name the load statement will use (relative to
/// the stage root), and the rows inside it — recorded at build time,
/// the only moment the count is known for free.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Part {
    /// Path under the stage root: `{load}/{table-hash}/{index}.parquet`.
    pub(super) tail: String,
    /// Rows written into the part.
    pub(super) rows: u64,
}

/// The columns an upload's result reports, resolved case-insensitively
/// by the executor.
const UPLOAD_TARGET: &str = "target";
const UPLOAD_STATUS: &str = "status";
const UPLOAD_MESSAGE: &str = "message";

/// The one status meaning a file arrived; anything else is failure,
/// whatever the service calls it.
const UPLOADED: &str = "UPLOADED";

/// How long a remote part must sit before a LATER load may reclaim it.
///
/// Deliberately generous: the window only has to exceed the longest
/// load still possibly in flight, and the failure modes are asymmetric —
/// a day of extra storage versus deleting a live load's parts out from
/// under it.
const STALE_AFTER_HOURS: u32 = 24;

/// One load's staging identity: where its parts go remotely and
/// locally, and the counter naming them.
pub(super) struct Stage {
    /// The staging object's unqualified name (pipeline-scoped).
    name: String,
    /// This load's segment under the object — what keeps two loads of
    /// one pipeline from ever sharing a part name.
    load: String,
    /// The local directory holding at most one part at a time.
    local: PathBuf,
    /// Part counter. Never reset within a session, so a part written
    /// after a commit cannot reuse the name of one whose cleanup
    /// failed.
    next_part: u64,
}

impl Stage {
    /// Derive the identity; touches nothing.
    ///
    /// Pipeline and load names are free text and are HASHED, never
    /// sanitised: two different names that sanitise alike would share a
    /// scope and reclaim each other's parts.
    pub(super) fn new(pipeline: &str, load: &str) -> Self {
        let pipeline_hash = ident_hash(pipeline, 12);
        let load = ident_hash(load, 12);
        Self {
            name: stage_object_name(pipeline),
            local: std::env::temp_dir().join(format!("rdlt-sf-{pipeline_hash}-{load}")),
            load,
            next_part: 0,
        }
    }

    /// The staging object's unqualified name, for the caller to
    /// qualify.
    pub(super) fn name(&self) -> &str {
        &self.name
    }

    /// The statement ensuring the staging object exists. `IF NOT
    /// EXISTS`, never `OR REPLACE`: the object derives entirely from
    /// the pipeline name and has no configuration to go stale, while
    /// replacing it would discard another session's staged parts for
    /// nothing. Schema work — it runs before any unit opens.
    pub(super) fn create_sql(qualified_name: &str) -> String {
        format!("CREATE STAGE IF NOT EXISTS {qualified_name}")
    }

    /// One table's prefix under this load's segment.
    fn prefix(&self, table: &str) -> String {
        format!("{}/{}", self.load, ident_hash(table, 16))
    }

    /// Build a part on disk, upload it, and remove the local copy on
    /// EVERY path out — a file left after a failure is exactly the
    /// residue a crash would leave.
    pub(super) async fn put_part(
        &mut self,
        executor: &dyn Executor,
        qualified_stage: &str,
        table: &str,
        bytes: Vec<u8>,
        rows: u64,
    ) -> Result<Part, DestinationError> {
        let index = self.next_part;
        self.next_part += 1;
        // `.parquet` is load-bearing: the upload recognises compressed
        // payloads by extension as well as content, and a part it
        // mis-recognised would gain a compression suffix the load
        // statement does not name.
        let file_name = format!("{index:08}.parquet");
        let prefix = self.prefix(table);

        std::fs::create_dir_all(&self.local).map_err(|e| local_err(&self.local, "creating", e))?;
        let path = self.local.join(&file_name);
        std::fs::write(&path, &bytes).map_err(|e| local_err(&path, "writing", e))?;
        // The part exists locally and nowhere else. This load's next
        // attempt must clear it; another load's file must survive — the
        // two are indistinguishable from here, which is why local
        // reclaim is scoped to the load's own directory.
        crash_point!(
            "sf.stage.write",
            Err(DestinationError::fatal("injected crash at sf.stage.write"))
        );

        let uploaded = self
            .upload(executor, &path, qualified_stage, &prefix, &file_name)
            .await;
        // Whether or not the upload worked.
        let _ = std::fs::remove_file(&path);
        uploaded?;
        // Uploaded, not yet recorded by the session: remote debris no
        // load statement will name, collected by the aged reclaim — a
        // DIFFERENT place and reclaimer than the point above, which is
        // why it is a point of its own.
        crash_point!(
            "sf.stage.upload",
            Err(DestinationError::fatal("injected crash at sf.stage.upload"))
        );

        Ok(Part {
            tail: format!("{prefix}/{file_name}"),
            rows,
        })
    }

    /// Issue the PUT and verify EVERY reported row.
    async fn upload(
        &self,
        executor: &dyn Executor,
        path: &Path,
        qualified_stage: &str,
        prefix: &str,
        file_name: &str,
    ) -> Result<(), DestinationError> {
        let sql = put_sql(path, qualified_stage, prefix);
        let report = executor
            .rows(&sql, &[], &[UPLOAD_TARGET, UPLOAD_STATUS, UPLOAD_MESSAGE])
            .await?;

        if report.is_empty() {
            return Err(DestinationError::fatal(format!(
                "snowflake: uploading `{file_name}` reported no result at all; the part \
                 cannot be assumed to have arrived"
            )));
        }
        for row in &report {
            let (target, status, message) = (&row[0], &row[1], &row[2]);
            if !status.eq_ignore_ascii_case(UPLOADED) {
                return Err(DestinationError::fatal(format!(
                    "snowflake: uploading `{file_name}` reported `{status}` for `{target}`{}; \
                     the unit is abandoned rather than loading a part that is not there",
                    if message.is_empty() {
                        String::new()
                    } else {
                        format!(" ({message})")
                    }
                )));
            }
            // With compression off the service returns the basename
            // unchanged; anything else means it renamed the file under
            // us and the load statement would miss it.
            if !target.eq_ignore_ascii_case(file_name) {
                return Err(DestinationError::fatal(format!(
                    "snowflake: uploading `{file_name}` staged it as `{target}` instead; the \
                     load statement names files by their local name and would not find this one"
                )));
            }
        }
        Ok(())
    }

    /// Remove this load's staged parts. Best effort: a part is named by
    /// exactly one COPY, an uncommitted unit never names its parts
    /// again, and a cleanup failure must not fail a committed load —
    /// the aged reclaim at the next open sweeps what remains.
    pub(super) async fn remove(&self, executor: &dyn Executor, qualified_stage: &str) {
        let _ = executor
            .execute(&format!("REMOVE @{qualified_stage}/{}/", self.load))
            .await;
    }

    /// Reclaim remote parts abandoned by dead loads, by AGE.
    ///
    /// A settling load removes its own parts, so whatever remains
    /// belongs to a dead load — or a live one, and by name the two are
    /// identical; age is the only separator (see [`STALE_AFTER_HOURS`]).
    /// The comparison runs in SQL on the SERVICE's clock: this host's
    /// clock has no defined relation to the one that stamped the
    /// object, and hand-parsing a foreign date format to decide what to
    /// DELETE is a bug with an expensive blast radius. Best effort
    /// throughout — reclaim failure is a storage cost, deleting a live
    /// part is a correctness cost, and the trade only goes one way.
    pub(super) async fn reclaim_remote(&self, executor: &dyn Executor, qualified_stage: &str) {
        self.reclaim_remote_older_than(executor, qualified_stage, STALE_AFTER_HOURS)
            .await
    }

    /// The reclaim against a chosen age — parameterised ONLY so a test
    /// can prove both outcomes (stale removed, fresh kept); a threshold
    /// no test can move never has both halves checked.
    pub(super) async fn reclaim_remote_older_than(
        &self,
        executor: &dyn Executor,
        qualified_stage: &str,
        stale_after_hours: u32,
    ) {
        // LIST first: the filter reads RESULT_SCAN of the PREVIOUS
        // query on this same session.
        if executor
            .execute(&format!("LIST @{qualified_stage}"))
            .await
            .is_err()
        {
            return;
        }
        let Ok(stale) = executor
            .rows(
                &format!(
                    "SELECT \"name\" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) \
                     WHERE TO_TIMESTAMP_TZ(\"last_modified\", \
                     'DY, DD MON YYYY HH24:MI:SS TZD') \
                     < DATEADD(hour, -{stale_after_hours}, CURRENT_TIMESTAMP())",
                ),
                &[],
                &["name"],
            )
            .await
        else {
            return;
        };
        for row in &stale {
            // The listing prefixes names with the stage's own name; the
            // removal wants the stage-relative path. A name without the
            // separator is left alone rather than guessed at. One
            // object at a time, never the shared scope: a long-running
            // load can hold stale AND fresh parts under one scope, and
            // a scope-wide remove would take the fresh ones with it.
            let Some((_, relative)) = row[0].split_once('/') else {
                continue;
            };
            let _ = executor
                .execute(&format!("REMOVE @{qualified_stage}/{relative}"))
                .await;
        }
    }

    /// Remove this load's LOCAL residue — its own directory, whole and
    /// unconditionally: a previous attempt of the same load wrote those
    /// files and no receipt names them. Other loads' directories are
    /// untouched (concurrent and abandoned look identical from here).
    pub(super) fn reclaim_local(&self) {
        let _ = std::fs::remove_dir_all(&self.local);
    }

    /// Where this load's parts wait between write and upload — exposed
    /// so a test can assert the directory is EMPTY once a load settles,
    /// instead of trusting that the next run cleans it.
    pub(super) fn local_dir(&self) -> &Path {
        &self.local
    }
}

/// The PUT uploading one local part. Verb FIRST: the library switches
/// result format on the first token, and a leading comment would
/// request a format the service refuses for uploads. AUTO_COMPRESS off
/// keeps the staged name equal to the local one (compression appends a
/// suffix the load statement would not find); OVERWRITE on lets a
/// retried unit re-upload the same part.
fn put_sql(path: &Path, qualified_stage: &str, prefix: &str) -> String {
    format!(
        "PUT 'file://{}' @{qualified_stage}/{prefix}/ AUTO_COMPRESS = FALSE OVERWRITE = TRUE",
        path.display()
    )
}

/// The COPY loading a set of parts into one table.
///
/// Columns are projected EXPLICITLY out of the file — never
/// `MATCH_BY_COLUMN_NAME`, which sets a target column absent from the
/// file to NULL instead of its default and thereby destroys the arrival
/// order the merge tie-break reads (every staged row ties; the survivor
/// goes arbitrary). The case asymmetry is load-bearing: the target list
/// is the CATALOG's upper case, the `$1:"col"` projection is the
/// FILE's own case — the accessor is case-sensitive, and the
/// symmetrical-looking upper form finds nothing (every column arrives
/// NULL). `FORCE = TRUE` because the service's file-level load history
/// would otherwise skip a rolled-back unit's re-load — exactly-once
/// here comes from the unit and the receipt, not from file dedup.
pub(super) fn copy_sql(
    qualified_target: &str,
    qualified_stage: &str,
    columns: &[String],
    parts: &[Part],
) -> String {
    let files = parts
        .iter()
        .map(|part| format!("'{}'", part.tail))
        .collect::<Vec<_>>()
        .join(", ");
    let target_list = columns
        .iter()
        .map(|column| super::ddl::quote(column))
        .collect::<Vec<_>>()
        .join(", ");
    let projection = columns
        .iter()
        .map(|column| format!("$1:\"{}\"", column.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "COPY INTO {qualified_target} ({target_list}) \
         FROM (SELECT {projection} FROM @{qualified_stage}/) FILES = ({files}) \
         FILE_FORMAT = ( TYPE = PARQUET ) FORCE = TRUE PURGE = FALSE \
         ON_ERROR = ABORT_STATEMENT"
    )
}

/// Classify a local filesystem failure by what the operator should DO —
/// never a bare errno.
fn local_err(path: &Path, doing: &str, error: std::io::Error) -> DestinationError {
    use std::io::ErrorKind;
    let location = path.display();
    match error.kind() {
        // TRANSIENT: the usual culprit is another process's temporary
        // files, and the next attempt often finds room — failing the
        // load costs more than one retry.
        ErrorKind::StorageFull => DestinationError::transient(format!(
            "snowflake: no space left while {doing} a staged part at `{location}`; parts are \
             built in the system temporary directory, which TMPDIR selects"
        )),
        ErrorKind::PermissionDenied | ErrorKind::ReadOnlyFilesystem => {
            DestinationError::fatal(format!(
                "snowflake: cannot write a staged part at `{location}` ({doing}): the temporary \
                 directory is not writable; TMPDIR selects it"
            ))
        }
        _ => DestinationError::fatal(format!(
            "snowflake: {doing} a staged part at `{location}`: {error}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The staging object shares rdlt's prefix but not the merge stage
    /// tables' namespace, and derives deterministically.
    #[test]
    fn the_staging_object_is_named_apart_from_the_merge_stage_tables() {
        let name = stage_object_name("sales");
        assert!(name.starts_with(rdlt_connector_sqlcore::names::STAGE_PREFIX));
        assert!(name.contains("int_"), "{name}");
        assert_ne!(
            name,
            rdlt_connector_sqlcore::names::stage_table("sales", "events")
        );
        assert_eq!(name, stage_object_name("sales"), "deterministic");
    }

    /// The collision this scheme exists to prevent: part numbering
    /// restarts per session, so without per-load segments two sessions'
    /// FIRST parts would share a name — and one session's reclaim would
    /// delete a part the other is about to load.
    #[test]
    fn two_loads_of_one_pipeline_cannot_collide_on_a_part_name() {
        let one = Stage::new("sales", "load-1");
        let two = Stage::new("sales", "load-2");
        assert_eq!(one.name(), two.name(), "same pipeline, same object");
        assert_ne!(one.load, two.load);
        assert_ne!(one.prefix("events"), two.prefix("events"));
        assert_ne!(one.local, two.local, "local directories differ too");
    }

    /// Tables under one load also never share a prefix.
    #[test]
    fn two_tables_never_share_a_prefix() {
        let stage = Stage::new("sales", "load-1");
        assert_ne!(stage.prefix("events"), stage.prefix("orders"));
    }

    /// The PUT's silent-breakage guards, on the statement the upload
    /// ACTUALLY renders (an earlier pin asserted on a string the test
    /// itself had written, which no code change could fail): verb
    /// first, compression off, overwrite on, and it passes the unit
    /// guard.
    #[test]
    fn the_upload_statement_starts_with_its_verb_and_disables_renaming() {
        let stage = Stage::new("sales", "load-1");
        let sql = put_sql(
            Path::new("/tmp/x/00000000.parquet"),
            "\"DB\".\"S\".\"ST\"",
            &stage.prefix("events"),
        );
        assert!(
            sql.starts_with("PUT 'file:///tmp/x/00000000.parquet' @"),
            "{sql}"
        );
        assert!(sql.contains("AUTO_COMPRESS = FALSE"), "{sql}");
        assert!(sql.contains("OVERWRITE = TRUE"), "{sql}");
        assert!(
            !super::super::unit::is_ddl(&sql),
            "an upload must pass the unit guard"
        );
    }

    /// A scripted executor: records what it is asked and answers `rows`
    /// with a canned upload report.
    struct Scripted {
        log: std::sync::Mutex<Vec<String>>,
        report: Vec<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl Executor for Scripted {
        async fn execute(&self, sql: &str) -> Result<(), DestinationError> {
            self.log.lock().expect("lock").push(sql.to_owned());
            Ok(())
        }
        async fn scalar_u64(&self, _: &str, _: &[&str]) -> Result<u64, DestinationError> {
            Ok(0)
        }
        async fn sum_column(&self, _: &str, _: &str) -> Result<u64, DestinationError> {
            Ok(0)
        }
        async fn rows(
            &self,
            sql: &str,
            _: &[&str],
            _: &[&str],
        ) -> Result<Vec<Vec<String>>, DestinationError> {
            self.log.lock().expect("lock").push(sql.to_owned());
            Ok(self.report.clone())
        }
    }

    fn scripted(report: Vec<Vec<String>>) -> Scripted {
        Scripted {
            log: std::sync::Mutex::new(Vec::new()),
            report,
        }
    }

    fn row(target: &str, status: &str, message: &str) -> Vec<String> {
        vec![target.to_owned(), status.to_owned(), message.to_owned()]
    }

    /// The measured service fact the verification exists for: a
    /// multi-file PUT returns Ok with a MIXED rowset, so EVERY row must
    /// be inspected — a report that is success-overall but carries one
    /// failed row abandons the unit, naming the failed file.
    #[tokio::test]
    async fn an_upload_with_one_failed_row_fails_even_among_successes() {
        let stage = Stage::new("sales", "load-1");
        let executor = scripted(vec![
            row("00000000.parquet", "UPLOADED", ""),
            row("00000000.parquet", "SKIPPED", "stale"),
        ]);
        let err = stage
            .upload(
                &executor,
                Path::new("/tmp/x/00000000.parquet"),
                "\"DB\".\"S\".\"ST\"",
                "seg/tab",
                "00000000.parquet",
            )
            .await
            .expect_err("one failed row abandons the unit");
        let rendered = format!("{err}");
        assert!(rendered.contains("SKIPPED"), "{rendered}");
        assert!(rendered.contains("(stale)"), "{rendered}");
    }

    /// An empty report proves nothing arrived — and must not pass.
    #[tokio::test]
    async fn an_upload_reporting_nothing_is_refused() {
        let stage = Stage::new("sales", "load-1");
        let executor = scripted(Vec::new());
        let err = stage
            .upload(
                &executor,
                Path::new("/tmp/x/00000000.parquet"),
                "\"DB\".\"S\".\"ST\"",
                "seg/tab",
                "00000000.parquet",
            )
            .await
            .expect_err("no result at all");
        assert!(format!("{err}").contains("no result at all"), "{err}");
    }

    /// A rename under us means the COPY would miss the file — refused,
    /// naming both spellings. Status matching stays case-insensitive.
    #[tokio::test]
    async fn a_renamed_upload_is_refused_and_status_case_is_ignored() {
        let stage = Stage::new("sales", "load-1");
        let executor = scripted(vec![row("00000000.parquet.gz", "uploaded", "")]);
        let err = stage
            .upload(
                &executor,
                Path::new("/tmp/x/00000000.parquet"),
                "\"DB\".\"S\".\"ST\"",
                "seg/tab",
                "00000000.parquet",
            )
            .await
            .expect_err("renamed");
        let rendered = format!("{err}");
        assert!(rendered.contains("00000000.parquet.gz"), "{rendered}");

        let ok = scripted(vec![row("00000000.parquet", "Uploaded", "")]);
        stage
            .upload(
                &ok,
                Path::new("/tmp/x/00000000.parquet"),
                "\"DB\".\"S\".\"ST\"",
                "seg/tab",
                "00000000.parquet",
            )
            .await
            .expect("case-insensitive status, unchanged name");
        assert!(
            ok.log.lock().expect("lock")[0].starts_with("PUT "),
            "the rendered statement reached the executor"
        );
    }

    /// Local reclaim removes the load's OWN directory — whole,
    /// unconditionally — and leaves everything else where it is.
    #[test]
    fn local_reclaim_removes_this_loads_directory_only() {
        let mine = Stage::new("sales-reclaim-pin", "load-1");
        let other = Stage::new("sales-reclaim-pin", "load-2");
        std::fs::create_dir_all(mine.local_dir()).expect("mine");
        std::fs::write(mine.local_dir().join("00000000.parquet"), b"x").expect("part");
        std::fs::create_dir_all(other.local_dir()).expect("other");
        std::fs::write(other.local_dir().join("00000000.parquet"), b"y").expect("part");

        mine.reclaim_local();
        assert!(!mine.local_dir().exists(), "my residue is gone");
        assert!(other.local_dir().exists(), "the other load's survives");
        let _ = std::fs::remove_dir_all(other.local_dir());
    }

    /// The shipped window is the recorded one: generous on purpose (a
    /// day of storage versus deleting a live load's parts), and any
    /// change to it must show up here.
    #[test]
    fn the_shipped_reclaim_window_is_twenty_four_hours() {
        assert_eq!(STALE_AFTER_HOURS, 24);
    }

    /// The COPY names exactly this unit's parts, keeps the case
    /// asymmetry, and never matches by name.
    #[test]
    fn the_load_statement_names_each_part_and_keeps_the_case_asymmetry() {
        let parts = vec![
            Part {
                tail: "aa/bb/00000000.parquet".into(),
                rows: 3,
            },
            Part {
                tail: "aa/bb/00000001.parquet".into(),
                rows: 2,
            },
        ];
        let columns = vec!["id".to_string(), "note".to_string()];
        let sql = copy_sql(
            "\"DB\".\"S\".\"EVENTS\"",
            "\"DB\".\"S\".\"ST\"",
            &columns,
            &parts,
        );
        assert!(
            sql.contains("FILES = ('aa/bb/00000000.parquet', 'aa/bb/00000001.parquet')"),
            "{sql}"
        );
        assert!(
            sql.contains("(\"ID\", \"NOTE\")") && sql.contains("$1:\"id\", $1:\"note\""),
            "catalog-case targets, file-case projection: {sql}"
        );
        assert!(!sql.contains("MATCH_BY_COLUMN_NAME"), "{sql}");
        assert!(sql.contains("FORCE = TRUE"), "{sql}");
        assert!(sql.contains("ON_ERROR = ABORT_STATEMENT"), "{sql}");
        assert!(!super::super::unit::is_ddl(&sql), "{sql}");
    }

    /// Stage creation is idempotent schema work — and IS DDL, which the
    /// unit guard must keep refusing.
    #[test]
    fn creating_the_staging_object_is_idempotent_ddl() {
        let sql = Stage::create_sql("\"DB\".\"S\".\"ST\"");
        assert_eq!(sql, "CREATE STAGE IF NOT EXISTS \"DB\".\"S\".\"ST\"");
        assert!(super::super::unit::is_ddl(&sql));
    }

    /// Local failures name the condition and the lever (TMPDIR), with
    /// the storage-full case transient.
    #[test]
    fn local_failures_name_the_condition_rather_than_the_errno() {
        use std::io::{Error, ErrorKind};
        let path = Path::new("/tmp/rdlt-sf-probe/00000000.parquet");

        let full = local_err(path, "writing", Error::from(ErrorKind::StorageFull));
        assert!(matches!(full, DestinationError::Transient(_)), "{full:?}");
        assert!(format!("{full}").contains("no space left"), "{full}");
        assert!(format!("{full}").contains("TMPDIR"), "{full}");

        let readonly = local_err(path, "writing", Error::from(ErrorKind::ReadOnlyFilesystem));
        assert!(
            matches!(readonly, DestinationError::Fatal(_)),
            "{readonly:?}"
        );
        assert!(format!("{readonly}").contains("not writable"), "{readonly}");
    }
}
