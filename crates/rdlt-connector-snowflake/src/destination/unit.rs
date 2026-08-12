//! The commit unit: one transaction, and the discipline that keeps it
//! one.
//!
//! On this service the transaction IS the atomicity story — rows,
//! receipt and state become durable together at `COMMIT` or not at
//! all — and the one thing that can break it does so silently: **any
//! DDL statement commits the open transaction as a side effect.** No
//! error, no log line; a half-written unit simply becomes durable. So
//! this module owns both halves of the defence: the unit's lifecycle
//! (open lazily, roll back on failure, commit once), and [`DmlOnly`],
//! the executor wrapper that refuses DDL in code rather than trusting
//! call sites to remember.
//!
//! The unit also captures its INSTANT at open: `CURRENT_TIMESTAMP()`
//! here moves per statement, not per transaction (a pinned service
//! fact), and the scd2 boundary needs the retired and inserted versions
//! of an entity to meet at exactly one moment. The capture is a session
//! variable — setting one does not commit, unlike DDL, and that
//! difference is invisible in the syntax, which is why both facts are
//! pinned by the live semantics suite.

use async_trait::async_trait;
use rdlt_connector_sdk::spi::DestinationError;

use super::client::Executor;

/// The three statements bounding a unit, named because their exact
/// spelling is part of the statement-economy pins.
const BEGIN: &str = "BEGIN";
const COMMIT: &str = "COMMIT";
const ROLLBACK: &str = "ROLLBACK";

/// The session variable carrying the unit's instant.
pub(super) const TX_TIMESTAMP_VAR: &str = "rdlt_tx_ts";

/// The capture statement the unit runs right after `BEGIN`.
pub(super) fn capture_instant_sql() -> String {
    format!("SET {TX_TIMESTAMP_VAR} = CURRENT_TIMESTAMP()")
}

/// The unit transaction's lifecycle. Owns nothing but the open flag;
/// every transition takes the executor it happens on.
#[derive(Debug, Default)]
pub(super) struct Unit {
    open: bool,
}

impl Unit {
    /// Open the transaction if it is not already open, capturing the
    /// unit's instant.
    ///
    /// Lazy because a unit with no writes still publishes state and a
    /// receipt — the commit hooks call this too, not only the first
    /// write.
    pub(super) async fn begin_if_closed(
        &mut self,
        executor: &dyn Executor,
    ) -> Result<(), DestinationError> {
        if !self.open {
            executor.execute(BEGIN).await?;
            self.open = true;
            executor.execute(&capture_instant_sql()).await?;
        }
        Ok(())
    }

    /// Abandon the unit, discarding everything it wrote.
    ///
    /// A rollback failure is swallowed on purpose: the caller is
    /// already carrying the failure that matters, the transaction dies
    /// with the session regardless, and a second error would displace
    /// the useful one.
    pub(super) async fn rollback(&mut self, executor: &dyn Executor) {
        if self.open {
            let _ = executor.execute(ROLLBACK).await;
            self.open = false;
        }
    }

    /// Commit the unit. On success the unit is closed; on failure the
    /// caller decides (a failed COMMIT's disposition is the protocol's
    /// to choose, not this lifecycle's).
    pub(super) async fn commit(&mut self, executor: &dyn Executor) -> Result<(), DestinationError> {
        executor.execute(COMMIT).await?;
        self.open = false;
        Ok(())
    }

    /// Whether a transaction is currently open.
    pub(super) fn is_open(&self) -> bool {
        self.open
    }
}

/// Is this statement DDL — i.e. would it commit the open unit?
///
/// `TRUNCATE` is the named trap: it reads like data manipulation and is
/// DDL here, which is why the Replace clear spells `DELETE FROM`.
pub(super) fn is_ddl(sql: &str) -> bool {
    const DDL_VERBS: &[&str] = &[
        "CREATE", "ALTER", "DROP", "TRUNCATE", "GRANT", "REVOKE", "COMMENT", "USE", "UNDROP",
        "RENAME",
    ];
    // `split_whitespace` skips leading whitespace, so an indented or
    // newline-prefixed statement classifies on its real first word.
    let first = sql
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_ascii_uppercase();
    DDL_VERBS.contains(&first.as_str())
}

/// An executor that refuses DDL, for the duration of a unit.
///
/// Code rather than convention because the failure it prevents leaves
/// no trace: a `CREATE` inside the unit commits the rows written before
/// it and exactly-once is broken with nothing to see.
pub(crate) struct DmlOnly<'a>(pub(crate) &'a dyn Executor);

impl DmlOnly<'_> {
    fn refuse(sql: &str) -> DestinationError {
        let verb = sql.split_whitespace().next().unwrap_or("");
        DestinationError::fatal(format!(
            "snowflake: `{verb}` cannot run inside a commit unit — it would commit the \
             transaction and publish rows the unit had not finished writing; schema \
             work belongs before the unit opens"
        ))
    }
}

#[async_trait]
impl Executor for DmlOnly<'_> {
    async fn execute(&self, sql: &str) -> Result<(), DestinationError> {
        if is_ddl(sql) {
            return Err(Self::refuse(sql));
        }
        self.0.execute(sql).await
    }

    async fn scalar_u64(&self, sql: &str, binds: &[&str]) -> Result<u64, DestinationError> {
        if is_ddl(sql) {
            return Err(Self::refuse(sql));
        }
        self.0.scalar_u64(sql, binds).await
    }

    async fn sum_column(&self, sql: &str, column: &str) -> Result<u64, DestinationError> {
        if is_ddl(sql) {
            return Err(Self::refuse(sql));
        }
        self.0.sum_column(sql, column).await
    }

    async fn rows(
        &self,
        sql: &str,
        binds: &[&str],
        columns: &[&str],
    ) -> Result<Vec<Vec<String>>, DestinationError> {
        if is_ddl(sql) {
            return Err(Self::refuse(sql));
        }
        self.0.rows(sql, binds, columns).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// DDL is recognised on the real first word, whatever the case or
    /// leading whitespace.
    #[test]
    fn ddl_verbs_are_recognised_whatever_the_spacing_or_case() {
        for sql in [
            "CREATE TABLE t (id NUMBER)",
            "  create table t (id NUMBER)",
            "\n\tALTER TABLE t ADD COLUMN c NUMBER",
            "drop table t",
            "GRANT SELECT ON t TO ROLE r",
            "USE SCHEMA s",
        ] {
            assert!(is_ddl(sql), "must be DDL: {sql}");
        }
    }

    /// The named trap: TRUNCATE commits the unit, DELETE does not —
    /// which is why the Replace clear deletes.
    #[test]
    fn truncate_is_ddl_which_is_why_replace_deletes_instead() {
        assert!(is_ddl("TRUNCATE TABLE \"EVENTS\""));
        assert!(!is_ddl("DELETE FROM \"EVENTS\""));
    }

    /// Everything a unit legitimately runs passes the guard.
    #[test]
    fn the_statements_a_unit_actually_runs_are_not_ddl() {
        for sql in [
            "INSERT INTO \"T\" (\"ID\") VALUES (1)",
            "MERGE INTO \"T\" USING (SELECT 1) s ON s.x = \"T\".x \
             WHEN MATCHED THEN UPDATE SET a = 1",
            "DELETE FROM \"T\" WHERE id = 1",
            "SELECT count(*) FROM \"T\"",
            "COPY INTO \"T\" FROM @stage",
            BEGIN,
            COMMIT,
            ROLLBACK,
            &capture_instant_sql(),
        ] {
            assert!(!is_ddl(sql), "must not be DDL: {sql}");
        }
    }

    /// Empty input is not DDL (nor anything else).
    #[test]
    fn an_empty_statement_is_not_mistaken_for_ddl() {
        assert!(!is_ddl(""));
        assert!(!is_ddl("   \n  "));
    }

    /// The capture spelling is the frozen one, and reads the clock into
    /// the variable every boundary later reads.
    #[test]
    fn the_instant_capture_is_a_session_variable_set() {
        assert_eq!(
            capture_instant_sql(),
            "SET rdlt_tx_ts = CURRENT_TIMESTAMP()"
        );
    }

    /// A scripted executor that records what reaches it.
    struct Recorder(std::sync::Mutex<Vec<String>>);

    #[async_trait]
    impl Executor for Recorder {
        async fn execute(&self, sql: &str) -> Result<(), DestinationError> {
            self.0.lock().expect("lock").push(sql.to_owned());
            Ok(())
        }
        async fn scalar_u64(&self, sql: &str, _: &[&str]) -> Result<u64, DestinationError> {
            self.0.lock().expect("lock").push(sql.to_owned());
            Ok(0)
        }
        async fn sum_column(&self, sql: &str, _: &str) -> Result<u64, DestinationError> {
            self.0.lock().expect("lock").push(sql.to_owned());
            Ok(0)
        }
        async fn rows(
            &self,
            sql: &str,
            _: &[&str],
            _: &[&str],
        ) -> Result<Vec<Vec<String>>, DestinationError> {
            self.0.lock().expect("lock").push(sql.to_owned());
            Ok(Vec::new())
        }
    }

    /// The refusal happens BEFORE the statement reaches the service —
    /// refused-after-running would already have done the damage — and
    /// it names the verb and the consequence.
    #[tokio::test]
    async fn ddl_is_refused_before_it_reaches_the_service() {
        let inner = Recorder(std::sync::Mutex::new(Vec::new()));
        let unit = DmlOnly(&inner);

        let err = unit
            .execute("CREATE TABLE \"T\" (\"ID\" NUMBER)")
            .await
            .expect_err("DDL inside a unit is refused");
        let rendered = format!("{err}");
        assert!(rendered.contains("CREATE"), "names the verb: {rendered}");
        assert!(
            rendered.contains("commit the transaction"),
            "explains the consequence: {rendered}"
        );
        assert!(
            inner.0.lock().expect("lock").is_empty(),
            "nothing may reach the service"
        );

        unit.execute("INSERT INTO \"T\" VALUES (1)")
            .await
            .expect("DML passes through");
        assert_eq!(inner.0.lock().expect("lock").len(), 1);
    }

    /// Every trait method is guarded — a probe cannot smuggle DDL past
    /// the wrapper.
    #[tokio::test]
    async fn the_refusal_covers_every_query_shape() {
        let inner = Recorder(std::sync::Mutex::new(Vec::new()));
        let unit = DmlOnly(&inner);
        unit.scalar_u64("TRUNCATE TABLE \"T\"", &[])
            .await
            .expect_err("refused");
        unit.sum_column("DROP TABLE \"T\"", "n")
            .await
            .expect_err("refused");
        unit.rows("ALTER TABLE \"T\" ADD COLUMN c NUMBER", &[], &["c"])
            .await
            .expect_err("refused");
        assert!(inner.0.lock().expect("lock").is_empty());
    }

    /// The lifecycle: lazy begin (once), rollback closes, a second
    /// begin reopens.
    #[tokio::test]
    async fn the_unit_opens_lazily_and_exactly_once() {
        let inner = Recorder(std::sync::Mutex::new(Vec::new()));
        let mut unit = Unit::default();
        unit.begin_if_closed(&inner).await.expect("begin");
        unit.begin_if_closed(&inner).await.expect("idempotent");
        assert_eq!(
            *inner.0.lock().expect("lock"),
            vec![BEGIN.to_owned(), capture_instant_sql()],
            "one BEGIN, one capture, however many calls"
        );
        assert!(unit.is_open());

        unit.rollback(&inner).await;
        assert!(!unit.is_open());
        unit.rollback(&inner).await; // idempotent: no second ROLLBACK
        assert_eq!(
            inner.0.lock().expect("lock").last().map(String::as_str),
            Some(ROLLBACK)
        );
        assert_eq!(
            inner
                .0
                .lock()
                .expect("lock")
                .iter()
                .filter(|s| *s == ROLLBACK)
                .count(),
            1
        );
    }
}
