//! How this destination rolls its output into part files: the vocabulary the file destination's
//! own config document speaks.
//!
//! Owned here rather than shared, because it IS this connector's
//! vocabulary — this connector writes parquet part files directly, so every
//! field here is one it honours itself. A neighbouring
//! connector answering the same question is answering it about its own
//! writer, and the two are free to differ where their writers do.
//!
//! Shape is refused at the parse; VALUES at [`Options::validate`],
//! which the config gate calls — so a semantic refusal reaches the
//! operator as the configuration error it is, never through a parse
//! arm.

use serde::{Deserialize, Serialize};

/// When an output part is closed and the next one started.
///
/// # What `target_bytes` measures
///
/// The ENCODED size of the part being written: what lands on disk or
/// in the object store, after compression. This is deliberately NOT
/// the engine's `batch_policy.every_bytes`, which measures the Arrow
/// IN-MEMORY footprint — snappy parquet can be 5-10x smaller than
/// its Arrow form, and the ratio moves with the data, so in-memory
/// size is not a usable proxy for file size.
///
/// # It is a floor, not a target
///
/// A batch is never split, so a part is closed AFTER crossing the
/// threshold. A 128 MiB target with large batches yields 128-140 MiB
/// files.
///
/// # A part never spans a commit
///
/// The publish protocols need whole files in a commit, so a commit
/// closes the open part. The commit cadence is therefore an UPPER
/// BOUND on part size: parts cannot grow past what one commit unit
/// contains.
///
/// # What each adopting destination does with these
///
/// `target_bytes` and `roll_after_seconds` are behavioural promises
/// and every adopting destination honours both. `max_open_bytes` is a
/// resource bound, and a destination that streams its part out rather
/// than accumulating it in memory MEETS the bound trivially — Iceberg
/// hands rows to the library's own rolling file writer, so nothing
/// accumulates for the ceiling to cap. It is satisfied there, not
/// ignored.
///
/// A destination that cannot honour a field must REFUSE it at its
/// config gate. Accepting a setting and not applying it is the defect
/// class this vocabulary exists to end.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[schemars(rename = "PartOptions")]
#[serde(deny_unknown_fields)]
pub struct Options {
    /// Close the part once its ENCODED size reaches this many bytes.
    ///
    /// Defaults to 128 MiB — the size query engines and object stores
    /// are happiest with. Without it a destination writes one part
    /// per batch handed to it, which on a paging source means many
    /// small files: the data-lake anti-pattern.
    ///
    /// `None` disables size-based rolling entirely.
    #[serde(default = "default_target_bytes")]
    pub target_bytes: Option<u64>,

    /// Close the part after this many seconds, whichever comes first.
    ///
    /// Defaults to OFF, and it is an approximation worth
    /// understanding: it is evaluated when a write arrives, not on a
    /// background timer, so a stream that goes quiet holds its part
    /// open until the next write or the commit. It bounds staleness
    /// on a BUSY stream; it cannot bound it on an idle one.
    #[serde(default = "default_roll_after_seconds")]
    pub roll_after_seconds: Option<u32>,

    /// The SAFETY VALVE, not a tuning knob: the most memory all open
    /// parts may hold between them before the largest is closed early.
    ///
    /// An open part lives in RAM until it closes, and a partitioned
    /// destination holds ONE PER PARTITION — so without a ceiling the
    /// footprint is `partitions × target_bytes`, which a 128 MiB
    /// target turns into gigabytes at a hundred partitions. MEASURED:
    /// 1.5M rows across 97 partitions in one commit peaked at 538 MB
    /// RSS with every part still open.
    #[serde(default = "default_max_open_bytes")]
    pub max_open_bytes: Option<u64>,
}

/// 128 MiB. See [`Options::target_bytes`].
fn default_target_bytes() -> Option<u64> {
    Some(128 * 1024 * 1024)
}

/// Off. See [`Options::roll_after_seconds`].
fn default_roll_after_seconds() -> Option<u32> {
    None
}

/// 512 MiB — four default-sized parts. See
/// [`Options::max_open_bytes`].
fn default_max_open_bytes() -> Option<u64> {
    Some(512 * 1024 * 1024)
}

impl Default for Options {
    fn default() -> Self {
        Self {
            target_bytes: default_target_bytes(),
            roll_after_seconds: default_roll_after_seconds(),
            max_open_bytes: default_max_open_bytes(),
        }
    }
}

/// Why a `parts` block was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// `target_bytes: 0` — every batch would close its own part.
    #[error(
        "`target_bytes` is 0 — a part must hold something; remove the setting to use \
         the 128 MiB default, or give a positive size"
    )]
    ZeroTargetBytes,
    /// `roll_after_seconds: 0` — the same, in the time dimension.
    #[error(
        "`roll_after_seconds` is 0 — a part would close on every write; remove the \
         setting, or give a positive number of seconds"
    )]
    ZeroRollSeconds,
    /// `max_open_bytes: 0` — no part could hold anything.
    #[error(
        "`max_open_bytes` is 0 — open parts must be allowed some memory; remove the \
         setting to use the 512 MiB default, or give a positive size"
    )]
    ZeroMaxOpenBytes,
    /// A memory ceiling below the size parts are asked to reach: the
    /// target could never be met, and would be missed SILENTLY.
    #[error(
        "`max_open_bytes` ({budget}) is below `target_bytes` ({target}) — no part could \
         ever reach its target before the memory ceiling closed it; raise the ceiling, \
         or lower the target"
    )]
    BudgetBelowTarget {
        /// The configured memory ceiling.
        budget: u64,
        /// The configured part target it cannot accommodate.
        target: u64,
    },
}

impl Options {
    /// Refuse settings that cannot be honoured, naming the offender.
    ///
    /// Zero is the whole rulebook. `should_roll` already refuses to
    /// close an empty part, so a zero threshold would not spin — it
    /// would silently degrade to one part per batch, which is what
    /// `parts` exists to stop. Say so instead.
    pub fn validate(&self) -> Result<(), Error> {
        if self.target_bytes == Some(0) {
            return Err(Error::ZeroTargetBytes);
        }
        if self.roll_after_seconds == Some(0) {
            return Err(Error::ZeroRollSeconds);
        }
        match (self.max_open_bytes, self.target_bytes) {
            (Some(0), _) => return Err(Error::ZeroMaxOpenBytes),
            (Some(budget), Some(target)) if budget < target => {
                return Err(Error::BudgetBelowTarget { budget, target });
            }
            _ => {}
        }
        Ok(())
    }

    /// Never roll — no size target, no time bound, and NO memory
    /// ceiling either, because a ceiling splits parts exactly like a
    /// target does and "unbounded" must not quietly mean "512 MiB".
    ///
    /// NOT "one part per write" — a part still closes at every commit,
    /// because no part may span one. This is therefore ONE part per
    /// table, partition and commit, however much lands in it — and the
    /// caller accepts that an accumulating destination holds that much
    /// in MEMORY. The default exists precisely so nobody gets these
    /// semantics without asking.
    #[must_use]
    pub fn unbounded() -> Self {
        Self {
            target_bytes: None,
            roll_after_seconds: None,
            max_open_bytes: None,
        }
    }

    /// Close a part after EVERY write — the behaviour that existed
    /// before parts had a size.
    ///
    /// Spelled as a one-byte target rather than its own variant: a
    /// part that has taken a batch is always at least one byte, so the
    /// threshold trips on every write and on no empty one.
    #[must_use]
    pub fn per_write() -> Self {
        Self {
            target_bytes: Some(1),
            roll_after_seconds: None,
            max_open_bytes: default_max_open_bytes(),
        }
    }

    /// The size threshold as a destination that does its OWN rolling
    /// wants it: a plain byte count, with `None` spelled as "never".
    ///
    /// Iceberg hands this to the library's rolling file writer, which
    /// takes a `usize` and has no way to say "no limit" — so `None`
    /// becomes the largest value it can hold, which no file reaches.
    #[must_use]
    pub fn target_file_size(&self) -> usize {
        self.target_bytes
            .and_then(|bytes| usize::try_from(bytes).ok())
            .unwrap_or(usize::MAX)
    }

    /// Has an open part been open long enough to close on time alone?
    ///
    /// Split out of [`Options::should_roll`] for destinations that
    /// delegate SIZE to something else — the size half would be
    /// answered twice, and the two answers would disagree.
    #[must_use]
    pub fn rolls_on_time(&self, open_for_secs: u64) -> bool {
        self.roll_after_seconds
            .is_some_and(|secs| open_for_secs >= u64::from(secs))
    }

    /// Are the open parts together holding too much?
    ///
    /// Answered across ALL open parts rather than per part, because
    /// the thing being protected is the process, and one destination
    /// can hold a part open per table and partition at once.
    #[must_use]
    pub fn over_budget(&self, total_open_bytes: u64) -> bool {
        self.max_open_bytes
            .is_some_and(|budget| total_open_bytes > budget.max(1))
    }

    /// Should the open part be closed now?
    ///
    /// `true` when ANY threshold is reached — a disjunction, matching
    /// `CommitPolicy` and `BatchPolicy`.
    #[must_use]
    pub fn should_roll(&self, encoded_bytes: u64, open_for_secs: u64) -> bool {
        // `max(1)` so a zero threshold cannot close an EMPTY part and
        // spin: a part must hold something to be worth closing.
        self.target_bytes
            .is_some_and(|target| encoded_bytes >= target.max(1))
            || (encoded_bytes > 0
                && self
                    .roll_after_seconds
                    .is_some_and(|secs| open_for_secs >= u64::from(secs)))
    }
}

#[cfg(test)]
mod tests {
    use super::{Error, Options};

    /// The default is 128 MiB and no time bound.
    #[test]
    fn the_default_targets_128_mib() {
        let options = Options::default();
        assert_eq!(options.target_bytes, Some(128 * 1024 * 1024));
        assert_eq!(options.roll_after_seconds, None);
        assert!(!options.should_roll(1, 0));
        assert!(options.should_roll(128 * 1024 * 1024, 0));
    }

    /// Whichever threshold is reached first closes the part.
    #[test]
    fn any_threshold_alone_rolls() {
        let options = Options {
            target_bytes: Some(1_000),
            roll_after_seconds: Some(60),
            ..Options::default()
        };
        assert!(options.should_roll(1_000, 0), "size alone");
        assert!(options.should_roll(1, 60), "time alone");
        assert!(!options.should_roll(999, 59));
    }

    /// An EMPTY part is never rolled by time — closing nothing would
    /// mint empty files forever on an idle stream.
    #[test]
    fn time_never_rolls_an_empty_part() {
        let options = Options {
            target_bytes: None,
            roll_after_seconds: Some(1),
            ..Options::default()
        };
        assert!(!options.should_roll(0, 86_400));
        assert!(options.should_roll(1, 1));
    }

    /// Zero says "one part per batch" without saying so — refused.
    #[test]
    fn a_zero_threshold_is_refused_by_name() {
        let zero_bytes = Options {
            target_bytes: Some(0),
            ..Options::default()
        };
        assert_eq!(zero_bytes.validate(), Err(Error::ZeroTargetBytes));
        let zero_secs = Options {
            roll_after_seconds: Some(0),
            ..Options::default()
        };
        assert_eq!(zero_secs.validate(), Err(Error::ZeroRollSeconds));
        assert_eq!(Options::default().validate(), Ok(()));
        assert_eq!(Options::unbounded().validate(), Ok(()));
    }

    #[test]
    fn unbounded_never_rolls() {
        let options = Options::unbounded();
        assert!(!options.should_roll(u64::MAX, u64::MAX));
        // Including the memory ceiling: an earlier constructor kept the
        // 512 MiB default, which split parts on the accumulating
        // destinations while the doc promised one part per commit
        // "however much lands in it".
        assert!(!options.over_budget(u64::MAX));
    }

    /// The ceiling is answered across ALL open parts, and a ceiling
    /// below the target is refused rather than silently missing it.
    #[test]
    fn the_memory_ceiling_is_a_total_and_must_admit_the_target() {
        let options = Options::default();
        assert!(!options.over_budget(512 * 1024 * 1024));
        assert!(options.over_budget(512 * 1024 * 1024 + 1));

        let contradiction = Options {
            target_bytes: Some(1_024),
            max_open_bytes: Some(512),
            ..Options::default()
        };
        assert_eq!(
            contradiction.validate(),
            Err(Error::BudgetBelowTarget {
                budget: 512,
                target: 1_024,
            })
        );

        // No ceiling means no budget question to answer.
        let uncapped = Options {
            max_open_bytes: None,
            ..Options::default()
        };
        assert!(!uncapped.over_budget(u64::MAX));
        assert_eq!(uncapped.validate(), Ok(()));
    }

    /// `per_write` rolls on any content and on no empty part.
    #[test]
    fn per_write_rolls_on_every_written_batch() {
        let options = Options::per_write();
        assert!(options.should_roll(1, 0));
        assert!(!options.should_roll(0, 0));
        assert_eq!(options.validate(), Ok(()));
    }

    /// The generated schema keeps the platform-facing name stable
    /// across the module-canonical Rust name — a bare `Options` would
    /// also collide with the `parquet` module's `Options` in a config
    /// schema's one `$defs` map.
    #[test]
    fn the_schema_name_stays_part_qualified() {
        let schema =
            serde_json::to_value(schemars::schema_for!(Options)).expect("a schema serializes");
        assert_eq!(schema["title"], "PartOptions", "{schema}");
    }

    /// The serde-default trap (documented on the parquet options beside it): an
    /// omitted field must take the DOCUMENTED default, not the field
    /// type's.
    #[test]
    fn omitted_fields_take_the_documented_defaults() {
        let options: Options = serde_json::from_str("{}").expect("empty document");
        assert_eq!(options, Options::default());
        let sized: Options =
            serde_json::from_str(r#"{"target_bytes": 1024}"#).expect("partial document");
        assert_eq!(sized.target_bytes, Some(1024));
        assert_eq!(sized.roll_after_seconds, None);
    }
}
