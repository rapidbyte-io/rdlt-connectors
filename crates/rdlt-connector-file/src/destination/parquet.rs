//! How this destination writes parquet: the vocabulary the file destination's
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

/// Parquet codecs, spelled as configuration writes them.
///
/// `#[non_exhaustive]`: parquet grows codecs, and adding one must not
/// break anyone matching on this.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
#[derive(schemars::JsonSchema)]
#[schemars(rename = "ParquetCompression")]
#[non_exhaustive]
pub enum Compression {
    /// No compression.
    Uncompressed,
    /// The default: consistently the best speed-for-size trade on this
    /// workload, and what the ecosystem assumes a parquet file carries.
    #[default]
    Snappy,
    /// Widely readable, slower than Snappy; accepts a level.
    Gzip,
    /// The frame-less LZ4 variant parquet readers expect. No level.
    Lz4Raw,
    /// Smaller than Snappy at more CPU; accepts a level.
    Zstd,
    /// Smallest of these at the most CPU; accepts a level.
    Brotli,
}

impl Compression {
    /// Whether this codec has a level to set.
    ///
    /// Snappy and LZ4_RAW define a single mode — naming a level beside
    /// them is a mistake worth refusing, because silently dropping it
    /// would leave the user believing they tuned something.
    pub fn takes_level(self) -> bool {
        matches!(self, Self::Gzip | Self::Zstd | Self::Brotli)
    }

    /// The inclusive level window this codec accepts — `None` for the
    /// levelless three. The windows are the parquet library's own
    /// (parquet 58 `GzipLevel`/`BrotliLevel`/`ZstdLevel`): its setters
    /// PANIC one step outside them, which is why [`Options::validate`]
    /// refuses the range here first.
    pub fn level_range(self) -> Option<(i32, i32)> {
        match self {
            Self::Gzip => Some((0, 9)),
            Self::Brotli => Some((0, 11)),
            Self::Zstd => Some((1, 22)),
            Self::Uncompressed | Self::Snappy | Self::Lz4Raw => None,
        }
    }

    /// The configuration spelling, for error messages and diagnostics.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Uncompressed => "uncompressed",
            Self::Snappy => "snappy",
            Self::Gzip => "gzip",
            Self::Lz4Raw => "lz4_raw",
            Self::Zstd => "zstd",
            Self::Brotli => "brotli",
        }
    }
}

/// The default dictionary page limit: 64 KiB, 16× below parquet's own
/// 1 MiB default.
///
/// Chosen by a recorded sweep (200k rows, all-distinct string column,
/// snappy, median of 5), not by taste: high-cardinality encoding is flat
/// from 4 KiB to 64 KiB and degrades sharply above — at 1 MiB it costs
/// 68% more CPU and produces a LARGER file — while low-cardinality
/// encoding is flat across the whole range, so a lower cap takes nothing
/// from the columns dictionaries actually help. 64 KiB is the TOP of the
/// flat region on purpose: 4 and 16 KiB are no faster, and a smaller cap
/// would abandon dictionary encoding for medium-cardinality columns that
/// 64 KiB still serves.
const DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT: usize = 64 * 1024;

const fn default_compression() -> Compression {
    Compression::Snappy
}

const fn default_dictionary_enabled() -> bool {
    true
}

const fn default_dictionary_page_size_limit() -> usize {
    DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT
}

/// How to write parquet. Every field is optional in configuration and
/// falls back to its documented default.
///
/// # The serde-default trap
///
/// Every field names its own default function. A bare `#[serde(default)]`
/// would silently invert the intent: it calls `Default::default()` on the
/// FIELD TYPE, so an omitted `dictionary_enabled` would come back `false`
/// and omitted limits `0`. The struct's own `Default` impl delegates to
/// the same functions so the two paths cannot drift.
///
/// Deserialization is the DERIVED one: a document whose shape is wrong
/// fails here, and a document whose VALUES are wrong fails at
/// [`Options::validate`], which every connector's config gate calls.
/// Validating inside `Deserialize` would report a semantic refusal
/// through the parse arm of a connector's error taxonomy — the document
/// parsed; it is its contents that are wrong, and the two deserve
/// different answers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[derive(schemars::JsonSchema)]
#[schemars(rename = "ParquetOptions")]
pub struct Options {
    /// Compression codec; defaults to `snappy`.
    #[serde(default = "default_compression")]
    pub compression: Compression,

    /// Compression level, for codecs that have one — refused for codecs
    /// that do not (see [`Compression::takes_level`]). `None` leaves the
    /// codec's own default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression_level: Option<i32>,

    /// Whether to dictionary-encode at all. On by default, as in parquet
    /// itself; off suits data known to be high-cardinality throughout.
    #[serde(default = "default_dictionary_enabled")]
    pub dictionary_enabled: bool,

    /// Bytes a column's dictionary page may reach before that column
    /// abandons dictionary encoding for the rest of the row group.
    #[serde(default = "default_dictionary_page_size_limit")]
    pub dictionary_page_size_limit: usize,

    /// Target data-page size in bytes. `None` leaves the library default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_page_size_limit: Option<usize>,

    /// Maximum ROWS per row group; `None` leaves the library's default
    /// (1,048,576).
    ///
    /// Rows, not bytes — parquet 58 deprecated the byte-oriented setter.
    /// Two facts about the library's row-count setter shape the code that
    /// consumes this field: its `None` means UNLIMITED (not "default"),
    /// so a translator must skip the call entirely when this is `None`;
    /// and it panics on `Some(0)`, which is why zero is refused in
    /// [`Options::validate`] before it can reach the panic.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_row_group_rows: Option<usize>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            compression: default_compression(),
            compression_level: None,
            dictionary_enabled: default_dictionary_enabled(),
            dictionary_page_size_limit: default_dictionary_page_size_limit(),
            data_page_size_limit: None,
            max_row_group_rows: None,
        }
    }
}

/// A parquet setting that cannot be honoured, named by what failed.
///
/// `#[non_exhaustive]`: validation can learn new refusals without a
/// breaking change.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A `compression_level` beside a codec that defines a single mode.
    #[error(
        "`compression_level` is set but `{codec}` has no compression level — \
         remove the level, or choose a codec that takes one (gzip, zstd, brotli)"
    )]
    LevelOnLevellessCodec {
        /// The configuration spelling of the levelless codec.
        codec: &'static str,
    },
    /// A `compression_level` outside its codec's accepted window,
    /// which the parquet level constructors would panic on.
    #[error(
        "`compression_level` {level} is outside `{codec}`'s accepted range {low}..={high} — \
         pick a level inside the range, or remove the setting to use the codec's default"
    )]
    LevelOutOfRange {
        /// The configuration spelling of the codec.
        codec: &'static str,
        /// The refused level.
        level: i32,
        /// The window's inclusive low edge.
        low: i32,
        /// The window's inclusive high edge.
        high: i32,
    },
    /// `max_row_group_rows: 0`, which the parquet setter would panic on.
    #[error(
        "`max_row_group_rows` is 0 — a row group must hold at least one row; \
         remove the setting to use the default, or give a positive count"
    )]
    ZeroRowGroupRows,
    /// A zero-byte dictionary page while dictionary encoding is enabled.
    #[error(
        "`dictionary_page_size_limit` is 0 while dictionary encoding is enabled — \
         a dictionary page cannot be zero bytes; raise the limit, or set \
         `dictionary_enabled: false` to disable dictionary encoding outright"
    )]
    ZeroDictionaryPageLimit,
}

impl Options {
    /// Refuse settings that cannot be honoured, naming the offender.
    ///
    /// Only rules decidable from these fields alone live here; whether a
    /// `parquet` block belongs on a destination at all depends on that
    /// destination's sibling `format` field, so that rule lives where
    /// `format` is in scope.
    pub fn validate(&self) -> Result<(), Error> {
        if self.compression_level.is_some() && !self.compression.takes_level() {
            return Err(Error::LevelOnLevellessCodec {
                codec: self.compression.as_str(),
            });
        }
        if let (Some(level), Some((low, high))) =
            (self.compression_level, self.compression.level_range())
            && !(low..=high).contains(&level)
        {
            return Err(Error::LevelOutOfRange {
                codec: self.compression.as_str(),
                level,
                low,
                high,
            });
        }
        if self.max_row_group_rows == Some(0) {
            return Err(Error::ZeroRowGroupRows);
        }
        if self.dictionary_enabled && self.dictionary_page_size_limit == 0 {
            return Err(Error::ZeroDictionaryPageLimit);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The trap this type exists to dodge: omitted fields take the
    /// DOCUMENTED defaults, not the field types' `Default`s (which would
    /// be `false` and `0`).
    #[test]
    fn an_empty_block_takes_the_documented_defaults() {
        let parsed: Options = serde_json::from_str("{}").expect("empty block valid");
        assert_eq!(parsed, Options::default());
        assert_eq!(parsed.compression, Compression::Snappy);
        assert!(parsed.dictionary_enabled, "must not default to false");
        assert_eq!(
            parsed.dictionary_page_size_limit, DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT,
            "must not default to 0"
        );
        // The constant itself is a measured decision; drifting it should
        // be a deliberate act, not a refactor side effect.
        assert_eq!(DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT, 65_536);
    }

    #[test]
    fn a_partial_block_keeps_the_defaults_it_leaves_out() {
        let parsed: Options = serde_json::from_str(r#"{"compression": "zstd"}"#).expect("valid");
        assert_eq!(parsed.compression, Compression::Zstd);
        assert!(parsed.dictionary_enabled);
        assert_eq!(
            parsed.dictionary_page_size_limit,
            DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT
        );
    }

    #[test]
    fn unknown_settings_are_refused_naming_the_typo() {
        let error = serde_json::from_str::<Options>(r#"{"compresion": "zstd"}"#)
            .expect_err("typos must not be dropped");
        assert!(error.to_string().contains("compresion"), "{error}");
    }

    /// Where each kind of wrongness is answered: SHAPE at
    /// deserialization, VALUES at the gate. A document naming a
    /// setting that does not exist has not been understood and fails
    /// where it is read; a document whose settings are all real but
    /// one of them impossible parsed perfectly well, and its refusal
    /// belongs to the caller's config gate — which frames it as the
    /// configuration error it is, rather than reporting a semantic
    /// problem through a parse arm.
    #[test]
    fn shape_is_refused_at_the_parse_and_values_at_the_gate() {
        let parsed: Options = serde_json::from_str(r#"{"max_row_group_rows": 0}"#)
            .expect("a zero row-group count is well-SHAPED — the parse has no quarrel with it");
        let refused = parsed
            .validate()
            .expect_err("the gate is where an impossible value is answered");
        assert!(
            refused.to_string().contains("max_row_group_rows"),
            "{refused}"
        );

        let parsed: Options =
            serde_json::from_str(r#"{"compression": "snappy", "compression_level": 3}"#)
                .expect("a level beside a levelless codec is well-shaped too");
        let refused = parsed.validate().expect_err("snappy takes no level");
        assert!(refused.to_string().contains("snappy"), "{refused}");
    }

    #[test]
    fn a_level_on_a_levelless_codec_is_refused_naming_the_codec() {
        let refused = Options {
            compression: Compression::Snappy,
            compression_level: Some(3),
            ..Default::default()
        }
        .validate()
        .expect_err("snappy has no level")
        .to_string();
        assert!(
            refused.contains("snappy") && refused.contains("compression_level"),
            "{refused}"
        );
        assert!(
            Options {
                compression: Compression::Zstd,
                compression_level: Some(3),
                ..Default::default()
            }
            .validate()
            .is_ok(),
            "the same level is fine on a levelled codec"
        );
    }

    /// Zero is refused HERE because the parquet setter panics on it — a
    /// library panic is no way to report a configuration mistake.
    #[test]
    fn zero_row_group_rows_is_refused_before_the_library_can_panic() {
        let refused = Options {
            max_row_group_rows: Some(0),
            ..Default::default()
        }
        .validate()
        .expect_err("zero rows per group is impossible")
        .to_string();
        assert!(refused.contains("max_row_group_rows"), "{refused}");
        assert!(
            Options {
                max_row_group_rows: Some(1),
                ..Default::default()
            }
            .validate()
            .is_ok()
        );
    }

    #[test]
    fn zero_dictionary_limit_is_refused_only_while_encoding_is_enabled() {
        let refused = Options {
            dictionary_page_size_limit: 0,
            ..Default::default()
        }
        .validate()
        .expect_err("an enabled zero-byte dictionary is impossible")
        .to_string();
        assert!(refused.contains("dictionary_page_size_limit"), "{refused}");
        assert!(
            Options {
                dictionary_enabled: false,
                dictionary_page_size_limit: 0,
                ..Default::default()
            }
            .validate()
            .is_ok(),
            "with encoding off the limit is inert"
        );
    }

    /// Each levelled codec's window edges, refused one past each edge
    /// and accepted AT each edge — the parquet setters panic on an
    /// out-of-window level, and a library panic is no way to report a
    /// configuration mistake (the `ZeroRowGroupRows` rule, applied to
    /// levels). The windows are parquet 58's own: GzipLevel 0..=9,
    /// BrotliLevel 0..=11, ZstdLevel 1..=22.
    #[test]
    fn an_out_of_range_level_is_refused_per_codec_before_the_library_can_panic() {
        for (codec, low, high) in [
            (Compression::Gzip, 0, 9),
            (Compression::Brotli, 0, 11),
            (Compression::Zstd, 1, 22),
        ] {
            for edge in [low, high] {
                assert!(
                    Options {
                        compression: codec,
                        compression_level: Some(edge),
                        ..Default::default()
                    }
                    .validate()
                    .is_ok(),
                    "{} accepts its edge level {edge}",
                    codec.as_str()
                );
            }
            for outside in [low - 1, high + 1] {
                let refused = Options {
                    compression: codec,
                    compression_level: Some(outside),
                    ..Default::default()
                }
                .validate()
                .expect_err("an out-of-window level is impossible to honour")
                .to_string();
                assert!(
                    refused.contains(codec.as_str())
                        && refused.contains(&outside.to_string())
                        && refused.contains(&format!("{low}..={high}")),
                    "the refusal names the codec, the level, and the range: {refused}"
                );
            }
        }
    }

    #[test]
    fn levelled_codecs_are_exactly_the_three_that_take_a_level() {
        for levelled in [Compression::Gzip, Compression::Zstd, Compression::Brotli] {
            assert!(levelled.takes_level(), "{}", levelled.as_str());
        }
        for levelless in [
            Compression::Uncompressed,
            Compression::Snappy,
            Compression::Lz4Raw,
        ] {
            assert!(!levelless.takes_level(), "{}", levelless.as_str());
        }
    }

    /// The generated schema keeps the platform-facing names stable
    /// across the module-canonical Rust names — a bare `Options` would
    /// also collide with the `parts` module's `Options` in a config
    /// schema's one `$defs` map.
    #[test]
    fn schema_names_stay_parquet_qualified() {
        let schema =
            serde_json::to_value(schemars::schema_for!(Options)).expect("a schema serializes");
        assert_eq!(schema["title"], "ParquetOptions", "{schema}");
        assert!(
            schema["$defs"]["ParquetCompression"].is_object(),
            "{schema}"
        );
    }

    /// Config documents keep their spelling in and out.
    #[test]
    fn codec_spellings_round_trip_as_written() {
        for (spelling, codec) in [
            ("uncompressed", Compression::Uncompressed),
            ("snappy", Compression::Snappy),
            ("gzip", Compression::Gzip),
            ("lz4_raw", Compression::Lz4Raw),
            ("zstd", Compression::Zstd),
            ("brotli", Compression::Brotli),
        ] {
            let parsed: Compression =
                serde_json::from_str(&format!("\"{spelling}\"")).expect(spelling);
            assert_eq!(parsed, codec);
            assert_eq!(
                serde_json::to_string(&codec).expect(spelling),
                format!("\"{spelling}\"")
            );
            assert_eq!(codec.as_str(), spelling);
        }
    }
}
