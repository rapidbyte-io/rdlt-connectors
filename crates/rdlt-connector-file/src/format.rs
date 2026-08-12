//! The format vocabulary both halves speak, and the per-file
//! compression rule.
//!
//! A file's FORMAT is declared, never inferred from its name; its
//! COMPRESSION is the opposite — decided per file by extension, with
//! the magic bytes checked at open so a mislabelled file fails loudly
//! instead of parsing garbage.

/// The declared record format of a stream or destination.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Format {
    Jsonl,
    Parquet,
    Csv,
}

/// Whole-file compression, decided by extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Codec {
    Plain,
    Gzip,
    Zstd,
}

impl Codec {
    pub(crate) fn is_plain(self) -> bool {
        self == Codec::Plain
    }
}

/// The per-file rule: `.gz` is gzip, `.zst` is zstd, anything else is
/// plain bytes.
pub(crate) fn codec_of(path: &str) -> Codec {
    if path.ends_with(".gz") {
        Codec::Gzip
    } else if path.ends_with(".zst") {
        Codec::Zstd
    } else {
        Codec::Plain
    }
}

/// The magic bytes each codec's extension promises. Checked at open:
/// a name that lies fails with the observed bytes in the message.
pub(crate) const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];
pub(crate) const ZSTD_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];

/// The shared read/write slab: 8 MiB.
pub(crate) const SLAB_BYTES: usize = 8 << 20;

#[cfg(test)]
mod tests {
    use super::*;

    /// The extension rule is exact — including compound extensions,
    /// where only the LAST one speaks.
    #[test]
    fn the_codec_is_decided_by_the_last_extension() {
        assert_eq!(codec_of("a/b.jsonl"), Codec::Plain);
        assert_eq!(codec_of("a/b.jsonl.gz"), Codec::Gzip);
        assert_eq!(codec_of("a/b.csv.zst"), Codec::Zstd);
        assert_eq!(codec_of("a/b.parquet"), Codec::Plain);
        assert_eq!(codec_of("gz"), Codec::Plain, "no extension, no codec");
    }

    /// The format spellings round-trip as written.
    #[test]
    fn the_format_spellings_round_trip() {
        for (text, format) in [
            ("jsonl", Format::Jsonl),
            ("parquet", Format::Parquet),
            ("csv", Format::Csv),
        ] {
            let parsed: Format = serde_yaml::from_str(text).expect("parses");
            assert_eq!(parsed, format);
            assert_eq!(
                serde_yaml::to_string(&format).expect("renders").trim(),
                text
            );
        }
    }
}
