//! The mapping face: a catalog type (plus an optional user hint) → its
//! [`Kind`], its projection, and its capabilities.
//!
//! Every reflected column gets a [`Mapping`]: how the SELECT projects it
//! ([`Projection`] — policy conversions happen SERVER-side so the binary
//! COPY stream only ever carries the lossless decode set), and the kind the
//! decoders build. Nothing falls through to inference: unknown types take
//! the textual fallback, visibly.

use super::Kind;

/// Stable pg_type OIDs for the lossless decode set (`pg_type.dat`, unchanged
/// across all supported PG versions).
pub(crate) mod oid {
    pub(crate) const BOOL: u32 = 16;
    pub(crate) const BYTEA: u32 = 17;
    pub(crate) const NAME: u32 = 19;
    pub(crate) const INT8: u32 = 20;
    pub(crate) const INT2: u32 = 21;
    pub(crate) const INT4: u32 = 23;
    pub(crate) const TEXT: u32 = 25;
    pub(crate) const JSON: u32 = 114;
    pub(crate) const FLOAT4: u32 = 700;
    pub(crate) const FLOAT8: u32 = 701;
    pub(crate) const BPCHAR: u32 = 1042;
    pub(crate) const VARCHAR: u32 = 1043;
    pub(crate) const DATE: u32 = 1082;
    pub(crate) const TIME: u32 = 1083;
    pub(crate) const TIMESTAMP: u32 = 1114;
    pub(crate) const TIMESTAMPTZ: u32 = 1184;
    pub(crate) const NUMERIC: u32 = 1700;
    pub(crate) const UUID: u32 = 2950;
    pub(crate) const JSONB: u32 = 3802;
}

/// How the SELECT projects the column inside `COPY (SELECT …)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Projection {
    /// The bare quoted column — its binary wire format is decoded natively.
    Direct,
    /// `column::text` — the canonical-text policy rows (enum, interval,
    /// money, unconstrained numeric, textual fallback…).
    CastText,
    /// `to_jsonb(column)::text` — arrays / composites / ranges.
    CastJsonbText,
    /// `(column)::<target>` — a per-column type hint's server-side cast.
    Cast(String),
}

/// The complete per-column decision the reflection layer emits.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Mapping {
    pub(crate) projection: Projection,
    pub(crate) kind: Kind,
    /// Accepted for `cursor.column` — the cursor-capable kinds.
    pub(crate) cursor_capable: bool,
    /// True for the policy rows whose REPRESENTATION changes (values always
    /// survive) — surfaced on `rdlt::lossy` so the change is visible, never
    /// silent.
    pub(crate) documented_lossy: bool,
}

/// The shape facts reflection extracts from `pg_type` for mapping decisions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogType {
    pub(crate) oid: u32,
    /// `pg_type.typtype`: b=base, e=enum, d=domain (pre-resolved by
    /// reflection to its base — never reaches here), c=composite, r=range,
    /// m=multirange, p=pseudo.
    pub(crate) typtype: char,
    /// `pg_type.typcategory`: 'A' = array.
    pub(crate) typcategory: char,
    /// `pg_attribute.atttypmod` (numeric precision/scale live here).
    pub(crate) typmod: i32,
}

/// numeric typmod → (precision, scale); None for unconstrained
/// (`typmod = -1`).
fn numeric_precision_scale(typmod: i32) -> Option<(i32, i32)> {
    if typmod < 4 {
        return None;
    }
    let packed = typmod - 4;
    // Scale sits in the low 16 bits; PG ≥ 15 allows negative scales, stored
    // two's-complement in those bits.
    let precision = (packed >> 16) & 0xFFFF;
    let scale = ((packed & 0x7FF) ^ 1024) - 1024;
    Some((precision, scale))
}

fn lossless(projection: Projection, kind: Kind, cursor_capable: bool) -> Mapping {
    Mapping {
        projection,
        kind,
        cursor_capable,
        documented_lossy: false,
    }
}

fn text_policy(projection: Projection) -> Mapping {
    Mapping {
        projection,
        kind: Kind::Text,
        cursor_capable: false,
        documented_lossy: true,
    }
}

/// The binding type mapping. Total: every input maps; the last arm is the
/// textual fallback (visible, documented — never inference).
pub(crate) fn map(catalog: &CatalogType) -> Mapping {
    use Projection::{CastJsonbText, CastText, Direct};

    // Policy shapes first: arrays, composites, ranges → canonical JSON text.
    if catalog.typcategory == 'A' || matches!(catalog.typtype, 'c' | 'r' | 'm') {
        return text_policy(CastJsonbText);
    }
    // Enum labels → text.
    if catalog.typtype == 'e' {
        return text_policy(CastText);
    }

    match catalog.oid {
        oid::BOOL => lossless(Direct, Kind::Bool, false),
        oid::INT2 => lossless(Direct, Kind::Int16, true),
        oid::INT4 => lossless(Direct, Kind::Int32, true),
        oid::INT8 => lossless(Direct, Kind::Int64, true),
        oid::FLOAT4 => lossless(Direct, Kind::Float32, false),
        oid::FLOAT8 => lossless(Direct, Kind::Float64, false),
        oid::NUMERIC => match numeric_precision_scale(catalog.typmod) {
            Some((precision, scale))
                if (1..=38).contains(&precision) && scale >= 0 && scale <= precision =>
            {
                lossless(
                    Direct,
                    Kind::Decimal {
                        precision: precision as u8,
                        scale: scale as u8,
                    },
                    true,
                )
            }
            // Unconstrained, oversized, or negative-scale numeric: canonical
            // text — no precision loss, ever [documented-lossy].
            _ => text_policy(CastText),
        },
        oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME => lossless(Direct, Kind::Text, true),
        oid::BYTEA => lossless(Direct, Kind::Bytea, false),
        oid::TIMESTAMPTZ => lossless(Direct, Kind::TimestampTz, true),
        oid::TIMESTAMP => lossless(Direct, Kind::TimestampNaive, true),
        oid::DATE => lossless(Direct, Kind::Date, true),
        oid::TIME => lossless(Direct, Kind::Time, true),
        // uuid: canonical text (the structured path derives logical types
        // from Arrow, and Arrow carries no uuid). Lossless in value.
        oid::UUID => lossless(Direct, Kind::Uuid, true),
        // json arrives as its text; jsonb carries a leading version byte.
        oid::JSON => lossless(Direct, Kind::Text, false),
        oid::JSONB => lossless(Direct, Kind::Jsonb, false),
        // Everything else — interval, timetz, money, inet/cidr/macaddr, xml,
        // tsvector, …: the textual fallback [documented-lossy].
        _ => text_policy(CastText),
    }
}

/// Per-column hint vocabulary — shared naming with the rest/file sources,
/// plus `decimal(p,s)`. Config form is the literal string vocabulary:
/// `"timestamp_tz"`, `"decimal(12,4)"`, …
///
/// Public via `source::config` (the config document is where hints live);
/// the conversion semantics stay here beside the mapping they refine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeHint {
    Bool,
    Int64,
    Float64,
    Decimal { precision: u8, scale: u8 },
    Utf8,
    Binary,
    TimestampTz,
    TimestampNaive,
    Date,
    Time,
    Uuid,
    Json,
}

impl std::str::FromStr for TypeHint {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let trimmed = text.trim();
        Ok(match trimmed {
            "bool" => Self::Bool,
            "int64" => Self::Int64,
            "float64" => Self::Float64,
            "utf8" => Self::Utf8,
            "binary" => Self::Binary,
            "timestamp_tz" => Self::TimestampTz,
            "timestamp_naive" => Self::TimestampNaive,
            "date" => Self::Date,
            "time" => Self::Time,
            "uuid" => Self::Uuid,
            "json" => Self::Json,
            other => {
                let arguments = other
                    .strip_prefix("decimal(")
                    .and_then(|rest| rest.strip_suffix(')'))
                    .ok_or_else(|| format!("unknown type hint `{other}`"))?;
                let (precision, scale) = arguments
                    .split_once(',')
                    .ok_or_else(|| format!("decimal hint needs `decimal(p,s)`, got `{other}`"))?;
                Self::Decimal {
                    precision: precision
                        .trim()
                        .parse()
                        .map_err(|e| format!("decimal precision: {e}"))?,
                    scale: scale
                        .trim()
                        .parse()
                        .map_err(|e| format!("decimal scale: {e}"))?,
                }
            }
        })
    }
}

impl std::fmt::Display for TypeHint {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bool => formatter.write_str("bool"),
            Self::Int64 => formatter.write_str("int64"),
            Self::Float64 => formatter.write_str("float64"),
            Self::Decimal { precision, scale } => write!(formatter, "decimal({precision},{scale})"),
            Self::Utf8 => formatter.write_str("utf8"),
            Self::Binary => formatter.write_str("binary"),
            Self::TimestampTz => formatter.write_str("timestamp_tz"),
            Self::TimestampNaive => formatter.write_str("timestamp_naive"),
            Self::Date => formatter.write_str("date"),
            Self::Time => formatter.write_str("time"),
            Self::Uuid => formatter.write_str("uuid"),
            Self::Json => formatter.write_str("json"),
        }
    }
}

impl serde::Serialize for TypeHint {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for TypeHint {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

impl schemars::JsonSchema for TypeHint {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "TypeHint".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        // Mirrors `FromStr` exactly — the string vocabulary IS the config
        // form.
        schemars::json_schema!({
            "type": "string",
            "description": "Type-hint vocabulary: a \
                            fixed name or `decimal(p,s)`",
            "pattern": "^(bool|int64|float64|utf8|binary|timestamp_tz|\
                        timestamp_naive|date|time|uuid|json|\
                        decimal\\([0-9]+, ?[0-9]+\\))$"
        })
    }
}

/// The CLOSED conversion table: every allowed (source type → hint) pair
/// yields the server-side cast + kind; anything else is an error the caller
/// surfaces as a typed config failure at open.
pub(crate) fn apply_hint(catalog: &CatalogType, hint: TypeHint) -> Result<Mapping, String> {
    // A hint applies only to a PLAIN base scalar of the named oid — never an
    // array element (`typcategory 'A'`) or an enum/composite/range
    // (`typtype != 'b'`), which carry their own mapping.
    let plain_base = |oids: &[u32]| {
        oids.contains(&catalog.oid) && catalog.typtype == 'b' && catalog.typcategory != 'A'
    };
    let text_family = plain_base(&[oid::TEXT, oid::VARCHAR, oid::BPCHAR, oid::NAME]);
    let int_family = plain_base(&[oid::INT2, oid::INT4, oid::INT8]);
    let float_family = plain_base(&[oid::FLOAT4, oid::FLOAT8]);
    let numeric = plain_base(&[oid::NUMERIC]);
    let timestamp_family = plain_base(&[oid::TIMESTAMP, oid::TIMESTAMPTZ]);

    // Cast expression per target; the kind matches the lossless decode set.
    let cast = |target: &str, kind: Kind, cursor_capable: bool, documented_lossy: bool| {
        Ok(Mapping {
            projection: Projection::Cast(target.to_string()),
            kind,
            cursor_capable,
            documented_lossy,
        })
    };
    match hint {
        // Universal row: ANY type → utf8 canonical text.
        TypeHint::Utf8 => Ok(Mapping {
            projection: if matches!(map(catalog).projection, Projection::CastJsonbText) {
                Projection::CastJsonbText
            } else {
                Projection::CastText
            },
            kind: Kind::Text,
            cursor_capable: true,
            documented_lossy: true,
        }),
        TypeHint::Int64 if text_family => cast("int8", Kind::Int64, true, false),
        TypeHint::Float64 if text_family || int_family || numeric => cast(
            "float8",
            Kind::Float64,
            false,
            numeric, // numeric → float64 is [documented-lossy]
        ),
        TypeHint::Decimal { precision, scale }
            if (text_family || int_family || float_family || numeric)
                && (1..=38).contains(&precision)
                && scale <= precision =>
        {
            cast(
                &format!("numeric({precision},{scale})"),
                Kind::Decimal { precision, scale },
                true,
                float_family, // float rounding per SQL rules is [documented-lossy]
            )
        }
        TypeHint::Bool if text_family || int_family => cast("bool", Kind::Bool, false, false),
        TypeHint::TimestampTz
            if text_family || matches!(catalog.oid, oid::TIMESTAMP | oid::DATE) =>
        {
            cast(
                "timestamptz",
                Kind::TimestampTz,
                true,
                catalog.oid == oid::TIMESTAMP, // zone semantics change
            )
        }
        TypeHint::TimestampNaive
            if text_family || matches!(catalog.oid, oid::TIMESTAMPTZ | oid::DATE) =>
        {
            cast(
                "timestamp",
                Kind::TimestampNaive,
                true,
                catalog.oid == oid::TIMESTAMPTZ,
            )
        }
        TypeHint::Date if text_family || timestamp_family => cast(
            "date",
            Kind::Date,
            true,
            timestamp_family, // truncation to date
        ),
        TypeHint::Time if text_family => cast("time", Kind::Time, true, false),
        TypeHint::Uuid if text_family => cast("uuid", Kind::Uuid, true, false),
        TypeHint::Json if text_family => cast("jsonb", Kind::Jsonb, false, false),
        TypeHint::Binary if text_family => cast("bytea", Kind::Bytea, false, false),
        other => Err(format!(
            "no defined conversion from this column's type (oid {}) to hint {:?} — \
             the type-hint conversion table is closed; `utf8` is always available",
            catalog.oid, other
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, TimeUnit};

    fn base(oid: u32, typmod: i32) -> CatalogType {
        CatalogType {
            oid,
            typtype: 'b',
            typcategory: 'X',
            typmod,
        }
    }

    #[test]
    fn lossless_rows() {
        let cases: &[(u32, DataType, Kind)] = &[
            (oid::BOOL, DataType::Boolean, Kind::Bool),
            (oid::INT2, DataType::Int64, Kind::Int16),
            (oid::INT4, DataType::Int64, Kind::Int32),
            (oid::INT8, DataType::Int64, Kind::Int64),
            (oid::FLOAT4, DataType::Float64, Kind::Float32),
            (oid::FLOAT8, DataType::Float64, Kind::Float64),
            (oid::TEXT, DataType::Utf8, Kind::Text),
            (oid::VARCHAR, DataType::Utf8, Kind::Text),
            (oid::BPCHAR, DataType::Utf8, Kind::Text),
            (oid::NAME, DataType::Utf8, Kind::Text),
            (oid::BYTEA, DataType::Binary, Kind::Bytea),
            (oid::DATE, DataType::Date32, Kind::Date),
            (
                oid::TIME,
                DataType::Time64(TimeUnit::Microsecond),
                Kind::Time,
            ),
            (oid::UUID, DataType::Utf8, Kind::Uuid),
            (oid::JSON, DataType::Utf8, Kind::Text),
            (oid::JSONB, DataType::Utf8, Kind::Jsonb),
        ];
        for (o, arrow, kind) in cases {
            let mapping = map(&base(*o, -1));
            assert_eq!(&mapping.kind.arrow(), arrow, "oid {o}");
            assert_eq!(&mapping.kind, kind, "oid {o}");
            assert_eq!(mapping.projection, Projection::Direct, "oid {o}");
            assert!(!mapping.documented_lossy, "oid {o}");
        }
        assert_eq!(
            map(&base(oid::TIMESTAMPTZ, -1)).kind.arrow(),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(
            map(&base(oid::TIMESTAMP, -1)).kind.arrow(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn numeric_constrained_is_decimal() {
        // numeric(10,2): typmod = ((10 << 16) | 2) + 4
        let typmod = ((10 << 16) | 2) + 4;
        let mapping = map(&base(oid::NUMERIC, typmod));
        assert_eq!(
            mapping.kind,
            Kind::Decimal {
                precision: 10,
                scale: 2
            }
        );
        assert_eq!(mapping.kind.arrow(), DataType::Decimal128(10, 2));
        assert!(mapping.cursor_capable);
    }

    #[test]
    fn policy_rows() {
        // Unconstrained numeric → text.
        let unconstrained = map(&base(oid::NUMERIC, -1));
        assert_eq!(unconstrained.projection, Projection::CastText);
        assert!(unconstrained.documented_lossy);
        // Oversized precision numeric(50,2) → text.
        let typmod = ((50 << 16) | 2) + 4;
        assert_eq!(
            map(&base(oid::NUMERIC, typmod)).projection,
            Projection::CastText
        );
        // Negative scale (PG15+) → text.
        let typmod = ((10 << 16) | ((-2i32) & 0x7FF)) + 4;
        assert_eq!(
            map(&base(oid::NUMERIC, typmod)).projection,
            Projection::CastText
        );
        // Enum → label text.
        let label = map(&CatalogType {
            oid: 99_999,
            typtype: 'e',
            typcategory: 'E',
            typmod: -1,
        });
        assert_eq!(label.projection, Projection::CastText);
        // Array → canonical JSON text.
        let array = map(&CatalogType {
            oid: 1007,
            typtype: 'b',
            typcategory: 'A',
            typmod: -1,
        });
        assert_eq!(array.projection, Projection::CastJsonbText);
        // Composite / range / multirange → canonical JSON text.
        for typtype in ['c', 'r', 'm'] {
            let mapping = map(&CatalogType {
                oid: 88_888,
                typtype,
                typcategory: 'C',
                typmod: -1,
            });
            assert_eq!(
                mapping.projection,
                Projection::CastJsonbText,
                "typtype {typtype}"
            );
        }
        // Textual fallback: interval (1186), money (790), inet (869),
        // timetz (1266).
        for o in [1186, 790, 869, 1266] {
            let mapping = map(&base(o, -1));
            assert_eq!(mapping.projection, Projection::CastText, "oid {o}");
            assert_eq!(mapping.kind, Kind::Text, "oid {o}");
            assert!(mapping.documented_lossy, "oid {o}");
        }
    }

    #[test]
    fn cursor_capability_matches_contract() {
        for (o, capable) in [
            (oid::INT8, true),
            (oid::TEXT, true),
            (oid::UUID, true),
            (oid::TIMESTAMPTZ, true),
            (oid::DATE, true),
            (oid::TIME, true),
            (oid::BOOL, false),
            (oid::FLOAT8, false),
            (oid::BYTEA, false),
            (oid::JSONB, false),
        ] {
            assert_eq!(map(&base(o, -1)).cursor_capable, capable, "oid {o}");
        }
    }

    #[test]
    fn hint_table_is_closed_and_utf8_is_universal() {
        // utf8 works on anything, keeping the canonical-JSON shape for
        // arrays.
        let array = CatalogType {
            oid: 1007,
            typtype: 'b',
            typcategory: 'A',
            typmod: -1,
        };
        let mapping = apply_hint(&array, TypeHint::Utf8).expect("utf8 is universal");
        assert_eq!(mapping.projection, Projection::CastJsonbText);
        // text → int64 casts server-side.
        let mapping = apply_hint(&base(oid::TEXT, -1), TypeHint::Int64).expect("text to int64");
        assert_eq!(mapping.projection, Projection::Cast("int8".into()));
        assert_eq!(mapping.kind, Kind::Int64);
        assert!(mapping.cursor_capable);
        // numeric → float64 is allowed but documented lossy.
        let mapping =
            apply_hint(&base(oid::NUMERIC, -1), TypeHint::Float64).expect("numeric to float64");
        assert!(mapping.documented_lossy);
        // A closed-table miss names the oid and points at utf8.
        let error = apply_hint(&base(oid::BYTEA, -1), TypeHint::Int64).unwrap_err();
        assert!(error.contains("closed"), "{error}");
        assert!(error.contains("utf8"), "{error}");
        // Hints never apply to arrays (other than utf8).
        assert!(apply_hint(&array, TypeHint::Int64).is_err());
        // decimal bounds enforced.
        assert!(
            apply_hint(
                &base(oid::TEXT, -1),
                TypeHint::Decimal {
                    precision: 39,
                    scale: 0
                }
            )
            .is_err()
        );
    }

    #[test]
    fn hint_vocabulary_round_trips_as_strings() {
        for spelling in [
            "bool",
            "int64",
            "float64",
            "utf8",
            "binary",
            "timestamp_tz",
            "timestamp_naive",
            "date",
            "time",
            "uuid",
            "json",
            "decimal(12,4)",
        ] {
            let hint: TypeHint = spelling.parse().expect(spelling);
            assert_eq!(hint.to_string(), spelling);
        }
        assert!("decimal(12)".parse::<TypeHint>().is_err());
        assert!("varchar".parse::<TypeHint>().is_err());
    }
}
