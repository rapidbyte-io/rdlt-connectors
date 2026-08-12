//! The literal face: values → injection-safe SQL text.
//!
//! COPY accepts no binds, so resume predicates interpolate literals. Every
//! literal here is TYPED and EXACT: quoted-and-escaped strings with a cast,
//! and integer µs/day arithmetic against `'epoch'` anchors for the temporal
//! kinds — never a float round-trip, never an unquoted value.

use super::Scalar;

/// Render a scalar as its injection-safe, typed SQL literal.
pub(crate) fn render(scalar: &Scalar) -> String {
    match scalar {
        Scalar::Int(value) => format!("{value}::int8"),
        Scalar::Decimal { scaled, scale } => {
            format!("'{}'::numeric", decimal_text(*scaled, *scale))
        }
        Scalar::Text(value) => format!("'{}'::text", value.replace('\'', "''")),
        // `uuid >= text` has no operator, so the literal must be
        // `::uuid`-typed or every predicate would fail to plan.
        Scalar::Uuid(value) => format!("'{}'::uuid", value.replace('\'', "''")),
        Scalar::TimestampTz(microseconds) => {
            format!("(TIMESTAMPTZ 'epoch' + {microseconds}::int8 * INTERVAL '1 microsecond')")
        }
        Scalar::TimestampNaive(microseconds) => {
            format!("(TIMESTAMP 'epoch' + {microseconds}::int8 * INTERVAL '1 microsecond')")
        }
        Scalar::Date(days) => format!("(DATE 'epoch' + {days}::int4)"),
        Scalar::Time(microseconds) => {
            format!("(TIME '00:00' + {microseconds}::int8 * INTERVAL '1 microsecond')")
        }
    }
}

/// A scaled i128 back to its plain decimal text, sign preserved, zero-padded
/// to the scale (`-5` @ scale 2 → `-0.05`).
pub(crate) fn decimal_text(scaled: i128, scale: u8) -> String {
    let sign = if scaled < 0 { "-" } else { "" };
    let digits = scaled.unsigned_abs().to_string();
    let width = scale as usize;
    if width == 0 {
        return format!("{sign}{digits}");
    }
    let padded = format!("{digits:0>minimum$}", minimum = width + 1);
    let (integer_part, fraction_part) = padded.split_at(padded.len() - width);
    format!("{sign}{integer_part}.{fraction_part}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literals_are_typed_and_escaped() {
        assert_eq!(render(&Scalar::Int(7)), "7::int8");
        assert_eq!(
            render(&Scalar::Decimal {
                scaled: -5,
                scale: 2
            }),
            "'-0.05'::numeric"
        );
        assert_eq!(render(&Scalar::Text("it's".into())), "'it''s'::text");
        assert_eq!(
            render(&Scalar::Uuid("550e8400-e29b-41d4-a716-446655440000".into())),
            "'550e8400-e29b-41d4-a716-446655440000'::uuid"
        );
        assert_eq!(
            render(&Scalar::TimestampTz(1)),
            "(TIMESTAMPTZ 'epoch' + 1::int8 * INTERVAL '1 microsecond')"
        );
        assert_eq!(render(&Scalar::Date(3)), "(DATE 'epoch' + 3::int4)");
    }

    #[test]
    fn decimal_text_round_trips_through_the_text_parser() {
        for (scaled, scale, expected) in [
            (125i128, 2u8, "1.25"),
            (-5, 2, "-0.05"),
            (1_234_567_899, 2, "12345678.99"),
            (42, 0, "42"),
            (-7, 0, "-7"),
        ] {
            let text = decimal_text(scaled, scale);
            assert_eq!(text, expected);
            assert_eq!(
                super::super::text::parse_decimal_scaled(&text, scale),
                Ok(scaled),
                "{text}"
            );
        }
    }
}
