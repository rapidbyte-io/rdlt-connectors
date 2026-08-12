//! Max-observed cursor tracking. ISO-8601 timestamps and equal-width
//! numerics order lexicographically — the documented constraint on REST
//! cursor fields.

use serde_json::Value;

/// Advance the max-observed cursor across a page's records.
pub(crate) fn observe(records: &[Value], field: &str, max: &mut Option<String>) {
    for record in records {
        let Some(value) = record.get(field) else {
            continue;
        };
        let Some(rendered) = render_scalar(value) else {
            continue;
        };
        if max.as_ref().is_none_or(|current| rendered > *current) {
            *max = Some(rendered);
        }
    }
}

/// Fold each child's max-observed cursor into the stream's running maximum;
/// an empty or absent child cursor never moves it backward.
pub(crate) fn merge_maxes(max: &mut Option<String>, child_maxes: Vec<Option<String>>) {
    for child_max in child_maxes.into_iter().flatten() {
        if max.as_ref().is_none_or(|current| child_max > *current) {
            *max = Some(child_max);
        }
    }
}

/// Render a JSON scalar (string or number) to its cursor/param string
/// form. Cursor and resume-param values are string-or-number by contract;
/// anything else yields `None` (skipped, never guessed).
pub(crate) fn render_scalar(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn observe_takes_the_lexicographic_maximum_and_skips_non_scalars() {
        let mut max = Some("2026-01-05".to_owned());
        observe(
            &[
                json!({"at": "2026-01-03"}),
                json!({"at": "2026-01-09"}),
                json!({"at": ["not", "scalar"]}),
                json!({"other": "2026-12-31"}),
            ],
            "at",
            &mut max,
        );
        assert_eq!(max.as_deref(), Some("2026-01-09"));
    }

    #[test]
    fn merge_maxes_never_moves_backward() {
        let mut max = Some("b".to_owned());
        merge_maxes(&mut max, vec![None, Some("a".into()), Some("c".into())]);
        assert_eq!(max.as_deref(), Some("c"));
        merge_maxes(&mut max, vec![Some("a".into())]);
        assert_eq!(max.as_deref(), Some("c"));
    }
}
