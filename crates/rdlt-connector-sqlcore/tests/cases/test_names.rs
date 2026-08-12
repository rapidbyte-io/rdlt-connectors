//! The naming/index-DDL pins, lifted from the module's former inline tests.

#[cfg(test)]
mod tests {
    use rdlt_connector_sqlcore::names::*;

    /// The golden pin these statements never had.
    ///
    /// Index DDL was the one piece of shared SQL with no byte-level pin, in both
    /// destinations, while every merge arm around it had one. `IF NOT EXISTS`
    /// is what makes that dangerous: a drifted name does not fail, it creates a
    /// SECOND index — so the failure is silent duplication rather than an error.
    #[test]
    fn index_ddl_is_pinned_byte_for_byte() {
        let q = |i: &str| format!("\"{i}\"");
        let cols = vec!["id".to_string(), "day".to_string()];

        let unique = create_index_sql(true, "orders", &cols, q);
        assert_eq!(
            unique,
            format!(
                "CREATE UNIQUE INDEX IF NOT EXISTS \"{}\" ON \"orders\" (\"id\", \"day\")",
                index_name(true, "orders", &cols)
            )
        );

        let plain = create_index_sql(false, "orders", &cols, q);
        assert_eq!(
            plain,
            format!(
                "CREATE INDEX IF NOT EXISTS \"{}\" ON \"orders\" (\"id\", \"day\")",
                index_name(false, "orders", &cols)
            )
        );

        // The two differ in more than the keyword: the NAME differs too, so a
        // plain and a unique index over the same columns can coexist.
        assert_ne!(unique, plain);
        assert_ne!(
            index_name(true, "orders", &cols),
            index_name(false, "orders", &cols)
        );

        // Quoting is the destination's, and it reaches every identifier —
        // the index name, the table, and each column.
        let backtick = create_index_sql(true, "orders", &cols, |i| format!("`{i}`"));
        assert!(backtick.contains("`orders`"), "{backtick}");
        assert!(backtick.contains("(`id`, `day`)"), "{backtick}");
        assert!(
            backtick.contains(&format!("`{}`", index_name(true, "orders", &cols))),
            "{backtick}"
        );
    }

    /// The duplicate-key diagnosis is ADVICE, and its shape is the contract:
    /// which strategy needs the index, which columns collide, and the two ways
    /// out.
    #[test]
    fn the_duplicate_key_diagnosis_names_columns_and_both_remedies() {
        let msg = duplicate_merge_key_diagnosis(
            "orders",
            &["id".to_string(), "day".to_string()],
            "SQLSTATE 23505",
        );
        assert!(msg.contains("`orders`"), "{msg}");
        assert!(
            msg.contains("id, day"),
            "names the colliding columns: {msg}"
        );
        assert!(
            msg.contains("upsert"),
            "names the strategy that requires it: {msg}"
        );
        assert!(
            msg.contains("delete_insert"),
            "offers the alternative: {msg}"
        );
        assert!(
            msg.contains("SQLSTATE 23505"),
            "keeps the driver's cause: {msg}"
        );
    }

    #[test]
    fn index_names_are_deterministic_and_distinct() {
        let a = index_name(false, "orders", &["id".into()]);
        let b = index_name(false, "orders", &["id".into()]);
        assert_eq!(a, b, "same inputs, same name (idempotency)");
        assert_ne!(
            a,
            index_name(true, "orders", &["id".into()]),
            "unique differs"
        );
        assert_ne!(a, index_name(false, "orders", &["other".into()]));
        assert_ne!(a, index_name(false, "other", &["id".into()]));
        assert!(a.len() <= 63, "under the identifier limit");
        assert!(a.starts_with("rdlt_ix_"));
        assert!(index_name(true, "t", &["k".into()]).starts_with("rdlt_ux_"));
    }
}
