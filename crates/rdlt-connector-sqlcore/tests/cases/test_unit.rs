//! The unit-transaction SQL pins, lifted from the module's former inline tests.

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use rdlt_connector::core::{LoadId, TableName, TableSchema};
    use rdlt_connector_sqlcore::protocol::FullLoadPublish;
    use rdlt_connector_sqlcore::protocol::unit::*;

    use crate::cases::common::linked_table;

    fn pg(n: usize) -> String {
        format!("${n}")
    }
    fn duck(_: usize) -> String {
        "?".to_owned()
    }

    #[test]
    fn the_replay_outcome_is_inverted_between_publish_paths() {
        // The whole reason this type exists. Reading these two lines together
        // is the point — they were previously two long comments in two files,
        // saying opposite things, with nothing binding them.
        assert_eq!(
            replay_disposition(FullLoadPublish::DirectToTarget),
            ReplayDisposition::DiscardUnit
        );
        assert_eq!(
            replay_disposition(FullLoadPublish::Staged),
            ReplayDisposition::RunScript
        );
    }

    #[test]
    fn the_probes_differ_only_in_their_placeholders() {
        assert_eq!(
            receipt_exists_sql(pg),
            "SELECT count(*) FROM _rdlt_commits WHERE load_id = $1 AND commit_seq = $2"
        );
        assert_eq!(
            receipt_exists_sql(duck),
            "SELECT count(*) FROM _rdlt_commits WHERE load_id = ? AND commit_seq = ?"
        );
        assert_eq!(
            load_committed_sql(pg),
            "SELECT count(*) FROM _rdlt_commits WHERE load_id = $1"
        );
        assert_eq!(
            stage_nonempty_sql("\"_rdlt_stage_x\""),
            "SELECT EXISTS (SELECT 1 FROM \"_rdlt_stage_x\")"
        );
    }

    #[test]
    fn a_child_resolves_to_its_topmost_ancestor() {
        let mut tables: BTreeMap<TableName, (TableSchema, ())> = BTreeMap::new();
        for (name, parent) in [
            ("root", None),
            ("child", Some("root")),
            ("grandchild", Some("child")),
            ("lonely", None),
        ] {
            tables.insert(TableName::from(name), (linked_table(name, parent), ()));
        }
        let roots = roots_of(&tables);
        assert_eq!(
            roots[&TableName::from("grandchild")],
            TableName::from("root")
        );
        assert_eq!(roots[&TableName::from("child")], TableName::from("root"));
        assert_eq!(roots[&TableName::from("root")], TableName::from("root"));
        assert_eq!(roots[&TableName::from("lonely")], TableName::from("lonely"));
    }

    #[test]
    fn a_cyclic_map_terminates_instead_of_hanging() {
        // Not reachable through a shredded schema, but a commit that hangs is
        // worse than one that resolves oddly.
        let mut tables: BTreeMap<TableName, (TableSchema, ())> = BTreeMap::new();
        for (name, parent) in [("a", "b"), ("b", "a")] {
            tables.insert(
                TableName::from(name),
                (linked_table(name, Some(parent)), ()),
            );
        }
        let roots = roots_of(&tables);
        assert_eq!(roots.len(), 2);
    }

    #[test]
    fn a_session_may_only_commit_its_own_load() {
        let a = LoadId::from("load-a");
        let b = LoadId::from("load-b");
        assert!(load_mismatch(&a, &a).is_none());
        let message = load_mismatch(&a, &b).expect("a mismatch is refused");
        assert!(message.contains("load-a") && message.contains("load-b"));
    }
}
