//! Unit-test fixtures for the commit-retry family.
//!
//! Three modules ride the one bounded retry (append, state property
//! write, schema evolution); they share this ONE mock catalog whose
//! `update_table` conflicts a configured number of times. The mock
//! exists because the race is untimeable: no live competitor can be
//! steered into the narrow load→commit window on demand, so the
//! conflict is injected where timing cannot flake.
//!
//! `table_with_schema` builds a table through
//! `TableMetadataBuilder::from_table_creation`, which RE-ASSIGNS field
//! ids exactly as a REST catalog does on create — so id-sensitivity is
//! reproducible without a container (a container-gated test skips
//! green and can never be the red evidence a fix needs).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, NestedField, PrimitiveType, Schema, Struct,
    TableMetadata, Type,
};
use iceberg::table::Table;
use iceberg::{Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent};

/// A live-shaped table over one required `id: long` column.
pub(super) fn test_table() -> Table {
    table_with_schema(
        Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("schema"),
    )
}

/// [`test_table`]'s schema, carrying `properties` — the state-refusal
/// tests need a mock marker table whose properties hold a planted
/// legacy-scope key.
pub(super) fn test_table_with_properties(properties: HashMap<String, String>) -> Table {
    table_with_schema_and_properties(
        Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("schema"),
        properties,
    )
}

/// A table carrying `schema`, built the way a catalog builds one —
/// field ids re-assigned, metadata in memory.
pub(super) fn table_with_schema(schema: Schema) -> Table {
    table_with_schema_and_properties(schema, HashMap::new())
}

/// A table carrying `schema` and `properties`, built the way a
/// catalog builds one — field ids re-assigned, metadata in memory.
pub(super) fn table_with_schema_and_properties(
    schema: Schema,
    properties: HashMap<String, String>,
) -> Table {
    let creation = TableCreation::builder()
        .name("events".to_owned())
        .location("memory://wh/ns/events".to_owned())
        .schema(schema)
        .properties(properties)
        .build();
    let metadata: TableMetadata =
        iceberg::spec::TableMetadataBuilder::from_table_creation(creation)
            .expect("metadata builder")
            .build()
            .expect("metadata")
            .metadata;
    Table::builder()
        .metadata(metadata)
        .identifier(TableIdent::new(
            NamespaceIdent::new("ns".into()),
            "events".into(),
        ))
        .file_io(iceberg::io::FileIO::new_with_memory())
        .metadata_location("memory://wh/ns/events/metadata/v0.json")
        .runtime(iceberg::Runtime::current())
        .build()
        .expect("table")
}

/// One well-formed parquet data-file record.
pub(super) fn data_file() -> iceberg::spec::DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path("memory://wh/ns/events/data/f.parquet".to_owned())
        .file_format(DataFileFormat::Parquet)
        .partition(Struct::empty())
        .partition_spec_id(0)
        .record_count(1)
        .file_size_in_bytes(1)
        .build()
        .expect("data file")
}

/// A catalog whose `update_table` conflicts `n` times, then lands.
#[derive(Debug)]
pub(super) struct ConflictCatalog {
    table: Mutex<Table>,
    conflicts_remaining: AtomicU32,
    pub(super) commits: AtomicU32,
}

impl ConflictCatalog {
    pub(super) fn failing(conflicts: u32) -> Arc<Self> {
        Self::over(test_table(), conflicts)
    }

    /// Like [`Self::failing`], but over a caller-supplied table — the
    /// state-refusal tests need `load_table` to answer a marker table
    /// carrying a planted property, not the generic `id: long` shape.
    pub(super) fn over(table: Table, conflicts: u32) -> Arc<Self> {
        Arc::new(Self {
            table: Mutex::new(table),
            conflicts_remaining: AtomicU32::new(conflicts),
            commits: AtomicU32::new(0),
        })
    }
}

#[async_trait]
impl Catalog for ConflictCatalog {
    async fn list_namespaces(
        &self,
        _parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        unimplemented!()
    }
    async fn create_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        unimplemented!()
    }
    async fn get_namespace(&self, _namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
        unimplemented!()
    }
    async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        unimplemented!()
    }
    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        unimplemented!()
    }
    async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> iceberg::Result<()> {
        unimplemented!()
    }
    async fn list_tables(&self, _namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        unimplemented!()
    }
    async fn create_table(
        &self,
        _namespace: &NamespaceIdent,
        _creation: TableCreation,
    ) -> iceberg::Result<Table> {
        unimplemented!()
    }
    async fn load_table(&self, _table: &TableIdent) -> iceberg::Result<Table> {
        Ok(self.table.lock().expect("table lock").clone())
    }
    async fn drop_table(&self, _table: &TableIdent) -> iceberg::Result<()> {
        unimplemented!()
    }
    async fn purge_table(&self, _table: &TableIdent) -> iceberg::Result<()> {
        unimplemented!()
    }
    async fn table_exists(&self, _table: &TableIdent) -> iceberg::Result<bool> {
        unimplemented!()
    }
    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> iceberg::Result<()> {
        unimplemented!()
    }
    async fn register_table(
        &self,
        _table: &TableIdent,
        _metadata_location: String,
    ) -> iceberg::Result<Table> {
        unimplemented!()
    }
    async fn update_table(&self, _commit: TableCommit) -> iceberg::Result<Table> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        let remaining = self.conflicts_remaining.load(Ordering::SeqCst);
        if remaining > 0 {
            self.conflicts_remaining
                .store(remaining - 1, Ordering::SeqCst);
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::CatalogCommitConflicts,
                "injected CAS conflict",
            ));
        }
        Ok(self.table.lock().expect("table lock").clone())
    }
}
