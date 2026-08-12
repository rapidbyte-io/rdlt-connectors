//! Data-file writing for one table's commit window.
//!
//! Unpartitioned tables stage through a plain data-file writer;
//! partitioned ones through the library's fanout writer with a
//! splitter computing partition values from source columns. Closing
//! yields the staged data files the commit loop appends. This module
//! also owns the ONE translation from the SPI's `ParquetOptions` to
//! the parquet library's writer properties.

use iceberg::arrow::RecordBatchPartitionSplitter;
use iceberg::spec::DataFileFormat;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::partitioning::PartitioningWriter as _;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder as _};
use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::WriterProperties;
use rdlt_connector_sdk::spi::core::crash_point;
use rdlt_connector_sdk::spi::{DestinationError, ParquetCompression, ParquetOptions};

use super::client::classify;

/// The concrete writer stack, named once.
type StagedWriterBuilder =
    DataFileWriterBuilder<ParquetWriterBuilder, DefaultLocationGenerator, DefaultFileNameGenerator>;

/// One table's writer for the current window.
pub(super) enum Writer {
    Plain(Box<dyn IcebergWriter>),
    Fanout(Box<Fanout>),
}

/// The partitioned half: the fanout writer plus the splitter that
/// computes each row's partition values.
pub(super) struct Fanout {
    writer: FanoutWriter<StagedWriterBuilder>,
    splitter: RecordBatchPartitionSplitter,
}

impl Writer {
    /// Open against the CURRENT table metadata. `session_nonce` makes
    /// file names unique ACROSS sessions: a recovery session
    /// re-staging the same (load, window) must never overwrite a file
    /// a prior session committed — the prior session's files stay
    /// orphaned and invisible when the replay check discards them.
    pub(super) async fn open(
        table: &iceberg::table::Table,
        file_prefix: &str,
        session_nonce: &str,
        properties: WriterProperties,
        target_file_size: usize,
    ) -> Result<Self, DestinationError> {
        let context = || format!("table `{}`", table.identifier());
        let location =
            DefaultLocationGenerator::new(table.metadata()).map_err(|e| classify(&context(), e))?;
        let names = DefaultFileNameGenerator::new(
            file_prefix.to_owned(),
            Some(session_nonce.to_owned()),
            DataFileFormat::Parquet,
        );
        // Never the library default properties: those write
        // UNCOMPRESSED, and an Iceberg table's files are read by every
        // engine pointed at the catalog, not only by rdlt.
        let parquet =
            ParquetWriterBuilder::new(properties, table.metadata().current_schema().clone());
        // The library rolls files for us — `parts.target_bytes` is fed
        // straight into it rather than reimplemented above it. Its own
        // default is the Iceberg spec's 512 MiB
        // (`write.target-file-size-bytes`), which is what this used
        // before the size became configurable.
        let rolling = RollingFileWriterBuilder::new(
            parquet,
            target_file_size,
            table.file_io().clone(),
            location,
            names,
        );
        crash_point!(
            "ice.files.write",
            Err(DestinationError::fatal("injected crash at ice.files.write"))
        );
        let builder = StagedWriterBuilder::new(rolling);
        let spec = table.metadata().default_partition_spec().clone();
        if spec.fields().is_empty() {
            let inner = builder
                .build(None)
                .await
                .map_err(|e| classify(&context(), e))?;
            Ok(Self::Plain(Box::new(inner)))
        } else {
            let splitter = RecordBatchPartitionSplitter::try_new_with_computed_values(
                table.metadata().current_schema().clone(),
                spec,
            )
            .map_err(|e| classify(&context(), e))?;
            Ok(Self::Fanout(Box::new(Fanout {
                writer: FanoutWriter::new(builder),
                splitter,
            })))
        }
    }

    /// Stage one aligned batch.
    pub(super) async fn write(
        &mut self,
        context: &str,
        batch: arrow_array::RecordBatch,
    ) -> Result<(), DestinationError> {
        match self {
            Self::Plain(inner) => inner.write(batch).await.map_err(|e| classify(context, e)),
            Self::Fanout(fanout) => {
                let parts = fanout
                    .splitter
                    .split(&batch)
                    .map_err(|e| classify(context, e))?;
                for (key, part) in parts {
                    fanout
                        .writer
                        .write(key, part)
                        .await
                        .map_err(|e| classify(context, e))?;
                }
                Ok(())
            }
        }
    }

    /// Close, yielding the window's staged data files.
    pub(super) async fn close(
        self,
        context: &str,
    ) -> Result<Vec<iceberg::spec::DataFile>, DestinationError> {
        match self {
            Self::Plain(mut inner) => inner.close().await.map_err(|e| classify(context, e)),
            Self::Fanout(fanout) => fanout
                .writer
                .close()
                .await
                .map_err(|e| classify(context, e)),
        }
    }
}

/// SPI parquet options → library writer properties, or the reason a
/// setting cannot be honoured (the caller maps it to a typed error at
/// the SPI boundary).
pub(super) fn writer_properties(options: &ParquetOptions) -> Result<WriterProperties, String> {
    let mut builder = WriterProperties::builder()
        .set_compression(compression(options)?)
        .set_dictionary_enabled(options.dictionary_enabled)
        .set_dictionary_page_size_limit(options.dictionary_page_size_limit);
    if let Some(limit) = options.data_page_size_limit {
        builder = builder.set_data_page_size_limit(limit);
    }
    // Only when set: the setter reads None as UNLIMITED rather than
    // "use the default" (and panics on Some(0), which validation
    // refuses upstream).
    if options.max_row_group_rows.is_some() {
        builder = builder.set_max_row_group_row_count(options.max_row_group_rows);
    }
    Ok(builder.build())
}

fn compression(options: &ParquetOptions) -> Result<Compression, String> {
    let level = options.compression_level;
    let unsigned = |what: &str| -> Result<u32, String> {
        let Some(level) = level else {
            return Err(format!("internal: {what} level requested without a value"));
        };
        u32::try_from(level).map_err(|_| {
            format!("`compression_level` {level} is negative — {what} levels start at 0")
        })
    };
    let bad = |what: &str, e: parquet::errors::ParquetError| {
        format!(
            "`compression_level` {} is not valid for {what}: {e}",
            level.unwrap_or_default()
        )
    };
    Ok(match (options.compression, level) {
        (ParquetCompression::Uncompressed, _) => Compression::UNCOMPRESSED,
        (ParquetCompression::Snappy, _) => Compression::SNAPPY,
        (ParquetCompression::Lz4Raw, _) => Compression::LZ4_RAW,
        (ParquetCompression::Gzip, None) => Compression::GZIP(GzipLevel::default()),
        (ParquetCompression::Gzip, Some(_)) => {
            Compression::GZIP(GzipLevel::try_new(unsigned("gzip")?).map_err(|e| bad("gzip", e))?)
        }
        (ParquetCompression::Brotli, None) => Compression::BROTLI(BrotliLevel::default()),
        (ParquetCompression::Brotli, Some(_)) => Compression::BROTLI(
            BrotliLevel::try_new(unsigned("brotli")?).map_err(|e| bad("brotli", e))?,
        ),
        (ParquetCompression::Zstd, None) => Compression::ZSTD(ZstdLevel::default()),
        (ParquetCompression::Zstd, Some(level)) => {
            Compression::ZSTD(ZstdLevel::try_new(level).map_err(|e| bad("zstd", e))?)
        }
        (other, _) => {
            return Err(format!(
                "compression `{}` is not supported by the iceberg destination",
                other.as_str()
            ));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The defaults COMPRESS (snappy) — the library default writes
    /// uncompressed files every catalog reader would pay for.
    #[test]
    fn the_default_options_compress() {
        let props = writer_properties(&ParquetOptions::default()).expect("defaults valid");
        assert_eq!(props.compression(&"any".into()), Compression::SNAPPY);
    }

    /// Unset row-group count keeps the library default — not the
    /// setter's None-means-unlimited reading.
    #[test]
    fn an_unset_row_group_count_keeps_the_library_default() {
        let props = writer_properties(&ParquetOptions::default()).expect("valid");
        assert_eq!(
            props.max_row_group_row_count(),
            WriterProperties::builder()
                .build()
                .max_row_group_row_count()
        );
    }

    /// Bad levels report with the frozen wordings, never panic.
    #[test]
    fn bad_levels_report_never_panic() {
        let err = writer_properties(&ParquetOptions {
            compression: ParquetCompression::Gzip,
            compression_level: Some(9_999),
            ..Default::default()
        })
        .expect_err("out of range");
        assert!(err.contains("is not valid for gzip"), "{err}");

        let err = writer_properties(&ParquetOptions {
            compression: ParquetCompression::Brotli,
            compression_level: Some(-2),
            ..Default::default()
        })
        .expect_err("negative");
        assert!(err.contains("brotli levels start at 0"), "{err}");
    }
}
