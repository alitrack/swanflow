//! File sinks: `core.io.csv_write` and `core.io.parquet_write`.
//!
//! `plugin-architecture.md` §1.2 lists these as the last two absent base nodes.
//! They are *sinks*, not transforms: the node's output port is empty and the
//! work happens at execution time, which is why they need a logical node plus
//! an `ExecutionPlan` (registered in `core::engine::planner`) instead of the
//! `SELECT ...` plan a transform returns.
//!
//! ## Three behaviours copied deliberately from `ExcelSinkExec`
//!
//! 1. **Coalesce each input to one partition.** `input.execute(partition, ..)`
//!    only pulls *one* partition of a repartitioned upstream (e.g. a hash
//!    repartitioned aggregation); the rows in the others are silently dropped.
//! 2. **Buffered sink semantics (Bug 16b).** A sink must not pass batches
//!    through as they arrive. The engine may drop the output stream early, and
//!    a pass-through sink would then write whatever partial batches it had
//!    already seen — a randomly truncated file that looks like a successful
//!    run. So: drain fully, write once at end-of-stream, then emit a single
//!    aggregated batch.
//! 3. **Emit a typed batch rather than nothing**, so the engine can still
//!    register the node's output instead of treating a sink as a row producer.
//!
//! ## One behaviour added here
//!
//! **Atomic write.** The file is written to a sibling temp file and renamed
//! into place. A mid-run encoder failure (or a killed process) then cannot
//! leave a truncated file that later looks like a complete result.

use std::any::Any;
use std::fmt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::{anyhow, Result as AnyResult};
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::Result as DfResult;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::{Stream, StreamExt};
use serde_json::Value as JsonValue;

use crate::nodes::config as cfg;
use crate::nodes::{NodeBehavior, NodeCapability, NodeSpec, PortSpec};

// ---------------------------------------------------------------------------
// Format + compression
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum FileFormat {
    Csv,
    Parquet,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ParquetCompression {
    None,
    Snappy,
    Gzip,
    Zstd,
    Lz4,
}

impl ParquetCompression {
    fn parse(raw: Option<&str>) -> AnyResult<Self> {
        match raw.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
            None | Some("") | Some("snappy") => Ok(Self::Snappy),
            Some("none") | Some("uncompressed") => Ok(Self::None),
            Some("gzip") => Ok(Self::Gzip),
            Some("zstd") => Ok(Self::Zstd),
            Some("lz4") | Some("lz4_raw") => Ok(Self::Lz4),
            Some(other) => Err(anyhow!(
                "Parquet 'compression' must be one of none, snappy, gzip, zstd, lz4 — got '{other}'"
            )),
        }
    }

    fn to_parquet(self) -> parquet::basic::Compression {
        use parquet::basic::{Compression, GzipLevel, ZstdLevel};
        match self {
            Self::None => Compression::UNCOMPRESSED,
            Self::Snappy => Compression::SNAPPY,
            Self::Gzip => Compression::GZIP(GzipLevel::default()),
            Self::Zstd => Compression::ZSTD(ZstdLevel::default()),
            Self::Lz4 => Compression::LZ4_RAW,
        }
    }
}

// ---------------------------------------------------------------------------
// Logical node
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileSinkLogical {
    pub path: PathBuf,
    pub format: FileFormat,
    pub header: bool,
    pub delimiter: u8,
    pub compression: ParquetCompression,
    pub inputs: Vec<LogicalPlan>,
}

impl std::hash::Hash for FileSinkLogical {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Inputs are deliberately skipped, matching ExcelSinkLogical: the plan
        // tree is rewritten by the optimiser, so hashing it would make the node
        // hash-unstable between planning passes.
        self.path.hash(state);
        self.format.hash(state);
        self.header.hash(state);
        self.delimiter.hash(state);
        self.compression.hash(state);
    }
}

impl PartialOrd for FileSinkLogical {
    fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for FileSinkLogical {
    fn name(&self) -> &str {
        "FileSink"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.inputs.iter().collect()
    }

    fn schema(&self) -> &DFSchemaRef {
        // Empty inputs must not panic on index [0]; an empty schema makes a
        // mis-wired sink fail at plan time (where propose_plan bails with a
        // readable message) instead of aborting the process.
        static EMPTY: std::sync::OnceLock<DFSchemaRef> = std::sync::OnceLock::new();
        self.inputs
            .first()
            .map(|p| p.schema())
            .unwrap_or_else(|| EMPTY.get_or_init(|| Arc::new(DFSchema::empty())))
    }

    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FileSink(format={:?}, path={:?})", self.format, self.path)
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DfResult<Self> {
        Ok(Self {
            path: self.path.clone(),
            format: self.format,
            header: self.header,
            delimiter: self.delimiter,
            compression: self.compression,
            inputs,
        })
    }
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

pub struct FileSinkExec {
    path: PathBuf,
    format: FileFormat,
    header: bool,
    delimiter: u8,
    compression: ParquetCompression,
    input: Arc<dyn ExecutionPlan>,
    cache: PlanProperties,
}

impl FileSinkExec {
    pub fn new(
        path: PathBuf,
        format: FileFormat,
        header: bool,
        delimiter: u8,
        compression: ParquetCompression,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Self {
        let input = inputs
            .first()
            .cloned()
            .expect("FileSinkExec requires exactly one input");
        let cache = PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            path,
            format,
            header,
            delimiter,
            compression,
            input,
            cache,
        }
    }
}

impl fmt::Debug for FileSinkExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("FileSinkExec")
            .field("format", &self.format)
            .field("path", &self.path)
            .finish()
    }
}

impl DisplayAs for FileSinkExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FileSinkExec({:?} -> {:?})", self.format, self.path)
    }
}

impl ExecutionPlan for FileSinkExec {
    fn name(&self) -> &str {
        "FileSinkExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            self.path.clone(),
            self.format,
            self.header,
            self.delimiter,
            self.compression,
            children,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        // Without coalescing, `execute(partition, ..)` pulls only `partition` of
        // a repartitioned upstream and the other partitions' rows vanish.
        let coalesced: Arc<dyn ExecutionPlan> = Arc::new(
            datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(
                self.input.clone(),
            ),
        );
        let stream = coalesced.execute(partition, context)?;
        Ok(Box::pin(FileWritingStream {
            input: stream,
            schema: self.input.schema(),
            path: self.path.clone(),
            format: self.format,
            header: self.header,
            delimiter: self.delimiter,
            compression: self.compression,
            batches: Vec::new(),
            finished: false,
            finalized: false,
        }))
    }
}

struct FileWritingStream {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    path: PathBuf,
    format: FileFormat,
    header: bool,
    delimiter: u8,
    compression: ParquetCompression,
    batches: Vec<RecordBatch>,
    finished: bool,
    finalized: bool,
}

impl RecordBatchStream for FileWritingStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for FileWritingStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if this.finished {
                if this.finalized {
                    return Poll::Ready(None);
                }
                this.finalized = true;
                match write_file(
                    &this.path,
                    this.format,
                    this.header,
                    this.delimiter,
                    this.compression,
                    &this.schema,
                    &this.batches,
                ) {
                    Ok(()) => {
                        // Buffered sink: one aggregated batch, then done. The
                        // engine still gets a typed, possibly empty, output.
                        let merged = datafusion::arrow::compute::concat_batches(
                            &this.schema,
                            &this.batches,
                        )
                        .unwrap_or_else(|_| RecordBatch::new_empty(this.schema.clone()));
                        return Poll::Ready(Some(Ok(merged)));
                    }
                    Err(e) => {
                        return Poll::Ready(Some(Err(datafusion::error::DataFusionError::Execution(
                            format!("{e:#}"),
                        ))))
                    }
                }
            }

            match this.input.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => this.batches.push(batch),
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => this.finished = true,
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Writing
// ---------------------------------------------------------------------------

fn temp_sibling(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "output".to_string());
    path.with_file_name(format!(".{}.{}.tmp", name, std::process::id()))
}

fn write_file(
    path: &Path,
    format: FileFormat,
    header: bool,
    delimiter: u8,
    compression: ParquetCompression,
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> AnyResult<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|e| {
                anyhow!("cannot create output directory {}: {e}", parent.display())
            })?;
        }
    }

    let tmp = temp_sibling(path);
    let written = match format {
        FileFormat::Csv => write_csv(&tmp, header, delimiter, batches),
        FileFormat::Parquet => write_parquet(&tmp, compression, schema, batches),
    };

    match written {
        Ok(()) => std::fs::rename(&tmp, path).map_err(|e| {
            let _ = std::fs::remove_file(&tmp);
            anyhow!(
                "wrote {} but cannot move it into place at {}: {e}",
                tmp.display(),
                path.display()
            )
        }),
        Err(e) => {
            let _ = std::fs::remove_file(&tmp);
            Err(e)
        }
    }
}

fn write_csv(path: &Path, header: bool, delimiter: u8, batches: &[RecordBatch]) -> AnyResult<()> {
    let file =
        std::fs::File::create(path).map_err(|e| anyhow!("cannot create {}: {e}", path.display()))?;
    let mut writer = datafusion::arrow::csv::WriterBuilder::new()
        .with_header(header)
        .with_delimiter(delimiter)
        .build(file);
    for batch in batches {
        writer
            .write(batch)
            .map_err(|e| anyhow!("CSV encode failed for {}: {e}", path.display()))?;
    }
    Ok(())
}

fn write_parquet(
    path: &Path,
    compression: ParquetCompression,
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> AnyResult<()> {
    let file =
        std::fs::File::create(path).map_err(|e| anyhow!("cannot create {}: {e}", path.display()))?;
    let props = parquet::file::properties::WriterProperties::builder()
        .set_compression(compression.to_parquet())
        .build();
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema.clone(), Some(props))
        .map_err(|e| anyhow!("cannot open a Parquet writer for {}: {e}", path.display()))?;
    for batch in batches {
        writer
            .write(batch)
            .map_err(|e| anyhow!("Parquet encode failed for {}: {e}", path.display()))?;
    }
    // close() writes the footer; without it the file is unreadable, so a
    // failure here must surface rather than be swallowed.
    writer
        .close()
        .map_err(|e| anyhow!("cannot finish Parquet file {}: {e}", path.display()))?;
    Ok(())
}

/// A path that is already a directory cannot become a single file. Say so
/// instead of letting `File::create` fail with a confusing OS error, or worse,
/// writing inside it.
fn reject_existing_dir(path: &str) -> AnyResult<()> {
    let p = Path::new(path);
    if p.is_dir() {
        return Err(anyhow!(
            "'{path}' is a directory; this node writes a single file, so name the file"
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------------------

pub struct CsvWriteNode;

#[async_trait]
impl NodeBehavior for CsvWriteNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.io.csv_write".to_string(),
            display_name: "CSV Writer".to_string(),
            description: "Write the incoming rows to a CSV file. The file appears only when the \
                          run completes — it is written to a temp file and renamed."
                .to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Rows to write".into(),
            }],
            output_ports: vec![],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Output file path." },
                    "header": { "type": "boolean", "default": true },
                    "delimiter": { "type": "string", "default": ",", "description": "Exactly one byte." }
                },
                "required": ["path"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> AnyResult<()> {
        let path = cfg::required_string(config, "path")?;
        if path.trim().is_empty() {
            return Err(anyhow!("CSV Writer 'path' must not be empty"));
        }
        reject_existing_dir(&path)?;
        if let Some(d) = cfg::optional_string(config, "delimiter") {
            if d.as_bytes().len() != 1 {
                return Err(anyhow!(
                    "CSV Writer 'delimiter' must be exactly one byte, got {d:?}"
                ));
            }
        }
        let _ = cfg::optional_bool(config, "header");
        Ok(())
    }

    fn capability(&self) -> NodeCapability {
        NodeCapability::WriteOnly
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> AnyResult<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            return Err(anyhow!(
                "CSV Writer has no input — nothing would be written, and the run would look successful"
            ));
        }
        let path = cfg::required_string(config, "path")?;
        reject_existing_dir(&path)?;
        let delimiter = cfg::optional_string(config, "delimiter")
            .and_then(|d| d.as_bytes().first().copied())
            .unwrap_or(b',');
        let header = cfg::optional_bool(config, "header").unwrap_or(true);
        Ok(vec![LogicalPlan::Extension(Extension {
            node: Arc::new(FileSinkLogical {
                path: PathBuf::from(path),
                format: FileFormat::Csv,
                header,
                delimiter,
                compression: ParquetCompression::Snappy,
                inputs,
            }),
        })])
    }
}

pub struct ParquetWriteNode;

#[async_trait]
impl NodeBehavior for ParquetWriteNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.io.parquet_write".to_string(),
            display_name: "Parquet Writer".to_string(),
            description: "Write the incoming rows to a Parquet file. The file appears only when the \
                          run completes — it is written to a temp file and renamed."
                .to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Rows to write".into(),
            }],
            output_ports: vec![],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Output file path." },
                    "compression": {
                        "enum": ["none", "snappy", "gzip", "zstd", "lz4"],
                        "default": "snappy"
                    }
                },
                "required": ["path"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> AnyResult<()> {
        let path = cfg::required_string(config, "path")?;
        if path.trim().is_empty() {
            return Err(anyhow!("Parquet Writer 'path' must not be empty"));
        }
        reject_existing_dir(&path)?;
        ParquetCompression::parse(cfg::optional_string(config, "compression").as_deref())?;
        Ok(())
    }

    fn capability(&self) -> NodeCapability {
        NodeCapability::WriteOnly
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> AnyResult<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            return Err(anyhow!(
                "Parquet Writer has no input — nothing would be written, and the run would look successful"
            ));
        }
        let path = cfg::required_string(config, "path")?;
        reject_existing_dir(&path)?;
        let compression =
            ParquetCompression::parse(cfg::optional_string(config, "compression").as_deref())?;
        Ok(vec![LogicalPlan::Extension(Extension {
            node: Arc::new(FileSinkLogical {
                path: PathBuf::from(path),
                format: FileFormat::Parquet,
                header: true,
                delimiter: b',',
                compression,
                inputs,
            }),
        })])
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::*;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap()
    }

    fn tmp_path(tag: &str, ext: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("swanflow-filesink-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join(format!("out.{ext}"))
    }

    #[tokio::test]
    async fn csv_round_trips_through_the_reader() {
        let path = tmp_path("csv", "csv");
        let batch = sample_batch();
        write_file(
            &path,
            FileFormat::Csv,
            true,
            b',',
            ParquetCompression::Snappy,
            &batch.schema(),
            &[batch.clone()],
        )
        .unwrap();
        assert!(path.exists(), "the file must be in place after a successful write");

        let ctx = SessionContext::new();
        let df = ctx.read_csv(path.to_str().unwrap(), CsvReadOptions::new()).await.unwrap();
        let batches = df.collect().await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 3, "every input row must come back");
        let names: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                b.column_by_name("name")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .flatten()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(names, vec!["a", "b", "c"], "values and order preserved");

        // No temp file left behind.
        let leftovers: Vec<_> = std::fs::read_dir(path.parent().unwrap())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains(".tmp"))
            .collect();
        assert!(leftovers.is_empty(), "temp file must be renamed away: {leftovers:?}");
    }

    #[tokio::test]
    async fn parquet_round_trips_through_the_reader() {
        let path = tmp_path("parquet", "parquet");
        let batch = sample_batch();
        write_file(
            &path,
            FileFormat::Parquet,
            true,
            b',',
            ParquetCompression::Zstd,
            &batch.schema(),
            &[batch.clone()],
        )
        .unwrap();
        assert!(path.exists());

        let ctx = SessionContext::new();
        let df = ctx
            .read_parquet(path.to_str().unwrap(), ParquetReadOptions::default())
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 3, "every input row must come back — a missing footer would fail here");
    }

    #[tokio::test]
    async fn multiple_batches_are_all_written() {
        let path = tmp_path("multi", "csv");
        let batch = sample_batch();
        write_file(
            &path,
            FileFormat::Csv,
            false,
            b';',
            ParquetCompression::Snappy,
            &batch.schema(),
            &[batch.clone(), batch.clone()],
        )
        .unwrap();
        let body = std::fs::read_to_string(&path).unwrap();
        // header=false → three data lines per batch, six in total.
        assert_eq!(
            body.lines().count(),
            6,
            "both batches must be written, not just the last: {body}"
        );
        assert!(body.contains(';'), "the custom delimiter must be used");
    }

    #[test]
    fn empty_delimiter_and_directory_paths_are_rejected() {
        let multi = serde_json::json!({ "path": "/tmp/x.csv", "delimiter": ",," });
        assert!(CsvWriteNode.validate(&multi).is_err(), "two bytes is not a delimiter");

        let dir = std::env::temp_dir();
        let as_dir = serde_json::json!({ "path": dir.to_str().unwrap() });
        let err = CsvWriteNode.validate(&as_dir).unwrap_err().to_string();
        assert!(err.contains("directory"), "must say why: {err}");
    }

    #[test]
    fn unknown_compression_is_rejected() {
        let bad = serde_json::json!({ "path": "/tmp/x.parquet", "compression": "bzip9" });
        let err = ParquetWriteNode.validate(&bad).unwrap_err().to_string();
        assert!(err.contains("compression"), "must name the field: {err}");
    }

    #[tokio::test]
    async fn a_sink_without_input_refuses_to_pretend_it_succeeded() {
        let err = CsvWriteNode
            .propose_plan(vec![], &serde_json::json!({ "path": "/tmp/x.csv" }))
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("no input"),
            "an empty sink must be an error, not a silent no-op: {err}"
        );
    }
}
