//! DuckDB-dialect SQL transform node (`core.sql.duckdb`).
//!
//! `core.sql` runs through DataFusion and therefore cannot use DuckDB's
//! statically-linked extension capabilities (json / icu / fuzzy matching /
//! list lambdas / PIVOT / rich statistics). This node executes the user's
//! SQL in a real DuckDB session instead:
//!
//!   input_0..N (DataFusion) -> temp parquet files -> DuckDB tables
//!   -> user query -> COPY (query) TO temp parquet -> DataFusion read-back
//!
//! Both bridge directions are the same temp-parquet technique proven by
//! `DuckDBScanExec` (read direction) and `DuckDBWriteSinkExec::write_batches`
//! (write direction). The Utf8View→declared-schema cast after read-back
//! follows the Bug-10/16a family guard.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::Array;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::execution::context::TaskContext;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::RecordBatchStream;
use futures::StreamExt;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use datafusion::physical_plan::{ExecutionPlan, PlanProperties};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::memory::MemoryStream;
use futures::stream::Stream;
use serde_json::Value as JsonValue;

use crate::nodes::config as cfg;
use crate::nodes::{NodeBehavior, NodeSpec, PortSpec, TypedNodeBehavior};

// ---------------------------------------------------------------------------
// Logical node
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct DuckSqlLogical {
    pub query: String,
    pub inputs: Vec<LogicalPlan>,
    pub schema: DFSchemaRef,
    /// DuckDB extensions to LOAD before executing the query (same contract
    /// as core.io.duckdb_read's `extensions` config). Entries ending in
    /// `.duckdb_extension` are treated as paths (unsigned load allowed);
    /// otherwise INSTALL+LOAD by name.
    pub extensions: Vec<String>,
}

impl std::cmp::PartialEq for DuckSqlLogical {
    fn eq(&self, other: &Self) -> bool {
        self.query == other.query && self.inputs == other.inputs
    }
}
impl std::cmp::Eq for DuckSqlLogical {}
impl std::hash::Hash for DuckSqlLogical {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.query.hash(state);
    }
}
impl PartialOrd for DuckSqlLogical {
    fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for DuckSqlLogical {
    fn name(&self) -> &str {
        "DuckSql"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.inputs.iter().collect()
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DuckSql(query={})", self.query)
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        Ok(Self {
            query: self.query.clone(),
            inputs,
            schema: self.schema.clone(),
            extensions: self.extensions.clone(),
        })
    }
}

// ---------------------------------------------------------------------------
// Physical node
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct DuckSqlExec {
    pub query: String,
    pub extensions: Vec<String>,
    pub input: Arc<dyn ExecutionPlan>,
    /// Extra inputs (multi-input workflows): input_1 .. input_N
    pub extra_inputs: Vec<Arc<dyn ExecutionPlan>>,
    pub project_schema: SchemaRef,
    cache: PlanProperties,
}

impl DuckSqlExec {
    pub fn new(
        query: String,
        extensions: Vec<String>,
        input: Arc<dyn ExecutionPlan>,
        extra_inputs: Vec<Arc<dyn ExecutionPlan>>,
        project_schema: SchemaRef,
    ) -> Self {
        // CoalescePartitionsExec discipline (Bug-7 family): we consume all
        // partitions of every input through a single pull loop.
        let input = Arc::new(CoalescePartitionsExec::new(input));
        let extra_inputs: Vec<Arc<dyn ExecutionPlan>> = extra_inputs
            .into_iter()
            .map(|i| Arc::new(CoalescePartitionsExec::new(i)) as Arc<dyn ExecutionPlan>)
            .collect();
        let cache = PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(project_schema.clone()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );
        Self {
            query,
            extensions,
            input,
            extra_inputs,
            project_schema,
            cache,
        }
    }
}

impl DisplayAs for DuckSqlExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DuckSqlExec(query={})", self.query)
    }
}

impl ExecutionPlan for DuckSqlExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "DuckSqlExec"
    }
    fn schema(&self) -> SchemaRef {
        self.project_schema.clone()
    }
    fn properties(&self) -> &PlanProperties {
        &self.cache
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        let mut v = vec![&self.input];
        v.extend(self.extra_inputs.iter());
        v
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let mut it = children.into_iter();
        let first = it.next().ok_or_else(|| {
            datafusion::error::DataFusionError::Internal("DuckSqlExec needs >=1 child".into())
        })?;
        Ok(Arc::new(Self {
            query: self.query.clone(),
            extensions: self.extensions.clone(),
            input: first,
            extra_inputs: it.collect(),
            project_schema: self.project_schema.clone(),
            cache: PlanProperties::new(
                datafusion::physical_expr::EquivalenceProperties::new(self.project_schema.clone()),
                datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
                datafusion::physical_plan::execution_plan::EmissionType::Incremental,
                datafusion::physical_plan::execution_plan::Boundedness::Bounded,
            ),
        }))
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let mut streams = Vec::new();
        streams.push(self.input.execute(0, context.clone())?);
        for extra in &self.extra_inputs {
            streams.push(extra.execute(0, context.clone())?);
        }
        let n_streams = streams.len();
        Ok(Box::pin(DuckSqlStream {
            streams,
            query: self.query.clone(),
            extensions: self.extensions.clone(),
            schema: self.project_schema.clone(),
            buffers: (0..n_streams).map(|_| Vec::new()).collect(),
            state: DuckSqlState::Polling,
        }))
    }
}

// ---------------------------------------------------------------------------
// Stream state machine
// ---------------------------------------------------------------------------

enum DuckSqlState {
    Polling,
    Running(std::pin::Pin<Box<dyn std::future::Future<Output = std::result::Result<RecordBatch, datafusion::error::DataFusionError>> + Send>>),
    Done,
}

struct DuckSqlStream {
    streams: Vec<SendableRecordBatchStream>,
    query: String,
    extensions: Vec<String>,
    schema: SchemaRef,
    buffers: Vec<Vec<RecordBatch>>,
    state: DuckSqlState,
}

impl RecordBatchStream for DuckSqlStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for DuckSqlStream {
    type Item = std::result::Result<RecordBatch, datafusion::error::DataFusionError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            // Discriminant-only match (E0499 borrow guard, vector.rs pattern).
            match self.state {
                DuckSqlState::Polling => {
                    let mut all_done = true;
                    for i in 0..self.streams.len() {
                        match self.streams[i].poll_next_unpin(cx) {
                            Poll::Ready(Some(Ok(b))) => {
                                self.buffers[i].push(b);
                                // keep polling this stream next round
                            }
                            Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                            Poll::Ready(None) => { /* stream i finished */ }
                            Poll::Pending => all_done = false,
                        }
                    }
                    if all_done {
                        let query = self.query.clone();
                        let extensions = self.extensions.clone();
                        let schema = self.schema.clone();
                        let buffers = std::mem::take(&mut self.buffers);
                        let fut = async move {
                            run_duckdb_query(&query, &extensions, schema, buffers).await
                        };
                        self.state = DuckSqlState::Running(Box::pin(fut));
                    } else {
                        return Poll::Pending;
                    }
                }
                DuckSqlState::Running(..) => {
                    let fut = match &mut self.state {
                        DuckSqlState::Running(fut) => fut,
                        _ => unreachable!(),
                    };
                    match fut.as_mut().poll(cx) {
                        Poll::Ready(result) => {
                            self.state = DuckSqlState::Done;
                            return Poll::Ready(Some(result));
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
                DuckSqlState::Done => return Poll::Ready(None),
            }
        }
    }
}

/// Load configured extensions on a scratch DuckDB connection. Path entries
/// (ending in `.duckdb_extension`) require an unsigned-allowed connection.
fn load_extensions(conn: &duckdb::Connection, extensions: &[String]) -> duckdb::Result<()> {
    for ext in extensions {
        if ext.ends_with(".duckdb_extension") {
            let path = ext.replace('\'', "''");
            conn.execute(&format!("LOAD '{path}'"), [])?;
        } else {
            let _ = conn.execute(&format!("INSTALL {ext};"), []);
            conn.execute(&format!("LOAD {ext};"), [])?;
        }
    }
    Ok(())
}

/// Core routine: spill inputs to temp parquet, register as DuckDB tables
/// named input_0..input_N, execute the query, COPY result to temp parquet,
/// read back with DataFusion, cast to the declared schema.
async fn run_duckdb_query(
    query: &str,
    extensions: &[String],
    schema: SchemaRef,
    buffers: Vec<Vec<RecordBatch>>,
) -> std::result::Result<RecordBatch, datafusion::error::DataFusionError> {
    use parquet::arrow::ArrowWriter;

    let tmp_dir = std::env::temp_dir();
    let mut input_files: Vec<std::path::PathBuf> = Vec::new();
    for (i, batches) in buffers.iter().enumerate() {
        if batches.is_empty() {
            return Err(datafusion::error::DataFusionError::Internal(
                format!("input_{} produced no batches", i),
            ));
        }
        let f = tmp_dir.join(format!("swan_ducksql_in_{}_{}.parquet", i, uuid::Uuid::new_v4()));
        {
            let file = std::fs::File::create(&f).map_err(ext_err)?;
            let mut w = ArrowWriter::try_new(file, batches[0].schema(), None).map_err(ext_err)?;
            for b in batches {
                w.write(b).map_err(ext_err)?;
            }
            w.close().map_err(ext_err)?;
        }
        input_files.push(f);
    }

    let out_file = tmp_dir.join(format!("swan_ducksql_out_{}.parquet", uuid::Uuid::new_v4()));
    let out_path_str = out_file.to_str().unwrap_or_default().to_string();

    // Execute in DuckDB (owned clones for the 'static spawn_blocking closure)
    {
        let query_owned = query.to_string();
        let ext_owned = extensions.to_vec();
        let out_path_owned = out_path_str.clone();
        tokio::task::spawn_blocking(move || -> duckdb::Result<()> {
            // Unsigned-allowed connection so locally-built extension
            // binaries (ml/turbovec/luajit/...) can be loaded by path.
            let config = duckdb::Config::default()
                .allow_unsigned_extensions()
                .map_err(|e| duckdb::Error::InvalidParameterName(format!("DuckDB config failed: {e}")))?;
            let conn = duckdb::Connection::open_in_memory_with_flags(config)?;
            load_extensions(&conn, &ext_owned)?;
            for (i, f) in input_files.iter().enumerate() {
                let path = f.to_str().unwrap_or_default().replace('\'', "''");
                conn.execute(
                    &format!(
                        "CREATE TEMP TABLE \"input_{}\" AS SELECT * FROM read_parquet('{}')",
                        i, path
                    ),
                    [],
                )?;
            }
            // NOTE: the query is embedded as raw SQL inside COPY (...), NOT as
            // a string literal — do not single-quote-escape it (doubling '
            // here produced ''.city'' parser errors in the first E2E).
            conn.execute(
                &format!("COPY ({}) TO '{}' (FORMAT PARQUET)", query_owned, out_path_owned),
                [],
            )?;
            for f in input_files.iter() {
                let _ = std::fs::remove_file(f);
            }
            Ok(())
        })
        .await
        .map_err(ext_err)?
        .map_err(ext_err)?;
    }

    // Read back with DataFusion and cast to declared schema
    let ctx = datafusion::prelude::SessionContext::new();
    let df = ctx
        .read_parquet(&out_path_str, datafusion::prelude::ParquetReadOptions::default())
        .await
        .map_err(ext_err)?;
    let batches = df.collect().await.map_err(ext_err)?;
    let _ = std::fs::remove_file(&out_file);

    let target = schema.as_ref();
    let mut cast_batches = Vec::with_capacity(batches.len());
    for b in batches {
        // FAIL-LOUD schema guard (Bug-10/16a family): a mismatched schema
        // must abort, never silently zip/rename/drop columns.
        if b.num_columns() != target.fields().len() {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "DuckSql output column count mismatch: query produced {} columns [{}], declared schema has {} [{}]",
                b.num_columns(),
                b.schema().fields().iter().map(|f| f.name().as_str()).collect::<Vec<_>>().join(", "),
                target.fields().len(),
                target.fields().iter().map(|f| f.name().as_str()).collect::<Vec<_>>().join(", "),
            )));
        }
        let mut name_mismatches = Vec::new();
        for (i, field) in target.fields().iter().enumerate() {
            let actual_name = b.schema().field(i).name().clone();
            if actual_name != *field.name() {
                name_mismatches.push(format!("col {}: declared '{}' but produced '{}'", i, field.name(), actual_name));
            }
        }
        if !name_mismatches.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "DuckSql output column name mismatch: {}",
                name_mismatches.join("; ")
            )));
        }
        let columns = target
            .fields()
            .iter()
            .zip(b.columns())
            .map(|(field, col)| cast(col, field.data_type()))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| {
                datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
            })?;
        cast_batches.push(
            RecordBatch::try_new(Arc::new(target.clone()), columns).map_err(|e| {
                datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
            })?,
        );
    }
    if cast_batches.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }
    Ok(concat_batches_safe(&cast_batches, schema)?)
}

fn concat_batches_safe(
    batches: &[RecordBatch],
    schema: SchemaRef,
) -> std::result::Result<RecordBatch, datafusion::error::DataFusionError> {
    // Use arrow concat kernel column-wise over all batches.
    let mut columns = Vec::with_capacity(schema.fields().len());
    for idx in 0..schema.fields().len() {
        let arrays: Vec<Arc<dyn Array>> = batches
            .iter()
            .map(|b| b.column(idx).clone() as Arc<dyn Array>)
            .collect();
        let combined = datafusion::arrow::compute::concat(
            &arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>(),
        )
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
        columns.push(combined);
    }
    Ok(RecordBatch::try_new(schema, columns).map_err(|e| {
        datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
    })?)
}

fn ext_err(e: impl std::error::Error + Send + Sync + 'static) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::External(Box::new(e))
}

// ---------------------------------------------------------------------------
// Node behavior (registry-facing)
// ---------------------------------------------------------------------------

pub struct DuckSqlNode;

#[async_trait]
impl NodeBehavior for DuckSqlNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.sql.duckdb".to_string(),
            display_name: "DuckDB SQL".to_string(),
            description: "Execute DuckDB-dialect SQL on inputs (json/icu/fuzzy/lists/PIVOT \
                          and other static extension capabilities unavailable in core.sql)"
                .to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input_0".into(),
                description: "Primary input".into(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Query result".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "query": { "type": "string" },
                    "extensions": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "DuckDB extensions to load before the query. Entries ending in .duckdb_extension are local binary paths (unsigned); others are INSTALL+LOAD by name."
                    },
                    "output_columns": {
                        "type": "object",
                        "description": "Optional declared output schema {name: type} for downstream planning when schema discovery fails"
                    }
                },
                "required": ["query"]
            }),
        }
    }

    fn analyze(&self, config: &JsonValue) -> Vec<String> {
        let query = cfg::required_string(config, "query").unwrap_or_default();
        if query.trim().is_empty() {
            return Vec::new();
        }
        let lineage = crate::nodes::sql_lineage::extract_lineage(&query);
        if lineage.reads.is_empty() && lineage.writes.is_empty() {
            // Pure port-to-port transform (or unparseable SQL): nothing to report.
            return Vec::new();
        }
        vec![format!("SQL lineage: {}", lineage)]
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        let q = cfg::required_string(config, "query")?;
        if q.trim().is_empty() {
            return Err(anyhow!("query must not be empty"));
        }
        // Guard: block writes to the engine's own metadata database via
        // sneaky statements embedded in the COPY payload.
        // Exception: LOAD '<path>.duckdb_extension' — loading a local
        // extension binary by absolute path (e.g. the ml extension) is a
        // legitimate use; ATTACH/INSTALL stay banned.
        let lower = q.to_lowercase();
        for banned in ["attach ", "install ", "load "] {
            if !lower.contains(banned) {
                continue;
            }
            if banned == "load " {
                // allow `LOAD '<path>.duckdb_extension'` as a standalone
                // statement; "load " anywhere else (by-name loads, COPY
                // payload smuggling, mid-statement) stays banned.
                let all_loads_local = q.split(';').all(|stmt| {
                    let s = stmt.trim().to_lowercase();
                    if s.starts_with("load ") {
                        let path = s["load ".len()..].trim();
                        path.starts_with('\'') && path.ends_with(".duckdb_extension'")
                    } else {
                        !s.contains("load ")
                    }
                });
                if all_loads_local {
                    continue;
                }
            }
            return Err(anyhow!("DuckDB statement '{}' is not allowed in this node", banned.trim()));
        }
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let query = cfg::required_string(config, "query")?;
        let extensions = cfg::optional_string_array(config, "extensions")?;

        // Schema discovery: execute the query against a scratch DuckDB with
        // sample inputs. Falls back to `output_columns` config, then to
        // pass-through.
        let schema: DFSchemaRef = discover_duck_schema(&query, &inputs, &extensions)
            .await
            .or_else(|| declared_output_schema(config))
            .unwrap_or_else(|| {
                if !inputs.is_empty() {
                    inputs[0].schema().clone()
                } else {
                    Arc::new(DFSchema::empty())
                }
            });

        Ok(vec![LogicalPlan::Extension(
            datafusion::logical_expr::Extension {
                node: Arc::new(DuckSqlLogical {
                    query,
                    inputs,
                    schema,
                    extensions,
                }),
            },
        )])
    }
}

impl TypedNodeBehavior for DuckSqlNode {
    const INPUT_COUNT: usize = 1;
    const OUTPUT_COUNT: usize = 1;
}

/// Run the query against empty typed batches from the inputs to learn the
/// output schema. Deterministic: only schemas are involved, no data, no
/// dependency on engine-session table registrations (a fresh-ctx collect()
/// of the input plan can NEVER work — the plan references tables registered
/// in the engine's SessionContext, not ours). Best-effort: any failure falls
/// back to declared `output_columns`, then pass-through.
async fn discover_duck_schema(
    query: &str,
    inputs: &[LogicalPlan],
    extensions: &[String],
) -> Option<DFSchemaRef> {
    if inputs.is_empty() {
        return None;
    }
    // Derive one zero-row typed batch per input from its logical schema.
    let mut sample_batches = Vec::new();
    for input in inputs {
        let arrow_schema = input.schema().as_arrow().clone();
        let empty = RecordBatch::new_empty(std::sync::Arc::new(arrow_schema));
        sample_batches.push(empty);
    }

    // Spill empty typed batches to temp parquet and prepare the query in DuckDB.
    let tmp_dir = std::env::temp_dir();
    let mut files = Vec::new();
    for (i, b) in sample_batches.iter().enumerate() {
        let f = tmp_dir.join(format!("swan_ducksql_disc_{}_{}.parquet", i, uuid::Uuid::new_v4()));
        write_sample_parquet(&f, b).ok()?;
        files.push(f);
    }
    let q = query.to_string();
    let ext_owned = extensions.to_vec();
    let res = tokio::task::spawn_blocking(move || -> duckdb::Result<Vec<(String, String)>> {
        // Unsigned-allowed connection, same as run time, so extension-backed
        // functions (ml_train_model etc.) bind during schema discovery too.
        let config = duckdb::Config::default()
            .allow_unsigned_extensions()
            .map_err(|e| duckdb::Error::InvalidParameterName(format!("DuckDB config failed: {e}")))?;
        let conn = duckdb::Connection::open_in_memory_with_flags(config)?;
        load_extensions(&conn, &ext_owned)?;
        for (i, f) in files.iter().enumerate() {
            let path = f.to_str().unwrap_or_default().replace('\'', "''");
            conn.execute(
                &format!(
                    "CREATE TEMP TABLE \"input_{}\" AS SELECT * FROM read_parquet('{}')",
                    i, path
                ),
                [],
            )?;
        }
        // Execute and read the arrow schema from the rows handle (same
        // pattern as luajit::discover_schema).
        let mut stmt = conn.prepare(&q)?;
        let rows = stmt.query([])?;
        let duck_schema = rows
            .as_ref()
            .expect("statement reference after query")
            .schema()
            .clone();
        let mut cols = Vec::new();
        for f in duck_schema.fields() {
            cols.push((f.name().clone(), format!("{:?}", f.data_type())));
        }
        for f in files.iter() {
            let _ = std::fs::remove_file(f);
        }
        Ok(cols)
    })
    .await
    .ok()?
    .ok()?;

    let mut fields = Vec::new();
    for (name, ty) in res {
        fields.push(datafusion::arrow::datatypes::Field::new(
            name,
            map_duck_type(&ty),
            true,
        ));
    }
    if fields.is_empty() {
        return None;
    }
    Some(Arc::new(DFSchema::try_from(
        datafusion::arrow::datatypes::Schema::new(fields),
    ).ok()?))
}

fn write_sample_parquet(path: &std::path::Path, batch: &RecordBatch) -> Result<()> {
    use parquet::arrow::ArrowWriter;
    let file = std::fs::File::create(path)?;
    let mut w = ArrowWriter::try_new(file, batch.schema(), None)?;
    w.write(batch)?;
    w.close()?;
    Ok(())
}

fn map_duck_type(col_type: &str) -> datafusion::arrow::datatypes::DataType {
    use datafusion::arrow::datatypes::DataType;
    let upper = col_type.to_uppercase();
    // Decimal (incl. HUGEINT results which duckdb COPYs to parquet as
    // DECIMAL(38,x), Debug-formatted "Decimal(38, 0)" / "Decimal128(38, 0)"):
    // scale 0 -> Int64, else Float64. Without this branch sum()/window sums
    // silently became Utf8 columns ('10.0') — wf43/wf48 first E2E caught it.
    if upper.starts_with("DECIMAL") {
        let scale = upper
            .rsplit(',')
            .next()
            .and_then(|s| s.trim_end_matches(')').trim().parse::<i32>().ok())
            .unwrap_or(0);
        return if scale == 0 {
            DataType::Int64
        } else {
            DataType::Float64
        };
    }
    match upper.as_str() {
        "BOOLEAN" => DataType::Boolean,
        "TINYINT" | "SMALLINT" | "INTEGER" | "BIGINT" | "HUGEINT" | "INT8" | "INT16" | "INT32"
        | "INT64" | "UINT8" | "UINT16" | "UINT32" | "UINT64" => DataType::Int64,
        "FLOAT" | "DOUBLE" | "REAL" | "DECIMAL" | "FLOAT32" | "FLOAT64" => DataType::Float64,
        "TIMESTAMP" | "TIMESTAMP WITH TIME ZONE" | "DATETIME" => {
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, None)
        }
        "DATE" => DataType::Date32,
        _ => DataType::Utf8,
    }
}

fn declared_output_schema(config: &JsonValue) -> Option<DFSchemaRef> {
    let cols = config.get("output_columns")?.as_object()?;
    let mut fields = Vec::new();
    for (name, ty) in cols {
        let dt = match ty.as_str()? {
            "string" | "utf8" => datafusion::arrow::datatypes::DataType::Utf8,
            "int64" => datafusion::arrow::datatypes::DataType::Int64,
            "float64" => datafusion::arrow::datatypes::DataType::Float64,
            "bool" => datafusion::arrow::datatypes::DataType::Boolean,
            _ => return None,
        };
        fields.push(datafusion::arrow::datatypes::Field::new(name, dt, true));
    }
    if fields.is_empty() {
        return None;
    }
    Some(Arc::new(
        DFSchema::try_from(datafusion::arrow::datatypes::Schema::new(fields)).ok()?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_blocks_attach_install_load() {
        let node = DuckSqlNode;
        assert!(node
            .validate(&serde_json::json!({ "query": "SELECT 1" }))
            .is_ok());
        assert!(node
            .validate(&serde_json::json!({ "query": "ATTACH 'x.db' AS y" }))
            .is_err());
        assert!(node
            .validate(&serde_json::json!({ "query": "LOAD json" }))
            .is_err());
        assert!(node
            .validate(&serde_json::json!({ "query": "INSTALL httpfs" }))
            .is_err());
    }

    #[test]
    fn test_validate_allows_local_extension_load() {
        let node = DuckSqlNode;
        // legit: load a local extension binary by path
        assert!(node
            .validate(&serde_json::json!({ "query":
                "LOAD '/home/u/.duckdb/extensions/v1.5.6/linux_amd64/ml.duckdb_extension'; SELECT 1;"
            }))
            .is_ok());
        // still banned: loading an installed extension by name
        assert!(node
            .validate(&serde_json::json!({ "query": "LOAD json" }))
            .is_err());
        // still banned: load buried mid-statement (COPY payload smuggling)
        assert!(node
            .validate(&serde_json::json!({ "query":
                "COPY (SELECT 'install stuff; load json;') TO 'x.csv'"
            }))
            .is_err());
    }

    #[test]
    fn test_map_duck_type() {
        use datafusion::arrow::datatypes::DataType;
        assert_eq!(map_duck_type("BIGINT"), DataType::Int64);
        assert_eq!(map_duck_type("DOUBLE"), DataType::Float64);
        assert_eq!(map_duck_type("VARCHAR"), DataType::Utf8);
        // HUGEINT/Decimal family: scale 0 -> Int64, fractional -> Float64
        assert_eq!(map_duck_type("Decimal(38, 0)"), DataType::Int64);
        assert_eq!(map_duck_type("Decimal128(38, 3)"), DataType::Float64);
    }
}
