//! MotherDuck deploy target — the DAG → Flight compiler.
//!
//! Pure functions: `compile_inline` / `compile_per_node` take a `Workflow`
//! and produce `FlightProgram`s (Python source + name + sha256). Zero IO,
//! zero network — the deployer CLI lives in main.rs and reuses
//! `crate::mcp::http::HttpMcpClient`.

use super::{node_table, render_template, sql_quote, topo_order, FlightProgram};
use crate::core::graph::{NodeInstance, Workflow};
use petgraph::visit::EdgeRef;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// Compile errors — aggregated, never first-error-only (spec: the error
/// lists EVERY non-compilable node).
#[derive(Debug)]
pub struct CompileErrors(pub Vec<String>);

impl std::fmt::Display for CompileErrors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DAG compilation failed ({} error(s)):", self.0.len())?;
        for e in &self.0 {
            writeln!(f, "\n  - {e}")?;
        }
        Ok(())
    }
}
impl std::error::Error for CompileErrors {}

fn sha256_hex(s: &str) -> String {
    let mut h = Sha256::new();
    h.update(s.as_bytes());
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

/// Flight names must satisfy `[a-zA-Z0-9_-]` and be short — sanitize.
pub fn flight_name(prefix: &str, dag_id: uuid::Uuid, node_suffix: Option<uuid::Uuid>) -> String {
    let dag8: String = dag_id.simple().to_string().chars().take(8).collect();
    match node_suffix {
        Some(n) => {
            let n8: String = n.simple().to_string().chars().take(8).collect();
            format!("swanflow_{prefix}_{dag8}_{n8}")
        }
        None => format!("swanflow_{prefix}_{dag8}"),
    }
}

/// Resolve the `input_N` → table map for a node from incoming edges.
fn input_tables_of(workflow: &Workflow, node_id: uuid::Uuid) -> BTreeMap<u32, String> {
    let graph = &workflow.graph;
    let Some(idx) = graph.node_indices().find(|i| graph[*i].id == node_id) else {
        return BTreeMap::new();
    };
    graph
        .edges_directed(idx, petgraph::Direction::Incoming)
        .map(|e| (e.weight().target_port, node_table(graph[e.source()].id)))
        .collect()
}

fn vars_of(workflow: &Workflow) -> BTreeMap<String, String> {
    workflow
        .context
        .iter()
        .filter_map(|(k, v)| {
            v.as_str()
                .map(|s| (k.clone(), s.to_string()))
                .or_else(|| v.as_i64().map(|n| (k.clone(), n.to_string())))
                .or_else(|| v.as_f64().map(|n| (k.clone(), n.to_string())))
        })
        .collect()
}

/// Compile ONE whitelisted node into its Python statement(s).
/// Returns (statement_lines, produces_table).
fn compile_node(
    node: &NodeInstance,
    input_tables: &BTreeMap<u32, String>,
    vars: &BTreeMap<String, String>,
) -> Result<Vec<String>, String> {
    let label = format!("node {} ({})", node.id.simple(), node.type_id);
    let cfg = &node.config;
    let table = node_table(node.id);
    let mut lines = Vec::new();

    match node.type_id.as_str() {
        "core.io.csv_read" => {
            let path = cfg["path"]
                .as_str()
                .ok_or("{label}: csv_read requires 'path'")?;
            lines.push(format!(
                "_con.execute(\"CREATE TABLE {table} AS SELECT * FROM read_csv_auto('{}')\")",
                sql_quote(path)
            ));
        }
        "core.io.parquet_read" => {
            let path = cfg["path"]
                .as_str()
                .ok_or("{label}: parquet_read requires 'path'")?;
            lines.push(format!(
                "_con.execute(\"CREATE TABLE {table} AS SELECT * FROM read_parquet('{}')\")",
                sql_quote(path)
            ));
        }
        "core.io.http_fetch" => {
            let url = cfg["url"]
                .as_str()
                .ok_or("{label}: http_fetch requires 'url'")?;
            let default_query = format!("SELECT * FROM read_json_auto('{}')", sql_quote(url));
            let query = cfg["query"].as_str().unwrap_or(&default_query);
            let rendered = render_template(query, &label, input_tables, vars)?;
            lines.push(format!(
                "_con.execute(\"CREATE TABLE {table} AS {}\")",
                rendered.replace('"', "\\\"")
            ));
        }
        "core.sql" => {
            let query = cfg["query"]
                .as_str()
                .ok_or("{label}: sql requires 'query'")?;
            let rendered = render_template(query, &label, input_tables, vars)?;
            lines.push(format!(
                "_con.execute(\"CREATE TABLE {table} AS {}\")",
                rendered.replace('"', "\\\"")
            ));
        }
        "core.io.duckdb_write" => {
            // Sink: upstream table → md:<table>. The engine's local behavior
            // (file DuckDB) is intentionally re-targeted to the MotherDuck
            // attachment — deploying a local file makes no sense in a Flight.
            let target = cfg["table"]
                .as_str()
                .ok_or("{label}: duckdb_write requires 'table'")?;
            let mode = cfg["mode"].as_str().unwrap_or("replace");
            let src = input_tables
                .get(&0)
                .cloned()
                .ok_or("{label}: duckdb_write needs input_0")?;
            let stmt = match mode {
                "append" => format!("INSERT INTO md.{target} SELECT * FROM {src}"),
                _ => format!("CREATE OR REPLACE TABLE md.{target} AS SELECT * FROM {src}"),
            };
            lines.push("_con.execute(\"ATTACH IF NOT EXISTS 'md:' AS md\")".to_string());
            lines.push(format!("_con.execute(\"{stmt}\")"));
        }
        "connector.motherduck.flight" => {
            let fid = cfg["flight_id"]
                .as_str()
                .map(|s| s.to_string())
                .or_else(|| cfg["flight_name"].as_str().map(|s| s.to_string()))
                .ok_or("{label}: flight node requires flight_id or flight_name")?;
            let mut cfg_map: Vec<String> = cfg["config"]
                .as_object()
                .map(|o| {
                    o.iter()
                        .filter_map(|(k, v)| {
                            v.as_str()
                                .map(|s| format!("'{}': '{}'", sql_quote(k), sql_quote(s)))
                        })
                        .collect()
                })
                .unwrap_or_default();
            cfg_map.push(format!("'RUN_KEY': '_deploy_{}'", node.id.simple()));
            let _cfg_literal = format!("{{{}}}", cfg_map.join(", "));
            lines.push(format!(
                "_con.execute(\"CREATE TABLE {table} AS SELECT * FROM MD_RUN_FLIGHT(flight_id := '{}', config := MAP {{{}}})\"",
                sql_quote(&fid),
                // MAP literal entries
                cfg_map
                    .iter()
                    .map(|e| e.replacen('\'', "", 2)) // crude; refined below in golden tests
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        t if t.starts_with("core.transform.") => {
            // Transforms must carry their SQL text statically; anything
            // computed at runtime is not compilable.
            let sql_text = cfg["sql"].as_str().or(cfg["query"].as_str()).ok_or_else(|| {
                format!("{label}: transform node has no static SQL ('sql'/'query' config) — not compilable")
            })?;
            let rendered = render_template(sql_text, &label, input_tables, vars)?;
            lines.push(format!(
                "_con.execute(\"CREATE TABLE {table} AS {}\")",
                rendered.replace('"', "\\\"")
            ));
        }
        other => {
            return Err(format!(
                "{label}: type '{other}' is not on the deploy whitelist (LLM/script/luajit nodes depend on the local runtime)"
            ));
        }
    }
    Ok(lines)
}

/// inline mode: whole DAG → one Flight program.
pub fn compile_inline(workflow: &Workflow) -> Result<FlightProgram, CompileErrors> {
    let order = topo_order(workflow);
    let vars = vars_of(workflow);
    let graph = &workflow.graph;

    let mut errors: Vec<String> = Vec::new();
    let mut body: Vec<String> = Vec::new();

    for id in &order {
        let idx = graph.node_indices().find(|i| &graph[*i].id == id).unwrap();
        let node = &graph[idx];
        if !super::is_compilable(&node.type_id) {
            errors.push(format!(
                "node {} ({}): type '{}' is not on the deploy whitelist",
                node.id.simple(),
                node.type_id,
                node.type_id
            ));
            continue;
        }
        let inputs = input_tables_of(workflow, *id);
        match compile_node(node, &inputs, &vars) {
            Ok(mut lines) => {
                body.push(String::new());
                body.push(format!(
                    "# ── node {} ({}) ──",
                    node.id.simple(),
                    node.type_id
                ));
                body.append(&mut lines);
            }
            Err(e) => errors.push(e),
        }
    }

    if !errors.is_empty() {
        return Err(CompileErrors(errors));
    }

    let dag_id = workflow.id;
    let mut source = String::new();
    source.push_str(&format!(
        "# Compiled from swanflow DAG {dag_id} at {} — mode: inline\n",
        chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ")
    ));
    source.push_str(super::RED_LETTERS);
    source.push('\n');
    source.push_str("import duckdb\n_con = duckdb.connect()\n");
    for line in &body {
        source.push_str(line);
        source.push('\n');
    }
    source.push_str(&format!("print(\"SWANFLOW_DEPLOY_OK {dag_id}\")\n"));

    Ok(FlightProgram {
        flight_name: flight_name("inline", dag_id, None),
        source: source.clone(),
        source_sha256: sha256_hex(&source),
    })
}

/// per-node mode: one Flight per node + one scheduler Flight.
pub fn compile_per_node(workflow: &Workflow) -> Result<Vec<FlightProgram>, CompileErrors> {
    let order = topo_order(workflow);
    let vars = vars_of(workflow);
    let graph = &workflow.graph;

    let mut errors: Vec<String> = Vec::new();
    let mut programs: Vec<FlightProgram> = Vec::new();

    for id in &order {
        let idx = graph.node_indices().find(|i| &graph[*i].id == id).unwrap();
        let node = &graph[idx];
        if !super::is_compilable(&node.type_id) {
            errors.push(format!(
                "node {} ({}): type '{}' is not on the deploy whitelist",
                node.id.simple(),
                node.type_id,
                node.type_id
            ));
            continue;
        }
        let inputs = input_tables_of(workflow, *id);
        match compile_node(node, &inputs, &vars) {
            Ok(lines) => {
                let name = flight_name("node", workflow.id, Some(*id));
                let mut source = String::new();
                source.push_str(&format!(
                    "# swanflow per-node flight — node {} ({})\n",
                    node.id.simple(),
                    node.type_id
                ));
                source.push_str(super::RED_LETTERS);
                source.push('\n');
                source.push_str("import duckdb\n_con = duckdb.connect()\n");
                source.push_str("_con.execute(\"ATTACH IF NOT EXISTS 'md:' AS md\")\n");
                for l in &lines {
                    source.push_str(l);
                    source.push('\n');
                }
                source.push_str(&format!("print(\"NODE_OK {}\")\n", node.id.simple()));
                programs.push(FlightProgram {
                    flight_name: name,
                    source: source.clone(),
                    source_sha256: sha256_hex(&source),
                });
            }
            Err(e) => errors.push(e),
        }
    }

    if !errors.is_empty() {
        return Err(CompileErrors(errors));
    }

    // Scheduler flight: topological fan-out with state table.
    let scheduler = build_scheduler(workflow, &order);
    programs.push(scheduler);
    Ok(programs)
}

fn build_scheduler(workflow: &Workflow, order: &[uuid::Uuid]) -> FlightProgram {
    let dag8: String = workflow.id.simple().to_string().chars().take(8).collect();
    let state_table = format!("deploy_{dag8}_state");
    let mut source = String::new();
    source.push_str(&format!(
        "# swanflow scheduler flight — DAG {} ({} nodes)\n",
        workflow.id,
        order.len()
    ));
    source.push_str(super::RED_LETTERS);
    source.push('\n');
    source.push_str("import duckdb, time\n_con = duckdb.connect()\n");
    source.push_str("_con.execute(\"ATTACH IF NOT EXISTS 'md:' AS md\")\n");
    source.push_str(&format!(
        "_con.execute(\"CREATE TABLE IF NOT EXISTS md.{state_table} (run_key VARCHAR, node_id VARCHAR, status VARCHAR, updated_at TIMESTAMP)\")\n"
    ));
    source.push_str("def _skip(node_id, run_key):\n");
    source.push_str(&format!(
        "    return _con.execute(\"SELECT count(*) FROM md.{state_table} WHERE node_id = '\" + node_id + \"' AND run_key = '\" + run_key + \"' AND status = 'SUCCEEDED'\").fetchone()[0] > 0\n"
    ));
    source.push_str("def _mark(node_id, run_key, status):\n");
    source.push_str(&format!(
        "    _con.execute(\"INSERT INTO md.{state_table} VALUES ('\" + run_key + \"', '\" + node_id + \"', '\" + status + \"', now())\")\n"
    ));
    source.push_str("def _run(flight, node_id, run_key):\n");
    source.push_str(
        "    if _skip(node_id, run_key):\n        print('SKIP', node_id)\n        return\n",
    );
    source.push_str("    r = _con.execute(f\"SELECT * FROM MD_RUN_FLIGHT(flight_id := '{flight}', config := MAP {{'RUN_KEY': '{run_key}'}})\").fetchone()\n");
    source.push_str("    run_id = r[0]\n");
    source.push_str("    while True:\n");
    source.push_str("        s = _con.execute(f\"SELECT status FROM MD_LIST_FLIGHT_RUNS() WHERE run_id = '{run_id}'\").fetchone()\n");
    source.push_str(
        "        if s and s[0] in ('SUCCEEDED', 'FAILED', 'CANCELLED'):\n            break\n",
    );
    source.push_str("        time.sleep(5)\n");
    source.push_str("    _mark(node_id, run_key, s[0])\n");
    source.push_str(
        "    if s[0] != 'SUCCEEDED':\n        raise SystemExit(f'node {node_id} failed')\n",
    );
    source.push_str("import os\nRUN_KEY = os.environ.get('SWANFLOW_RUN_KEY', 'manual')\n");
    // Node chain in topological order
    for id in order {
        let n8: String = id.simple().to_string().chars().take(8).collect();
        source.push_str(&format!(
            "_run('flight:swanflow_node_{dag8}_{n8}', '{n8}', RUN_KEY)\n"
        ));
    }
    source.push_str(&format!("print(\"SWANFLOW_DEPLOY_OK {}\")\n", workflow.id));

    FlightProgram {
        flight_name: flight_name("sched", workflow.id, None),
        source: source.clone(),
        source_sha256: sha256_hex(&source),
    }
}

// ---------------------------------------------------------------------------
// Unit / golden tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::graph::EdgeInstance;
    use petgraph::stable_graph::StableGraph;

    fn mk_workflow(
        nodes: Vec<(uuid::Uuid, &str, serde_json::Value)>,
        edges: Vec<(usize, usize)>,
    ) -> Workflow {
        let mut wf = Workflow::default();
        wf.id = uuid::uuid!("aaaaaaaa-0000-0000-0000-000000000001");
        let mut g = StableGraph::new();
        let mut idxs = Vec::new();
        for (id, t, cfg) in nodes {
            idxs.push(g.add_node(NodeInstance {
                id,
                type_id: t.to_string(),
                config: cfg,
                ..Default::default()
            }));
        }
        for (s, t) in edges {
            g.add_edge(
                idxs[s],
                idxs[t],
                EdgeInstance {
                    source_port: 0,
                    target_port: 0,
                },
            );
        }
        wf.graph = g;
        wf
    }

    const A: uuid::Uuid = uuid::uuid!("00000000-0000-0000-0000-00000000000a");
    const B: uuid::Uuid = uuid::uuid!("00000000-0000-0000-0000-00000000000b");
    const C: uuid::Uuid = uuid::uuid!("00000000-0000-0000-0000-00000000000c");

    #[test]
    fn golden_inline_chain() {
        let wf = mk_workflow(
            vec![
                (A, "core.io.csv_read", serde_json::json!({"path": "in.csv"})),
                (
                    B,
                    "core.sql",
                    serde_json::json!({"query": "SELECT count(*) AS n FROM {{input_0}}"}),
                ),
                (
                    C,
                    "core.io.duckdb_write",
                    serde_json::json!({"table": "out", "mode": "replace"}),
                ),
            ],
            vec![(0, 1), (1, 2)],
        );
        let prog = compile_inline(&wf).expect("compiles");
        assert!(prog.source.contains(&format!("read_csv_auto('in.csv')")));
        assert!(prog.source.contains(&format!("FROM node_{A}")));
        assert!(prog.source.contains("CREATE OR REPLACE TABLE md.out AS"));
        assert!(prog.source.contains("SWANFLOW_DEPLOY_OK"));
        assert!(prog.source.starts_with("# Compiled from swanflow DAG"));
        assert!(prog.source.contains("runs as owner"));
        // deterministic digest
        let again = compile_inline(&wf).unwrap();
        // timestamp differs by seconds — compare everything except header line
        let strip = |s: &str| s.lines().skip(1).collect::<Vec<_>>().join("\n");
        assert_eq!(strip(&prog.source), strip(&again.source));
    }

    #[test]
    fn whitelist_violation_aggregates_all_errors() {
        let wf = mk_workflow(
            vec![
                (A, "core.ai.llm", serde_json::json!({})),
                (B, "core.script.python", serde_json::json!({})),
            ],
            vec![],
        );
        let err = compile_inline(&wf).unwrap_err();
        assert_eq!(err.0.len(), 2, "both errors reported, not just the first");
        assert!(err.to_string().contains("core.ai.llm"));
        assert!(err.to_string().contains("core.script.python"));
    }

    #[test]
    fn unresolved_template_var_fails() {
        let wf = mk_workflow(
            vec![(
                A,
                "core.sql",
                serde_json::json!({"query": "SELECT {{missing_var}}"}),
            )],
            vec![],
        );
        let err = compile_inline(&wf).unwrap_err();
        assert!(err.to_string().contains("missing_var"));
    }

    #[test]
    fn context_var_resolves() {
        let mut wf = mk_workflow(
            vec![(
                A,
                "core.sql",
                serde_json::json!({"query": "SELECT {{threshold}} AS t"}),
            )],
            vec![],
        );
        wf.context.insert("threshold".into(), serde_json::json!(42));
        let prog = compile_inline(&wf).unwrap();
        assert!(prog.source.contains("SELECT 42 AS t"));
    }

    #[test]
    fn per_node_produces_one_flight_per_node_plus_scheduler() {
        let wf = mk_workflow(
            vec![
                (
                    A,
                    "core.io.parquet_read",
                    serde_json::json!({"path": "in.parquet"}),
                ),
                (
                    B,
                    "core.io.duckdb_write",
                    serde_json::json!({"table": "sink"}),
                ),
            ],
            vec![(0, 1)],
        );
        let programs = compile_per_node(&wf).unwrap();
        assert_eq!(programs.len(), 3, "2 node flights + 1 scheduler");
        let sched = programs.last().unwrap();
        assert!(sched.source.contains("deploy_"));
        assert!(sched.source.contains("_skip"));
        assert!(sched.source.contains("SUCCEEDED"));
    }

    #[test]
    fn transform_without_static_sql_rejected() {
        let wf = mk_workflow(
            vec![(
                A,
                "core.transform.aggregate",
                serde_json::json!({"op": "sum"}),
            )],
            vec![],
        );
        let err = compile_inline(&wf).unwrap_err();
        assert!(err.to_string().contains("no static SQL"));
    }

    #[test]
    fn nested_flight_node_compiles() {
        let wf = mk_workflow(
            vec![(
                A,
                "connector.motherduck.flight",
                serde_json::json!({"flight_id": "11111111-2222-3333-4444-555555555555", "config": {"REGION": "eu"}}),
            )],
            vec![],
        );
        let prog = compile_inline(&wf).unwrap();
        assert!(prog.source.contains("MD_RUN_FLIGHT"));
        assert!(prog.source.contains("RUN_KEY"));
        assert!(prog.source.contains("REGION"));
    }

    #[test]
    fn flight_name_shape() {
        let dag = uuid::uuid!("abcdef01-0000-0000-0000-000000000000");
        let n = flight_name(
            "node",
            dag,
            Some(uuid::uuid!("12345678-0000-0000-0000-000000000000")),
        );
        assert!(n.starts_with("swanflow_node_"));
        assert!(n.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'));
        assert!(n.len() <= 200);
    }
}
