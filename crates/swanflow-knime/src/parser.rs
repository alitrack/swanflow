use std::collections::HashMap;
use std::io::Read;
use std::path::Path;

use serde_xml_rs::from_str as xml_from_str;

use crate::error::{KnimeError, KnimeResult};
use crate::types::{KnimeEdge, KnimeNode, KnwfArchive, WorkflowMeta};

/// Parser for `.knwf` workflow archives.
///
/// A `.knwf` file is a ZIP archive containing:
/// - `workflow.knime` — XML describing nodes and connections
/// - `Node Name (#N)/settings.xml` — per-node configuration XML
/// - Metanode dirs contain their own `workflow.knime` sub-structure
pub struct KnwfParser;

impl KnwfParser {
    /// Parse a .knwf file from disk.
    pub fn parse_file<P: AsRef<Path>>(path: P) -> KnimeResult<KnwfArchive> {
        let file = std::fs::File::open(path.as_ref()).map_err(KnimeError::Io)?;
        let mut archive = zip::ZipArchive::new(file).map_err(KnimeError::Zip)?;
        Self::parse_zip(&mut archive)
    }

    /// Parse .knwf from in-memory bytes.
    pub fn parse_bytes(data: &[u8]) -> KnimeResult<KnwfArchive> {
        let cursor = std::io::Cursor::new(data);
        let mut archive = zip::ZipArchive::new(cursor).map_err(KnimeError::Zip)?;
        Self::parse_zip(&mut archive)
    }

    /// Find a file entry by name suffix anywhere in the ZIP.
    fn find_entry(archive: &mut zip::ZipArchive<impl Read + std::io::Seek>, name: &str) -> KnimeResult<String> {
        for i in 0..archive.len() {
            let file = archive.by_index(i).map_err(KnimeError::Zip)?;
            let fname = file.name().to_string();
            if fname.ends_with(name) {
                return Ok(fname);
            }
        }
        Err(KnimeError::MissingWorkflow)
    }

    fn parse_zip(
        archive: &mut zip::ZipArchive<impl Read + std::io::Seek>,
    ) -> KnimeResult<KnwfArchive> {
        // Step 1: Find and read workflow.knime
        let wf_entry = Self::find_entry(archive, "workflow.knime")?;
        let mut wf_file = archive
            .by_name(&wf_entry)
            .map_err(|_| KnimeError::MissingWorkflow)?;
        let mut wf_xml = String::new();
        wf_file.read_to_string(&mut wf_xml).map_err(KnimeError::Io)?;

        // Step 2: Determine the workflow root prefix
        let prefix = if wf_entry == "workflow.knime" {
            String::new()
        } else {
            // "Walmart Sales Prediction/workflow.knime" → "Walmart Sales Prediction/"
            wf_entry
                .strip_suffix("workflow.knime")
                .unwrap_or("")
                .to_string()
        };

        // Step 3: Parse workflow.knime XML
        let wf_root: serde_xml_rs::value::Element = xml_from_str(&wf_xml)?;
        let (nodes_raw, edges) = Self::extract_nodes_and_edges(&wf_root)?;

        // Step 4: Build a name→node_raw lookup from the raw nodes
        // KNIME's workflow.knime stores node names like "CSV Reader"
        // The ZIP directories use "CSV Reader (#1)" format
        let raw_by_name: HashMap<String, &RawNode> = nodes_raw
            .iter()
            .filter_map(|n| n.name.as_ref().map(|name| (name.clone(), n)))
            .collect();

        // Step 5: Collect all node directories (matching "Name (#N)" pattern)
        let node_dirs: Vec<String> = archive
            .file_names()
            .filter(|n| {
                let stripped = n.strip_prefix(&prefix).unwrap_or(n);
                // Match "Something (#N)/" pattern
                stripped.ends_with('/')
                    && stripped != "workflow.knime"
                    && !stripped.starts_with("report")
                    && stripped.contains("(#")
            })
            .map(|n| n.trim_end_matches('/').to_string())
            .collect();

        // Step 6: Detect metanode directories (contain their own workflow.knime)
        let metanode_dirs: Vec<String> = node_dirs
            .iter()
            .filter(|d| {
                let meta_path = format!("{}/workflow.knime", d);
                archive.by_name(&meta_path).is_ok()
            })
            .cloned()
            .collect();

        // Step 7: Parse settings.xml for each node, matching by name
        let mut nodes = Vec::new();
        let mut metanodes = HashMap::new();

        for (idx, raw_node) in nodes_raw.iter().enumerate() {
            let node_name = raw_node.name.as_deref().unwrap_or("Unknown");

            // Find the matching directory in the ZIP
            let dir_name = node_dirs
                .iter()
                .find(|d| {
                    let stripped = d.strip_prefix(&prefix).unwrap_or(d);
                    stripped.starts_with(node_name)
                })
                .cloned();

            // Parse settings.xml
            let settings = if let Some(ref dir) = dir_name {
                let settings_path = format!("{}/settings.xml", dir);
                if let Ok(mut sf) = archive.by_name(&settings_path) {
                    let mut xml = String::new();
                    sf.read_to_string(&mut xml).ok();
                    Self::parse_settings_xml(&xml)
                } else {
                    serde_json::Value::Null
                }
            } else {
                serde_json::Value::Null
            };

            let is_metanode = dir_name
                .as_ref()
                .map(|d| metanode_dirs.contains(d))
                .unwrap_or(false);

            nodes.push(KnimeNode {
                id: raw_node.id.clone(),
                name: Some(node_name.to_string()),
                node_type: raw_node.node_type.clone(),
                state: raw_node.state.clone(),
                settings,
                is_metanode,
                x: raw_node.x,
                y: raw_node.y,
                num_inputs: raw_node.num_inputs,
                num_outputs: raw_node.num_outputs,
            });
        }

        Ok(KnwfArchive {
            workflow_meta: WorkflowMeta {
                name: None,
                version: None,
                created_by: None,
            },
            nodes,
            edges,
            metanodes,
        })
    }

    /// Extract nodes and edges from the workflow.knime XML root element.
    fn extract_nodes_and_edges(
        root: &serde_xml_rs::value::Element,
    ) -> KnimeResult<(Vec<RawNode>, Vec<KnimeEdge>)> {
        let mut nodes = Vec::new();
        let mut edges = Vec::new();

        let config = root
            .children
            .iter()
            .find(|c| c.name.local_name == "config");

        if let Some(config) = config {
            // Find nodes entry
            let nodes_entry = config.children.iter().find(|c| {
                c.name.local_name == "entry"
                    && c.attributes
                        .iter()
                        .any(|a| a.name.local_name == "key" && a.value == "nodes")
            });

            if let Some(nodes_container) = nodes_entry {
                for child in &nodes_container.children {
                    if child.name.local_name == "entry" {
                        if let Some(raw) = Self::parse_node_entry(child) {
                            nodes.push(raw);
                        }
                    }
                }
            }

            // Find connections entry
            let conns_entry = config.children.iter().find(|c| {
                c.name.local_name == "entry"
                    && c.attributes
                        .iter()
                        .any(|a| a.name.local_name == "key" && a.value == "connections")
            });

            if let Some(conns_container) = conns_entry {
                for child in &conns_container.children {
                    if child.name.local_name == "entry" {
                        if let Some(edge) = Self::parse_edge_entry(child) {
                            edges.push(edge);
                        }
                    }
                }
            }
        }

        Ok((nodes, edges))
    }

    fn parse_node_entry(entry: &serde_xml_rs::value::Element) -> Option<RawNode> {
        let id = entry
            .attributes
            .iter()
            .find(|a| a.name.local_name == "key")?
            .value
            .clone();

        let config = entry
            .children
            .iter()
            .find(|c| c.name.local_name == "config")?;

        let mut node_type = None;
        let mut node_name = None;
        let mut state = None;
        let mut x = None;
        let mut y = None;
        let mut num_inputs = None;
        let mut num_outputs = None;

        for child in &config.children {
            if child.name.local_name == "entry" {
                let key = child
                    .attributes
                    .iter()
                    .find(|a| a.name.local_name == "key");
                let value = child
                    .attributes
                    .iter()
                    .find(|a| a.name.local_name == "value");

                if let (Some(k), Some(v)) = (key, value) {
                    match k.value.as_str() {
                        "node_type" => node_type = Some(v.value.clone()),
                        "name" | "custom_description" => {
                            if node_name.is_none() {
                                node_name = Some(v.value.clone());
                            }
                        }
                        "state" => state = Some(v.value.clone()),
                        "x" => x = v.value.parse().ok(),
                        "y" => y = v.value.parse().ok(),
                        "num_inputs" => num_inputs = v.value.parse().ok(),
                        "num_outputs" => num_outputs = v.value.parse().ok(),
                        _ => {}
                    }
                }
            }
        }

        Some(RawNode {
            id,
            name: node_name,
            node_type,
            state,
            x,
            y,
            num_inputs,
            num_outputs,
        })
    }

    fn parse_edge_entry(entry: &serde_xml_rs::value::Element) -> Option<KnimeEdge> {
        let config = entry
            .children
            .iter()
            .find(|c| c.name.local_name == "config")?;

        let mut source_id = None;
        let mut source_port = 0u32;
        let mut target_id = None;
        let mut target_port = 0u32;

        for child in &config.children {
            if child.name.local_name == "entry" {
                let key = child
                    .attributes
                    .iter()
                    .find(|a| a.name.local_name == "key");
                let value = child
                    .attributes
                    .iter()
                    .find(|a| a.name.local_name == "value");

                if let (Some(k), Some(v)) = (key, value) {
                    match k.value.as_str() {
                        "srcID" => source_id = Some(v.value.clone()),
                        "src_port" => source_port = v.value.parse().unwrap_or(0),
                        "destID" => target_id = Some(v.value.clone()),
                        "dest_port" => target_port = v.value.parse().unwrap_or(0),
                        _ => {}
                    }
                }
            }
        }

        let source_id = source_id?;
        let target_id = target_id?;

        Some(KnimeEdge {
            source_id,
            source_port,
            target_id,
            target_port,
        })
    }

    /// Parse KNIME settings.xml into a simplified JSON model.
    fn parse_settings_xml(xml: &str) -> serde_json::Value {
        if xml.is_empty() {
            return serde_json::Value::Null;
        }

        let root: Result<serde_xml_rs::value::Element, _> = xml_from_str(xml);
        let root = match root {
            Ok(r) => r,
            Err(_) => return serde_json::Value::Null,
        };

        let config = match root
            .children
            .iter()
            .find(|c| c.name.local_name == "config")
        {
            Some(c) => c,
            None => return serde_json::Value::Null,
        };

        let model = config.children.iter().find(|c| {
            c.name.local_name == "entry"
                && c.attributes
                    .iter()
                    .any(|a| a.name.local_name == "key" && a.value == "model")
        });

        let model = match model {
            Some(m) => m,
            None => return serde_json::Value::Null,
        };

        Self::config_to_json(model)
    }

    /// Convert KNIME XML config tree to JSON, stripping KNIME type wrappers.
    fn config_to_json(entry: &serde_xml_rs::value::Element) -> serde_json::Value {
        let entry_type = entry
            .attributes
            .iter()
            .find(|a| a.name.local_name == "type");

        match entry_type.map(|t| t.value.as_str()) {
            Some("xstring") | Some("xdouble") | Some("xint") | Some("xboolean") => {
                let value = entry
                    .attributes
                    .iter()
                    .find(|a| a.name.local_name == "value");
                match value {
                    Some(v) => {
                        let et = entry_type.unwrap().value.as_str();
                        match et {
                            "xdouble" => {
                                if let Ok(n) = v.value.parse::<f64>() {
                                    return serde_json::Value::Number(
                                        serde_json::Number::from_f64(n)
                                            .unwrap_or(serde_json::Number::from(0)),
                                    );
                                }
                            }
                            "xint" => {
                                if let Ok(n) = v.value.parse::<i64>() {
                                    return serde_json::Value::Number(n.into());
                                }
                            }
                            "xboolean" => {
                                return serde_json::Value::Bool(v.value == "true");
                            }
                            _ => {}
                        }
                        serde_json::Value::String(v.value.clone())
                    }
                    None => serde_json::Value::Null,
                }
            }
            _ => {
                let children: Vec<_> = entry
                    .children
                    .iter()
                    .filter(|c| c.name.local_name == "entry")
                    .collect();

                let all_numeric = children.iter().all(|c| {
                    c.attributes
                        .iter()
                        .find(|a| a.name.local_name == "key")
                        .map(|a| a.value.parse::<usize>().is_ok())
                        .unwrap_or(false)
                });

                if all_numeric && !children.is_empty() {
                    let mut sorted = children.clone();
                    sorted.sort_by_key(|c| {
                        c.attributes
                            .iter()
                            .find(|a| a.name.local_name == "key")
                            .and_then(|a| a.value.parse::<usize>().ok())
                            .unwrap_or(0)
                    });
                    let arr: Vec<_> = sorted.iter().map(|c| Self::config_to_json(c)).collect();
                    serde_json::Value::Array(arr)
                } else {
                    let mut map = serde_json::Map::new();
                    for child in children {
                        let key = child
                            .attributes
                            .iter()
                            .find(|a| a.name.local_name == "key")
                            .map(|a| a.value.clone())
                            .unwrap_or_default();
                        let val = Self::config_to_json(child);
                        map.insert(key, val);
                    }
                    serde_json::Value::Object(map)
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
struct RawNode {
    id: String,
    name: Option<String>,
    node_type: Option<String>,
    state: Option<String>,
    x: Option<f64>,
    y: Option<f64>,
    num_inputs: Option<u32>,
    num_outputs: Option<u32>,
}
