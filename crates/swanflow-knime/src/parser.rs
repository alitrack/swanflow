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
/// - `Node #N/settings.xml` — per-node configuration XML
/// - `Node #N/` (metanode) — nested sub-workflow with its own `workflow.knime`
pub struct KnwfParser;

impl KnwfParser {
    /// Parse a .knwf file from disk.
    pub fn parse_file<P: AsRef<Path>>(path: P) -> KnimeResult<KnwfArchive> {
        let file = std::fs::File::open(path.as_ref()).map_err(KnimeError::Io)?;
        let mut archive = zip::ZipArchive::new(file).map_err(KnimeError::Zip)?;
        Self::parse_zip(&mut archive).map(|(a, _)| a)
    }

    /// Parse .knwf from in-memory bytes.
    pub fn parse_bytes(data: &[u8]) -> KnimeResult<KnwfArchive> {
        let cursor = std::io::Cursor::new(data);
        let mut archive = zip::ZipArchive::new(cursor).map_err(KnimeError::Zip)?;
        Self::parse_zip(&mut archive).map(|(a, _)| a)
    }

    fn parse_zip(archive: &mut zip::ZipArchive<impl Read + std::io::Seek>) -> KnimeResult<(KnwfArchive, HashMap<String, KnwfArchive>)> {
        // Step 1: Read workflow.knime
        let mut wf_file = archive
            .by_name("workflow.knime")
            .map_err(|_| KnimeError::MissingWorkflow)?;
        let mut wf_xml = String::new();
        wf_file.read_to_string(&mut wf_xml).map_err(KnimeError::Io)?;

        // Step 2: Parse workflow.knime XML
        let wf_root: serde_xml_rs::value::Element = xml_from_str(&wf_xml)?;
        let (nodes_raw, edges) = Self::extract_nodes_and_edges(&wf_root)?;

        // Step 3: Collect node directory names
        let dir_names: Vec<String> = archive
            .file_names()
            .filter(|n| n.ends_with('/') && n != "workflow.knime")
            .map(|n| n.trim_end_matches('/').to_string())
            .collect();

        // Step 4: Parse each node's settings.xml
        let mut nodes = Vec::new();
        let mut metanodes = HashMap::new();
        // Track which node directories are metanodes (contain workflow.knime)
        let metanode_dirs: Vec<String> = dir_names
            .iter()
            .filter(|d| {
                let meta_path = format!("{}/workflow.knime", d);
                archive.by_name(&meta_path).is_ok()
            })
            .cloned()
            .collect();

        for (idx, raw_node) in nodes_raw.iter().enumerate() {
            let node_id = raw_node.id.clone();
            let dir_name = format!("Node #{}", idx);

            // Try to find and parse settings.xml inside this node's directory
            let settings_path = format!("{}/settings.xml", dir_name);
            let settings = if let Ok(mut sf) = archive.by_name(&settings_path) {
                let mut xml = String::new();
                sf.read_to_string(&mut xml).ok();
                Self::parse_settings_xml(&xml)
            } else {
                serde_json::Value::Null
            };

            let is_metanode = metanode_dirs.contains(&dir_name);

            // Parse metanode sub-workflow if present
            if is_metanode {
                let meta_path = format!("{}/workflow.knime", dir_name);
                if let Ok(mut mf) = archive.by_name(&meta_path) {
                    let mut meta_xml = String::new();
                    if mf.read_to_string(&mut meta_xml).is_ok() {
                        // For metanodes, parse their internal structure too
                        let meta_wf_root: Result<serde_xml_rs::value::Element, _> = xml_from_str(&meta_xml);
                        if let Ok(meta_root) = meta_wf_root {
                            if let Ok((meta_nodes_raw, meta_edges)) = Self::extract_nodes_and_edges(&meta_root) {
                                let meta_children: Vec<KnimeNode> = meta_nodes_raw
                                    .into_iter()
                                    .enumerate()
                                    .map(|(child_idx, raw)| KnimeNode {
                                        id: raw.id,
                                        name: raw.name,
                                        node_type: raw.node_type,
                                        state: raw.state,
                                        settings: serde_json::Value::Null,
                                        is_metanode: false,
                                        x: raw.x,
                                        y: raw.y,
                                        num_inputs: raw.num_inputs,
                                        num_outputs: raw.num_outputs,
                                    })
                                    .collect();
                                let child_meta = KnwfArchive {
                                    workflow_meta: WorkflowMeta::default(),
                                    nodes: meta_children,
                                    edges: meta_edges,
                                    metanodes: HashMap::new(),
                                };
                                metanodes.insert(node_id.clone(), child_meta);
                            }
                        }
                    }
                }
            }

            nodes.push(KnimeNode {
                id: node_id,
                name: raw_node.name,
                node_type: raw_node.node_type,
                state: raw_node.state,
                settings,
                is_metanode,
                x: raw_node.x,
                y: raw_node.y,
                num_inputs: raw_node.num_inputs,
                num_outputs: raw_node.num_outputs,
            });
        }

        Ok((KnwfArchive {
            workflow_meta: WorkflowMeta {
                name: None,
                version: None,
                created_by: None,
            },
            nodes,
            edges,
            metanodes,
        }, HashMap::new()))
    }

    /// Extract nodes and edges from the workflow.knime XML root element.
    fn extract_nodes_and_edges(
        root: &serde_xml_rs::value::Element,
    ) -> KnimeResult<(Vec<RawNode>, Vec<KnimeEdge>)> {
        let mut nodes = Vec::new();
        let mut edges = Vec::new();

        // Navigate the KNIME XML structure:
        // <config> → <entry key="nodes"> → <config> → entry per node
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
        // Find id attribute on the entry
        let id = entry
            .attributes
            .iter()
            .find(|a| a.name.local_name == "key")?
            .value
            .clone();

        // Find the inner config
        let config = entry.children.iter().find(|c| c.name.local_name == "config")?;

        // Extract typed settings
        let mut node_type = None;
        let mut node_name = None;
        let mut state = None;
        let mut x = None;
        let mut y = None;
        let mut num_inputs = None;
        let mut num_outputs = None;

        for child in &config.children {
            if child.name.local_name == "entry" {
                let key = child.attributes.iter().find(|a| a.name.local_name == "key");
                let value = child.attributes.iter().find(|a| a.name.local_name == "value");

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
        let config = entry.children.iter().find(|c| c.name.local_name == "config")?;

        let mut source_id = None;
        let mut source_port = 0u32;
        let mut target_id = None;
        let mut target_port = 0u32;

        for child in &config.children {
            if child.name.local_name == "entry" {
                let key = child.attributes.iter().find(|a| a.name.local_name == "key");
                let value = child.attributes.iter().find(|a| a.name.local_name == "value");

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

        // Look for the <config key="model"> entry
        let config = match root.children.iter().find(|c| c.name.local_name == "config") {
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

        // Convert model config to JSON recursively
        Self::config_to_json(model)
    }

    /// Convert KNIME XML config tree to JSON, stripping KNIME type wrappers.
    fn config_to_json(entry: &serde_xml_rs::value::Element) -> serde_json::Value {
        let entry_type = entry
            .attributes
            .iter()
            .find(|a| a.name.local_name == "type");

        match entry_type.map(|t| t.value.as_str()) {
            Some("xstring") | Some("xdouble" | Some("xint")) | Some("xboolean") => {
                // Leaf value
                let value = entry
                    .attributes
                    .iter()
                    .find(|a| a.name.local_name == "value");
                match value {
                    Some(v) => {
                        if entry_type.unwrap().value == "xdouble" {
                            if let Ok(n) = v.value.parse::<f64>() {
                                return serde_json::Value::Number(
                                    serde_json::Number::from_f64(n).unwrap_or(serde_json::Number::from(0)),
                                );
                            }
                        }
                        if entry_type.unwrap().value == "xint" {
                            if let Ok(n) = v.value.parse::<i64>() {
                                return serde_json::Value::Number(n.into());
                            }
                        }
                        if entry_type.unwrap().value == "xboolean" {
                            return serde_json::Value::Bool(v.value == "true");
                        }
                        serde_json::Value::String(v.value.clone())
                    }
                    None => serde_json::Value::Null,
                }
            }
            _ => {
                // Compound: object or array
                // KNIME uses entry elements with type="..." for array items
                let children: Vec<_> = entry
                    .children
                    .iter()
                    .filter(|c| c.name.local_name == "entry")
                    .collect();

                // If all entries have integer keys, treat as array
                let all_numeric = children.iter().all(|c| {
                    c.attributes
                        .iter()
                        .find(|a| a.name.local_name == "key")
                        .map(|a| a.value.parse::<usize>().is_ok())
                        .unwrap_or(false)
                });

                if all_numeric && !children.is_empty() {
                    // Array
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
                    // Object
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
