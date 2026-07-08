use std::collections::HashMap;
use std::io::Read;
use std::path::Path;

use quick_xml::events::Event;
use quick_xml::Reader;
use crate::error::{KnimeError, KnimeResult};
use crate::types::{KnimeEdge, KnimeNode, KnwfArchive, WorkflowMeta};

/// Parser for `.knwf` workflow archives.
pub struct KnwfParser;

impl KnwfParser {
    pub fn parse_file<P: AsRef<Path>>(path: P) -> KnimeResult<KnwfArchive> {
        let file = std::fs::File::open(path.as_ref()).map_err(KnimeError::Io)?;
        let mut archive = zip::ZipArchive::new(file).map_err(KnimeError::Zip)?;
        Self::parse_zip(&mut archive)
    }

    pub fn parse_bytes(data: &[u8]) -> KnimeResult<KnwfArchive> {
        let cursor = std::io::Cursor::new(data);
        let mut archive = zip::ZipArchive::new(cursor).map_err(KnimeError::Zip)?;
        Self::parse_zip(&mut archive)
    }

    fn find_entry(
        archive: &mut zip::ZipArchive<impl Read + std::io::Seek>,
        name: &str,
    ) -> KnimeResult<String> {
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
            wf_entry
                .strip_suffix("workflow.knime")
                .unwrap_or("")
                .to_string()
        };

        // Step 3: Parse workflow.knime XML
        let (nodes_raw, edges) = Self::parse_workflow_xml(&wf_xml)?;

        // Step 4: Collect node directories
        let node_dirs: Vec<String> = archive
            .file_names()
            .filter(|n| {
                let stripped = n.strip_prefix(&prefix).unwrap_or(n);
                stripped.ends_with('/')
                    && stripped != "workflow.knime"
                    && !stripped.starts_with("report")
                    && stripped.contains("(#")
            })
            .map(|n| n.trim_end_matches('/').to_string())
            .collect();

        // Step 5: Detect metanode directories
        let metanode_dirs: Vec<String> = node_dirs
            .iter()
            .filter(|d| {
                let meta_path = format!("{}/workflow.knime", d);
                archive.by_name(&meta_path).is_ok()
            })
            .cloned()
            .collect();

        // Step 6: Parse settings.xml for each node
        let mut nodes = Vec::new();
        let mut metanodes = HashMap::new();

        for raw_node in nodes_raw.iter() {
            let node_name = raw_node.name.as_deref().unwrap_or("Unknown");

            let dir_name = node_dirs
                .iter()
                .find(|d| {
                    let stripped = d.strip_prefix(&prefix).unwrap_or(d);
                    stripped.starts_with(node_name)
                })
                .cloned();

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
            workflow_meta: WorkflowMeta::default(),
            nodes,
            edges,
            metanodes,
        })
    }

    /// Parse workflow.knime XML to extract nodes and edges.
    fn parse_workflow_xml(xml: &str) -> KnimeResult<(Vec<RawNode>, Vec<KnimeEdge>)> {
        let mut reader = Reader::from_str(xml);
        reader.config_mut().trim_text(true);

        let mut nodes = Vec::new();
        let mut edges = Vec::new();
        let mut buf = Vec::new();

        let mut in_nodes = false;
        let mut in_connections = false;
        let mut in_node_entry = false;
        let mut in_edge_entry = false;
        let mut current_node: Option<RawNodeBuilder> = None;
        let mut current_edge: Option<EdgeBuilder> = None;
        let mut current_field = String::new();
        let mut depth = 0u32;

        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Start(ref e)) => {
                    depth += 1;
                    let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();

                    // Track entry context
                    if tag == "entry" {
                        for attr in e.attributes().flatten() {
                            if attr.key.as_ref() == b"key" {
                                let val = String::from_utf8_lossy(&attr.value);
                                match val.as_ref() {
                                    "nodes" => in_nodes = true,
                                    "connections" => in_connections = true,
                                    _ => {}
                                }
                            }
                        }

                        if in_nodes && depth == 4 {
                            // Node entry at correct depth: <config> <entry key="nodes"> <config> <entry key="id">
                            let key = e.attributes().flatten().find(|a| a.key.as_ref() == b"key");
                            if let Some(k) = key {
                                let kid = String::from_utf8_lossy(&k.value);
                                if kid.parse::<u32>().is_ok() || kid.chars().all(|c| c.is_ascii_digit()) {
                                    in_node_entry = true;
                                    current_node = Some(RawNodeBuilder {
                                        id: kid.to_string(),
                                        ..Default::default()
                                    });
                                }
                            }
                        }

                        if in_connections && depth == 4 {
                            in_edge_entry = true;
                            current_edge = Some(EdgeBuilder::default());
                        }
                    }
                }
                Ok(Event::Text(ref e)) => {
                    let text = e.unescape().unwrap_or_default();
                    if !text.trim().is_empty() {
                        current_field = text.to_string();
                    }
                }
                Ok(Event::Empty(ref e)) => {
                    let entry_attrs: Vec<(String, String)> = e
                        .attributes()
                        .flatten()
                        .map(|a| {
                            let k = String::from_utf8_lossy(a.key.as_ref()).to_string();
                            let v = String::from_utf8_lossy(&a.value).to_string();
                            (k, v)
                        })
                        .collect();
                    let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                    if tag == "entry" {
                        let key = entry_attrs.iter().find(|(k, _)| k == "key").map(|(_, v)| v.clone());
                        let value = entry_attrs.iter().find(|(k, _)| k == "value").map(|(_, v)| v.clone());

                        if in_node_entry {
                            if let Some(ref mut node) = current_node {
                                if let Some(k) = key {
                                    match k.as_str() {
                                        "node_type" => node.node_type = value,
                                        "custom_description" | "name" => {
                                            if node.name.is_none() { node.name = value; }
                                        }
                                        "state" => node.state = value,
                                        "num_inputs" => node.num_inputs = value.as_deref().and_then(|v| v.parse().ok()),
                                        "num_outputs" => node.num_outputs = value.as_deref().and_then(|v| v.parse().ok()),
                                        "x" => node.x = value.as_deref().and_then(|v| v.parse().ok()),
                                        "y" => node.y = value.as_deref().and_then(|v| v.parse().ok()),
                                        _ => {}
                                    }
                                }
                            }
                        }

                        if in_edge_entry {
                            if let Some(ref mut edge) = current_edge {
                                if let (Some(k), Some(v)) = (&key, &value) {
                                    match k.as_str() {
                                        "srcID" => edge.source_id = Some(v.clone()),
                                        "src_port" => edge.source_port = v.parse().unwrap_or(0),
                                        "destID" => edge.target_id = Some(v.clone()),
                                        "dest_port" => edge.target_port = v.parse().unwrap_or(0),
                                        _ => {}
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(Event::End(ref e)) => {
                    let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                    depth = depth.saturating_sub(1);

                    if tag == "entry" && in_node_entry && depth == 3 {
                        // Node entry complete
                        if let Some(builder) = current_node.take() {
                            nodes.push(RawNode {
                                id: builder.id,
                                name: builder.name,
                                node_type: builder.node_type,
                                state: builder.state,
                                x: builder.x,
                                y: builder.y,
                                num_inputs: builder.num_inputs,
                                num_outputs: builder.num_outputs,
                            });
                        }
                        in_node_entry = false;
                    }

                    if tag == "entry" && in_edge_entry && depth == 3 {
                        if let Some(builder) = current_edge.take() {
                            if let (Some(src), Some(tgt)) = (builder.source_id, builder.target_id) {
                                edges.push(KnimeEdge {
                                    source_id: src,
                                    source_port: builder.source_port,
                                    target_id: tgt,
                                    target_port: builder.target_port,
                                });
                            }
                        }
                        in_edge_entry = false;
                    }

                    if tag == "entry" && in_nodes && depth == 2 {
                        in_nodes = false;
                    }
                    if tag == "entry" && in_connections && depth == 2 {
                        in_connections = false;
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => return Err(KnimeError::Xml(format!("XML parse error: {}", e))),
                _ => {}
            }
            buf.clear();
        }

        Ok((nodes, edges))
    }

    /// Parse KNIME settings.xml into simplified JSON.
    fn parse_settings_xml(xml: &str) -> serde_json::Value {
        if xml.is_empty() {
            return serde_json::Value::Null;
        }
        // Quick-n-dirty: KNIME settings.xml has <entry key="model"> as the root config.
        // We search for key/value pairs and build a flat JSON object.
        let mut reader = Reader::from_str(xml);
        reader.config_mut().trim_text(true);
        let mut buf = Vec::new();
        let mut result = serde_json::Map::new();
        let mut in_model = false;

        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Empty(ref e)) | Ok(Event::Start(ref e)) => {
                    let entry_attrs: Vec<(String, String)> = e
                        .attributes()
                        .flatten()
                        .map(|a| {
                            (String::from_utf8_lossy(a.key.as_ref()).to_string(),
                             String::from_utf8_lossy(&a.value).to_string())
                        })
                        .collect();
                    if entry_attrs.iter().any(|(k, _)| k == "key") {
                        let key = entry_attrs.iter().find(|(k, _)| k == "key").map(|(_, v)| v.clone());
                        let value = entry_attrs.iter().find(|(k, _)| k == "value").map(|(_, v)| v.clone());

                        if let Some(k) = key {
                            if k == "model" {
                                in_model = true;
                            } else if in_model {
                                if let Some(v) = value {
                                    result.insert(k, serde_json::Value::String(v));
                                }
                            }
                        }
                    }
                }
                Ok(Event::Eof) => break,
                Err(_) => break,
                _ => {}
            }
            buf.clear();
        }

        if result.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::Value::Object(result)
        }
    }
}

#[derive(Debug, Clone, Default)]
struct RawNodeBuilder {
    id: String,
    name: Option<String>,
    node_type: Option<String>,
    state: Option<String>,
    x: Option<f64>,
    y: Option<f64>,
    num_inputs: Option<u32>,
    num_outputs: Option<u32>,
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

#[derive(Debug, Clone, Default)]
struct EdgeBuilder {
    source_id: Option<String>,
    source_port: u32,
    target_id: Option<String>,
    target_port: u32,
}
