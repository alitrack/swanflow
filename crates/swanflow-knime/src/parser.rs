use std::collections::HashMap;
use std::io::Read;
use std::path::Path;

use quick_xml::events::Event;
use quick_xml::Reader;
use crate::error::{KnimeError, KnimeResult};
use crate::types::{KnimeEdge, KnimeNode, KnwfArchive, WorkflowMeta};

/// Collect XML element attributes into a Vec of (key, value) strings.
fn collect_attrs(e: &quick_xml::events::BytesStart) -> Vec<(String, String)> {
    e.attributes()
        .flatten()
        .map(|a| {
            (
                String::from_utf8_lossy(a.key.as_ref()).to_string(),
                String::from_utf8_lossy(&a.value).to_string(),
            )
        })
        .collect()
}

/// Get the value of a named attribute.
fn get_attr(attrs: &[(String, String)], name: &str) -> Option<String> {
    attrs.iter().find(|(k, _)| k == name).map(|(_, v)| v.clone())
}

/// Parser for `.knwf` workflow archives.
pub struct KnwfParser;

impl KnwfParser {
    pub fn parse_file<P: AsRef<Path>>(path: P) -> KnimeResult<KnwfArchive> {
        let file = std::fs::File::open(path.as_ref()).map_err(KnimeError::Io)?;
        let mut archive = zip::ZipArchive::new(file).map_err(KnimeError::Zip)?;
        Self::parse_zip_at(&mut archive, "")
    }

    pub fn parse_bytes(data: &[u8]) -> KnimeResult<KnwfArchive> {
        let cursor = std::io::Cursor::new(data);
        let mut archive = zip::ZipArchive::new(cursor).map_err(KnimeError::Zip)?;
        Self::parse_zip_at(&mut archive, "")
    }

    /// Find the shallowest entry matching `suffix` under a given prefix.
    fn find_shallowest_under(
        archive: &mut zip::ZipArchive<impl Read + std::io::Seek>,
        suffix: &str,
        prefix: &str,
    ) -> KnimeResult<String> {
        let mut best: Option<(usize, String)> = None;
        for i in 0..archive.len() {
            let Ok(file) = archive.by_index(i) else { continue };
            let fname = file.name().to_string();
            if fname.starts_with(prefix) && fname.ends_with(suffix) {
                let depth = fname.matches('/').count();
                if best.as_ref().map_or(true, |(d, _)| depth < *d) {
                    best = Some((depth, fname));
                }
            }
        }
        best.map(|(_, s)| s).ok_or(KnimeError::MissingWorkflow)
    }

    /// Parse a workflow at the given ZIP prefix ("" = root).
    pub(crate) fn parse_zip_at(
        archive: &mut zip::ZipArchive<impl Read + std::io::Seek>,
        prefix: &str,
    ) -> KnimeResult<KnwfArchive> {
        let wf_entry = Self::find_shallowest_under(archive, "workflow.knime", prefix)?;
        let mut wf_file = archive
            .by_name(&wf_entry)
            .map_err(|_| KnimeError::MissingWorkflow)?;
        let mut wf_xml = String::new();
        wf_file.read_to_string(&mut wf_xml).map_err(KnimeError::Io)?;
        drop(wf_file);

        // Derive the enclosing directory prefix for this workflow
        let wf_prefix = wf_entry
            .strip_suffix("workflow.knime")
            .unwrap_or("")
            .to_string();

        let (nodes_raw, edges) = Self::parse_workflow_xml(&wf_xml)?;

        let all_names: Vec<String> = archive.file_names().map(|n| n.to_string()).collect();

        // Collect settings.xml content
        let node_settings: HashMap<String, String> = {
            let mut map = HashMap::new();
            for raw_node in nodes_raw.iter() {
                let settings_rel = raw_node.settings_file.as_deref().unwrap_or("");
                let full_path = format!("{}{}", wf_prefix, settings_rel);
                match archive.by_name(&full_path) {
                    Ok(mut sf) => {
                        let mut xml = String::new();
                        // P2-6: a declared settings file that cannot be read
                        // is an error, not a silent empty config.
                        if let Err(e) = sf.read_to_string(&mut xml) {
                            return Err(KnimeError::Unsupported(format!(
                                "failed to read settings XML for node {} ({}): {}",
                                raw_node.id, full_path, e
                            )));
                        }
                        map.insert(raw_node.id.clone(), xml);
                    }
                    Err(_) => {
                        // No settings file for this node is a normal KNIME
                        // pattern (bare meta nodes) — skip only when absent.
                    }
                }
            }
            map
        };

        // Build KnimeNode list
        let mut nodes: Vec<KnimeNode> = Vec::new();
        for raw_node in nodes_raw.iter() {
            let settings_xml = node_settings.get(&raw_node.id);
            let settings = match settings_xml {
                Some(xml) => Self::parse_settings_xml(xml)?,
                None => serde_json::Value::Null,
            };

            // Detect metanode: check if node dir has its own workflow.knime
            let is_metanode = raw_node
                .settings_file
                .as_deref()
                .map(|sf| {
                    let dir = sf.strip_suffix("settings.xml").unwrap_or(sf);
                    let meta_wf = format!("{}{}workflow.knime", wf_prefix, dir);
                    all_names.iter().any(|n| n == &meta_wf)
                })
                .unwrap_or(false);

            nodes.push(KnimeNode {
                id: raw_node.id.clone(),
                name: raw_node.name.clone(),
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

        // Recursively parse metanode sub-workflows
        let mut metanodes: HashMap<String, KnwfArchive> = HashMap::new();
        for node in nodes.iter().filter(|n| n.is_metanode) {
            let node_name = node.name.as_deref().unwrap_or("");
            // The metanode directory is wf_prefix + node_name + "/"
            let meta_dir = format!("{}{}/", wf_prefix, node_name);
            match Self::parse_zip_at(archive, &meta_dir) {
                Ok(sub) => { metanodes.insert(node.id.clone(), sub); }
                Err(e) => {
                    tracing::warn!("Failed to parse metanode {} ({}): {}", node.id, node_name, e);
                }
            }
        }

        Ok(KnwfArchive {
            workflow_meta: WorkflowMeta::default(),
            nodes,
            edges,
            metanodes,
        })
    }

    /// Parse workflow.knime XML.
    fn parse_workflow_xml(xml: &str) -> KnimeResult<(Vec<RawNode>, Vec<KnimeEdge>)> {
        let mut reader = Reader::from_str(xml);
        reader.config_mut().trim_text(true);

        let mut nodes = Vec::new();
        let mut edges = Vec::new();
        let mut buf = Vec::new();

        #[derive(Debug, PartialEq)]
        enum Section { Root, Nodes, Connections }

        let mut section = Section::Root;
        let mut current_node: Option<RawNodeBuilder> = None;
        let mut current_edge: Option<EdgeBuilder> = None;
        let mut config_stack: Vec<String> = Vec::new();

        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Start(ref e)) => {
                    let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                    if tag == "config" {
                        let key = e.attributes().flatten()
                            .find(|a| a.key.as_ref() == b"key")
                            .map(|a| String::from_utf8_lossy(&a.value).to_string());

                        let key_str = key.as_deref().unwrap_or("");
                        config_stack.push(key_str.to_string());

                        match key_str {
                            "nodes" => section = Section::Nodes,
                            "connections" => section = Section::Connections,
                            k if section == Section::Nodes && k.starts_with("node_") => {
                                current_node = Some(RawNodeBuilder {
                                    id: k.strip_prefix("node_").unwrap_or(k).to_string(),
                                    ..Default::default()
                                });
                            }
                            k if section == Section::Connections && k.starts_with("connection_") => {
                                current_edge = Some(EdgeBuilder::default());
                            }
                            _ => {}
                        }
                    }
                }
                Ok(Event::Empty(ref e)) => {
                    let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                    if tag == "entry" {
                        let key = e.attributes().flatten()
                            .find(|a| a.key.as_ref() == b"key")
                            .map(|a| String::from_utf8_lossy(&a.value).to_string());
                        let value = e.attributes().flatten()
                            .find(|a| a.key.as_ref() == b"value")
                            .map(|a| String::from_utf8_lossy(&a.value).to_string());

                        if let Some(ref mut node) = current_node {
                            if let Some(k) = &key {
                                let v = value.as_deref().unwrap_or("");
                                match k.as_str() {
                                    "node_type" => node.node_type = value.clone(),
                                    "node_settings_file" => {
                                        node.settings_file = value.clone();
                                        if node.name.is_none() {
                                            node.name = value.as_ref().map(|s| {
                                                s.strip_suffix("/settings.xml")
                                                    .unwrap_or(s)
                                                    .to_string()
                                            });
                                        }
                                    }
                                    "custom_description" => {
                                        if node.name.is_none() && !v.is_empty() {
                                            node.name = value.clone();
                                        }
                                    }
                                    "state" => node.state = value.clone(),
                                    "num_inputs" => node.num_inputs = v.parse().ok(),
                                    "num_outputs" => node.num_outputs = v.parse().ok(),
                                    "x" => node.x = v.parse().ok(),
                                    "y" => node.y = v.parse().ok(),
                                    _ => {}
                                }
                            }
                        }

                        if let Some(ref mut edge) = current_edge {
                            if let (Some(k), Some(v)) = (&key, &value) {
                                match k.as_str() {
                                    "sourceID" => edge.source_id = Some(v.clone()),
                                    "sourcePort" => edge.source_port = v.parse().unwrap_or(0),
                                    "destID" => edge.target_id = Some(v.clone()),
                                    "destPort" => edge.target_port = v.parse().unwrap_or(0),
                                    _ => {}
                                }
                            }
                        }
                    }
                }
                Ok(Event::End(ref e)) => {
                    let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                    if tag == "config" {
                        let popped = config_stack.pop().unwrap_or_default();

                        if let Some(builder) = current_node.take() {
                            nodes.push(RawNode {
                                id: builder.id,
                                name: builder.name,
                                settings_file: builder.settings_file,
                                node_type: builder.node_type,
                                state: builder.state,
                                x: builder.x,
                                y: builder.y,
                                num_inputs: builder.num_inputs,
                                num_outputs: builder.num_outputs,
                            });
                        }
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

                        if section == Section::Nodes && popped == "nodes" {
                            section = Section::Root;
                        }
                        if section == Section::Connections && popped == "connections" {
                            section = Section::Root;
                        }
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

    /// Parse KNIME settings.xml into nested JSON.
    ///
    /// Uses a stack-based approach to capture nested `<config>` elements
    /// and their `<entry key="X" value="Y"/>` children. The result preserves
    /// the full config hierarchy.
    fn parse_settings_xml(xml: &str) -> KnimeResult<serde_json::Value> {
        if xml.is_empty() {
            return Ok(serde_json::Value::Null);
        }
        let mut reader = Reader::from_str(xml);
        reader.config_mut().trim_text(true);
        let mut buf = Vec::new();

        // Stack of (config_name, Map) — push on <config>, pop on </config>
        let mut stack: Vec<(String, serde_json::Map<String, serde_json::Value>)> = Vec::new();

        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Start(ref e)) => {
                    let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                    let attrs = collect_attrs(e);

                    if tag == "config" {
                        if let Some(k) = get_attr(&attrs, "key") {
                            stack.push((k, serde_json::Map::new()));
                        }
                    } else if tag == "entry" {
                        if let Some(k) = get_attr(&attrs, "key") {
                            if let Some(v) = get_attr(&attrs, "value") {
                                if let Some((_, current)) = stack.last_mut() {
                                    current.insert(k, serde_json::Value::String(v));
                                }
                            }
                        }
                    }
                }
                Ok(Event::Empty(ref e)) => {
                    let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                    let attrs = collect_attrs(e);

                    if tag == "entry" {
                        if let Some(k) = get_attr(&attrs, "key") {
                            if let Some(v) = get_attr(&attrs, "value") {
                                if let Some((_, current)) = stack.last_mut() {
                                    current.insert(k, serde_json::Value::String(v));
                                }
                            }
                        }
                    }
                    // Self-closing <config key="X"/> — rare but valid
                    if tag == "config" {
                        if let Some(k) = get_attr(&attrs, "key") {
                            if let Some((_, current)) = stack.last_mut() {
                                current.insert(k, serde_json::Value::Object(Default::default()));
                            }
                        }
                    }
                }
                Ok(Event::End(ref e)) => {
                    let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                    if tag == "config" && stack.len() > 1 {
                        let (name, map) = stack.pop().unwrap();
                        if let Some((_, parent)) = stack.last_mut() {
                            parent.insert(name, serde_json::Value::Object(map));
                        }
                    }
                }
                Ok(Event::Eof) => break,
                // P2-6: fail closed. The old `Err(_) => break` silently
                // truncated malformed XML and the importer "succeeded" with
                // half a config.
                Err(e) => {
                    return Err(KnimeError::Unsupported(format!(
                        "XML parse error in settings XML: {}",
                        e
                    )));
                }
                _ => {}
            }
            buf.clear();
        }

        // Return the root config's object
        Ok(if let Some((_, root_map)) = stack.pop() {
            if root_map.is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::Value::Object(root_map)
            }
        } else {
            serde_json::Value::Null
        })
    }
}

#[derive(Debug, Clone, Default)]
struct RawNodeBuilder {
    id: String,
    name: Option<String>,
    settings_file: Option<String>,
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
    settings_file: Option<String>,
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
