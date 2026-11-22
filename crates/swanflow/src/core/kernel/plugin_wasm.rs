//! A real `PluginHost` — dynamic WASM plugin discovery, loading and execution.
//!
//! The trait in `plugin.rs` was mock-only until now: the *static* plugin path
//! (`swanflow_core::Plugin` + `NodeRegistry::load_plugin_nodes`) is real and
//! shipped, but nothing discovered a plugin at runtime. This module is that
//! missing layer.
//!
//! ## ABI: `swanflow-plugin-1`
//!
//! Deliberately the **same** protocol `core.script.wasm` already uses, rather
//! than a second one invented here:
//!
//! ```text
//! alloc(size: i32) -> ptr
//! dealloc(ptr: i32, size: i32)
//! <entry>(ptr: i32, len: i32) -> ptr
//! ```
//!
//! Input is placed in linear memory at `(ptr, len)`; the entry point returns a
//! pointer whose first 4 bytes are the output length as little-endian `i32`,
//! followed by that many payload bytes. Payloads are Arrow IPC streams, but the
//! host treats them as opaque bytes — it never parses them, so a plugin can be
//! written in any language that can emit them.
//!
//! ## Discovery
//!
//! A plugin is a directory entry `<name>.plugin.json`:
//!
//! ```json
//! {
//!   "name": "sort",
//!   "version": "0.1.0",
//!   "abi": "swanflow-plugin-1",
//!   "entry": "sort_table",
//!   "description": "sort by first column",
//!   "functions": ["sort_table"],
//!   "module": "swanflow_plugin_sort.wasm"
//! }
//! ```
//!
//! `module` is optional and defaults to `<name>.wasm` next to the descriptor.
//!
//! ## Sandbox
//!
//! Two limits are enforced, not merely documented:
//! - **memory**: a per-store `StoreLimits` cap, so a plugin cannot eat the host;
//! - **time**: epoch interruption. A runaway plugin is interrupted at the
//!   deadline and the host survives — that is the whole point of paying for WASM.
//!
//! Both are the reason this is WASM and not `dlopen`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use wasmtime::{Config, Engine, Instance, Linker, Module, Store, StoreLimits, StoreLimitsBuilder, Trap};

use crate::core::error::{CoreResult, SwanFlowError};
use crate::core::kernel::plugin::{PluginDescriptor, PluginHandle, PluginHost, PluginType};

/// The only ABI this host speaks. Bump on any wire-format change.
pub const PLUGIN_ABI: &str = "swanflow-plugin-1";

/// Default per-instance memory cap: 256 MiB. Generous for a row batch,
/// far below "takes down the process".
pub const DEFAULT_MEMORY_LIMIT: usize = 256 * 1024 * 1024;

/// Default wall-clock cap per `run` call.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// On-disk plugin descriptor (`<name>.plugin.json`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmPluginManifest {
    pub name: String,
    #[serde(default)]
    pub version: String,
    /// Must equal [`PLUGIN_ABI`]. Missing is an error, not a default: a plugin
    /// that does not declare its wire format cannot be trusted to match.
    pub abi: String,
    /// Exported function the host calls by default.
    pub entry: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub functions: Vec<String>,
    /// Module file name; defaults to `<name>.wasm`.
    #[serde(default)]
    pub module: Option<String>,
}

impl WasmPluginManifest {
    fn module_path(&self, dir: &Path) -> PathBuf {
        dir.join(
            self.module
                .clone()
                .unwrap_or_else(|| format!("{}.wasm", self.name)),
        )
    }
}

/// Store data carrying the resource limiter.
struct HostState {
    limits: StoreLimits,
}

/// A plugin that passed `scan`/`load`.
struct LoadedPlugin {
    manifest: WasmPluginManifest,
    module: Module,
    path: PathBuf,
}

/// WASM plugin host. Cloning is not supported on purpose — one host owns one
/// engine (and therefore one epoch clock).
pub struct WasmPluginHost {
    engine: Engine,
    timeout: Duration,
    memory_limit: usize,
    loaded: Mutex<HashMap<String, LoadedPlugin>>,
}

impl std::fmt::Debug for WasmPluginHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let n = self.loaded.lock().map(|m| m.len()).unwrap_or(0);
        f.debug_struct("WasmPluginHost")
            .field("loaded", &n)
            .field("timeout", &self.timeout)
            .field("memory_limit", &self.memory_limit)
            .finish()
    }
}

impl Default for WasmPluginHost {
    fn default() -> Self {
        Self::new()
    }
}

impl WasmPluginHost {
    pub fn new() -> Self {
        Self::with_limits(DEFAULT_TIMEOUT, DEFAULT_MEMORY_LIMIT)
    }

    pub fn with_limits(timeout: Duration, memory_limit: usize) -> Self {
        let mut config = Config::new();
        // Epoch interruption is what makes a deadline enforceable. Without it a
        // plugin with `loop {}` hangs the host forever.
        config.epoch_interruption(true);
        let engine = Engine::new(&config).expect("wasmtime engine");
        Self {
            engine,
            timeout,
            memory_limit,
            loaded: Mutex::new(HashMap::new()),
        }
    }

    /// Read and validate one descriptor. Every rejection names the offending
    /// field, so a bad plugin reads as "your abi is wrong", never as
    /// "node not found".
    fn read_manifest(path: &Path) -> CoreResult<WasmPluginManifest> {
        let text = std::fs::read_to_string(path).map_err(|e| {
            SwanFlowError::io_error(format!("plugin manifest {}: {}", path.display(), e))
        })?;
        let m: WasmPluginManifest = serde_json::from_str(&text).map_err(|e| {
            SwanFlowError::config_error(format!(
                "plugin manifest {}: field `{}` — {}",
                path.display(),
                e.classify_field(),
                e
            ))
        })?;
        if m.abi != PLUGIN_ABI {
            return Err(SwanFlowError::config_error(format!(
                "plugin '{}': field `abi` is '{}', this host speaks '{}'",
                m.name, m.abi, PLUGIN_ABI
            )));
        }
        if m.entry.is_empty() {
            return Err(SwanFlowError::config_error(format!(
                "plugin '{}': field `entry` is empty — name the exported function to call",
                m.name
            )));
        }
        Ok(m)
    }

    /// Compile and cache. Compilation is the expensive part; instantiation is
    /// per-call so one run cannot observe another's memory.
    fn compile(&self, dir: &Path, manifest: WasmPluginManifest) -> CoreResult<LoadedPlugin> {
        let path = manifest.module_path(dir);
        let bytes = std::fs::read(&path).map_err(|e| {
            SwanFlowError::io_error(format!("plugin '{}': module {}: {}", manifest.name, path.display(), e))
        })?;
        let module = Module::new(&self.engine, &bytes).map_err(|e| {
            SwanFlowError::config_error(format!(
                "plugin '{}': field `module` — not a loadable wasm module: {}",
                manifest.name, e
            ))
        })?;
        if module.get_export(&manifest.entry).is_none() {
            let exports: Vec<String> = module.exports().map(|e| e.name().to_string()).collect();
            return Err(SwanFlowError::config_error(format!(
                "plugin '{}': field `entry` — module exports no '{}' (exports: {:?})",
                manifest.name, manifest.entry, exports
            )));
        }
        for required in ["alloc", "dealloc", "memory"] {
            if module.get_export(required).is_none() {
                return Err(SwanFlowError::config_error(format!(
                    "plugin '{}': missing required export '{}' (ABI {})",
                    manifest.name, required, PLUGIN_ABI
                )));
            }
        }
        Ok(LoadedPlugin {
            manifest,
            module,
            path,
        })
    }

    /// Call `function` with `input`, enforcing the deadline.
    ///
    /// A fresh store per call: no state leaks between runs, and a poisoned
    /// instance is simply dropped.
    fn invoke(
        &self,
        engine: &Engine,
        plugin: &LoadedPlugin,
        function: &str,
        input: &[u8],
    ) -> CoreResult<Vec<u8>> {
        let limits = StoreLimitsBuilder::new()
            .memory_size(self.memory_limit)
            .build();
        let mut store = Store::new(engine, HostState { limits });
        store.limiter(|s| &mut s.limits);
        store.set_epoch_deadline(1);

        // Deadline enforcer. One tick after `timeout`, the store is interrupted
        // at its next wasm instruction boundary.
        let ticker_engine = engine.clone();
        let timeout = self.timeout;
        let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
        let watchdog = std::thread::spawn(move || {
            if stop_rx.recv_timeout(timeout).is_err() {
                ticker_engine.increment_epoch();
            }
        });

        let result = (|| -> CoreResult<Vec<u8>> {
            let linker = Linker::new(engine);
            let instance: Instance = linker.instantiate(&mut store, &plugin.module).map_err(|e| {
                SwanFlowError::config_error(format!(
                    "plugin '{}': instantiate failed: {}",
                    plugin.manifest.name, e
                ))
            })?;

            let alloc = instance
                .get_typed_func::<i32, i32>(&mut store, "alloc")
                .map_err(|_| abi_error(&plugin.manifest.name, "alloc(size: i32) -> i32"))?;
            let dealloc = instance
                .get_typed_func::<(i32, i32), ()>(&mut store, "dealloc")
                .map_err(|_| abi_error(&plugin.manifest.name, "dealloc(ptr: i32, size: i32)"))?;
            let func = instance
                .get_typed_func::<(i32, i32), i32>(&mut store, function)
                .map_err(|_| abi_error(&plugin.manifest.name, &format!("{}(ptr: i32, len: i32) -> i32", function)))?;
            let memory = instance.get_memory(&mut store, "memory").ok_or_else(|| {
                abi_error(&plugin.manifest.name, "linear memory exported as 'memory'")
            })?;

            let input_len = input.len() as i32;
            let input_ptr = alloc.call(&mut store, input_len).map_err(|e| trapped(&plugin.manifest.name, "alloc", e))?;
            if input_ptr == 0 && input_len > 0 {
                return Err(SwanFlowError::unknown(format!(
                    "plugin '{}': alloc({}) returned null",
                    plugin.manifest.name, input_len
                )));
            }
            memory
                .data_mut(&mut store)
                .get_mut(input_ptr as usize..(input_ptr + input_len) as usize)
                .ok_or_else(|| {
                    SwanFlowError::unknown(format!(
                        "plugin '{}': alloc returned {input_ptr} for {input_len} bytes, outside linear memory",
                        plugin.manifest.name
                    ))
                })?
                .copy_from_slice(input);

            let out_ptr = func
                .call(&mut store, (input_ptr, input_len))
                .map_err(|e| trapped(&plugin.manifest.name, function, e))?;

            let data = memory.data(&store);
            let len_bytes: [u8; 4] = data
                .get(out_ptr as usize..out_ptr as usize + 4)
                .ok_or_else(|| {
                    SwanFlowError::unknown(format!(
                        "plugin '{}': output pointer {out_ptr} outside linear memory",
                        plugin.manifest.name
                    ))
                })?
                .try_into()
                .expect("4 bytes");
            let out_len = i32::from_le_bytes(len_bytes);
            if out_len < 0 {
                return Err(SwanFlowError::unknown(format!(
                    "plugin '{}': negative output length {out_len}",
                    plugin.manifest.name
                )));
            }
            let out_start = (out_ptr + 4) as usize;
            let payload = data
                .get(out_start..out_start + out_len as usize)
                .ok_or_else(|| {
                    SwanFlowError::unknown(format!(
                        "plugin '{}': output {out_len} bytes at {out_start} outside linear memory",
                        plugin.manifest.name
                    ))
                })?
                .to_vec();

            // Best-effort free; a plugin that traps here already returned data.
            let _ = dealloc.call(&mut store, (input_ptr, input_len));
            let _ = dealloc.call(&mut store, (out_ptr, out_len + 4));
            Ok(payload)
        })();

        let _ = stop_tx.send(());
        let _ = watchdog.join();
        result
    }
}

/// A JSON error's field, when serde points at one. Keeps the "name the field"
/// promise honest even for structural mistakes.
trait ClassifyField {
    fn classify_field(&self) -> &str;
}

impl ClassifyField for serde_json::Error {
    fn classify_field(&self) -> &str {
        match self.classify() {
            serde_json::error::Category::Data => "abi/entry/module (schema)",
            serde_json::error::Category::Syntax => "not valid JSON",
            serde_json::error::Category::Eof => "truncated JSON",
            serde_json::error::Category::Io => "io",
        }
    }
}

fn abi_error(plugin: &str, expectation: &str) -> SwanFlowError {
    SwanFlowError::config_error(format!(
        "plugin '{plugin}': ABI mismatch — expected export '{expectation}' ({PLUGIN_ABI})"
    ))
}

fn trapped(plugin: &str, function: &str, e: wasmtime::Error) -> SwanFlowError {
    // Match on the trap itself, not on its rendered text: the Display of a wasm
    // backtrace says nothing about *why* it stopped, so string-matching would
    // report a deadline as a malformed input — the exact misdiagnosis this
    // error is supposed to prevent.
    if let Some(trap) = e.downcast_ref::<Trap>() {
        if *trap == Trap::Interrupt {
            return SwanFlowError::new(
                crate::core::error::ErrorCategory::Transient,
                crate::core::error::ErrorCode::Timeout,
                format!(
                    "plugin '{plugin}': '{function}' exceeded its deadline and was interrupted \
                     (the host is still healthy; the instance was discarded)"
                ),
            );
        }
        return SwanFlowError::unknown(format!(
            "plugin '{plugin}': '{function}' trapped ({trap:?})"
        ));
    }
    SwanFlowError::unknown(format!("plugin '{plugin}': '{function}' failed: {e}"))
}

#[async_trait]
impl PluginHost for WasmPluginHost {
    async fn scan(&self, dir: &str) -> CoreResult<Vec<PluginDescriptor>> {
        let dir_path = PathBuf::from(dir);
        let entries = std::fs::read_dir(&dir_path).map_err(|e| {
            SwanFlowError::io_error(format!("plugin dir {}: {}", dir_path.display(), e))
        })?;

        let mut found = Vec::new();
        let mut candidates: Vec<PathBuf> = Vec::new();
        for entry in entries.flatten() {
            let p = entry.path();
            if p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.ends_with(".plugin.json"))
                .unwrap_or(false)
            {
                candidates.push(p);
            }
        }
        candidates.sort();

        for path in candidates {
            // A malformed neighbour must not abort discovery: skip it, and let
            // `load` explain it properly if someone asks for it by name.
            let Ok(manifest) = Self::read_manifest(&path) else {
                continue;
            };
            found.push(PluginDescriptor {
                name: manifest.name.clone(),
                version: manifest.version.clone(),
                plugin_type: PluginType::Wasm,
                description: manifest.description.clone(),
                functions: if manifest.functions.is_empty() {
                    vec![manifest.entry.clone()]
                } else {
                    manifest.functions.clone()
                },
            });
        }
        Ok(found)
    }

    async fn load(&self, name: &str) -> CoreResult<PluginHandle> {
        if let Ok(map) = self.loaded.lock() {
            if map.contains_key(name) {
                return Ok(PluginHandle {
                    name: name.to_string(),
                    plugin_type: PluginType::Wasm,
                });
            }
        }
        Err(SwanFlowError::config_error(format!(
            "plugin '{name}' is not loaded — call `scan` on its directory and `init` its descriptor first"
        )))
    }

    async fn init(&self, handle: &PluginHandle, config: HashMap<String, String>) -> CoreResult<()> {
        let dir = config.get("dir").ok_or_else(|| {
            SwanFlowError::config_error(
                "plugin init: config field `dir` is required (directory holding the descriptor)",
            )
        })?;
        let dir_path = PathBuf::from(dir);
        let manifest_path = dir_path.join(format!("{}.plugin.json", handle.name));
        let manifest = Self::read_manifest(&manifest_path)?;
        if manifest.name != handle.name {
            return Err(SwanFlowError::config_error(format!(
                "plugin '{}': field `name` says '{}' but the descriptor is {}",
                handle.name,
                manifest.name,
                manifest_path.display()
            )));
        }
        let compiled = self.compile(&dir_path, manifest)?;
        self.loaded
            .lock()
            .map_err(|_| SwanFlowError::unknown("plugin registry poisoned"))?
            .insert(handle.name.clone(), compiled);
        Ok(())
    }

    async fn run(&self, handle: &PluginHandle, function: &str, args: &[u8]) -> CoreResult<Vec<u8>> {
        let plugin = {
            let map = self
                .loaded
                .lock()
                .map_err(|_| SwanFlowError::unknown("plugin registry poisoned"))?;
            map.get(&handle.name).ok_or_else(|| {
                SwanFlowError::config_error(format!("plugin '{}' is not loaded", handle.name))
            })?
            .clone_ref()
        };
        let engine = self.engine.clone();
        self.invoke(&engine, &plugin, function, args)
    }

    async fn unload(&self, handle: &PluginHandle) -> CoreResult<()> {
        self.loaded
            .lock()
            .map_err(|_| SwanFlowError::unknown("plugin registry poisoned"))?
            .remove(&handle.name);
        Ok(())
    }

    fn loaded(&self) -> Vec<String> {
        self.loaded
            .lock()
            .map(|m| {
                let mut v: Vec<String> = m.keys().cloned().collect();
                v.sort();
                v
            })
            .unwrap_or_default()
    }
}

impl LoadedPlugin {
    /// A shallow copy: `Module` and the manifest are cheap to clone / share,
    /// the point is only to stop holding the registry lock across execution.
    fn clone_ref(&self) -> LoadedPlugin {
        LoadedPlugin {
            manifest: self.manifest.clone(),
            module: self.module.clone(),
            path: self.path.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `echo` plugin: copies its input back out. Proves the whole
    /// alloc → write → call → read-length → read-payload → dealloc path without
    /// committing a binary fixture; `Module::new` accepts WAT text.
    const ECHO_WAT: &str = r#"
(module
  (memory (export "memory") 4 64)
  (global $bump (mut i32) (i32.const 1024))
  (func $alloc (export "alloc") (param $size i32) (result i32)
    (local $p i32)
    (local.set $p (global.get $bump))
    (global.set $bump
      (i32.add (global.get $bump)
               (i32.and (i32.add (local.get $size) (i32.const 7)) (i32.const -8))))
    (local.get $p))
  (func (export "dealloc") (param i32) (param i32))
  (func (export "echo") (param $ptr i32) (param $len i32) (result i32)
    (local $out i32)
    (local.set $out (call $alloc (i32.add (local.get $len) (i32.const 4))))
    (i32.store (local.get $out) (local.get $len))
    (memory.copy (i32.add (local.get $out) (i32.const 4)) (local.get $ptr) (local.get $len))
    (local.get $out))
)
"#;

    /// A plugin that never returns. Used to prove the deadline is enforced.
    const RUNAWAY_WAT: &str = r#"
(module
  (memory (export "memory") 1 1)
  (func (export "alloc") (param i32) (result i32) (i32.const 16))
  (func (export "dealloc") (param i32) (param i32))
  (func (export "spin") (param i32) (param i32) (result i32)
    (loop $forever (br $forever))
    (i32.const 0))
)
"#;

    fn write_plugin(dir: &Path, name: &str, abi: &str, wat: &str) {
        std::fs::create_dir_all(dir).unwrap();
        // Translate here rather than relying on wasmtime's `wat` feature, which
        // the released runtime does not carry — the test must go through the
        // same "read bytes, Module::new" path production does.
        let wasm = wat::parse_str(wat).expect("hand-written WAT must be valid");
        std::fs::write(dir.join(format!("{name}.wasm")), wasm).unwrap();
        let manifest = format!(
            r#"{{"name":"{name}","version":"0.1.0","abi":"{abi}","entry":"{}","description":"test"}}"#,
            if name == "spin" { "spin" } else { "echo" }
        );
        std::fs::write(dir.join(format!("{name}.plugin.json")), manifest).unwrap();
    }

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "swanflow-plug-{}-{}",
            tag,
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[tokio::test]
    async fn scan_finds_the_valid_plugin_and_skips_the_one_with_a_wrong_abi() {
        let dir = tmpdir("scan");
        write_plugin(&dir, "echo", PLUGIN_ABI, ECHO_WAT);
        write_plugin(&dir, "future", "swanflow-plugin-99", ECHO_WAT);

        let host = WasmPluginHost::new();
        let found = host.scan(dir.to_str().unwrap()).await.unwrap();

        // The good one is discoverable; the bad one does not poison discovery.
        assert_eq!(found.len(), 1, "found: {:?}", found.iter().map(|d| &d.name).collect::<Vec<_>>());
        assert_eq!(found[0].name, "echo");
        assert_eq!(found[0].plugin_type, PluginType::Wasm);

        // ...and asking for it by name explains the mismatch by field name.
        let handle = PluginHandle { name: "future".into(), plugin_type: PluginType::Wasm };
        let mut cfg = HashMap::new();
        cfg.insert("dir".to_string(), dir.to_str().unwrap().to_string());
        let err = host.init(&handle, cfg).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("abi"), "must name the offending field, got: {msg}");
        assert!(msg.contains("swanflow-plugin-99"), "must quote what it found, got: {msg}");
    }

    #[tokio::test]
    async fn run_round_trips_bytes_through_the_abi() {
        let dir = tmpdir("echo");
        write_plugin(&dir, "echo", PLUGIN_ABI, ECHO_WAT);

        let host = WasmPluginHost::new();
        let handle = PluginHandle { name: "echo".into(), plugin_type: PluginType::Wasm };
        let mut cfg = HashMap::new();
        cfg.insert("dir".to_string(), dir.to_str().unwrap().to_string());
        host.init(&handle, cfg).await.unwrap();

        assert_eq!(host.loaded(), vec!["echo".to_string()]);
        let payload = b"\x00\x01far more interesting than an empty buffer\xff";
        let out = host.run(&handle, "echo", payload).await.unwrap();
        assert_eq!(out, payload, "the host must return exactly the plugin's payload, length prefix stripped");
    }

    #[tokio::test]
    async fn a_runaway_plugin_is_interrupted_and_the_host_survives() {
        let dir = tmpdir("spin");
        write_plugin(&dir, "spin", PLUGIN_ABI, RUNAWAY_WAT);

        // 150 ms is enough: the plugin never yields, so only the epoch can stop it.
        let host = WasmPluginHost::with_limits(Duration::from_millis(150), DEFAULT_MEMORY_LIMIT);
        let handle = PluginHandle { name: "spin".into(), plugin_type: PluginType::Wasm };
        let mut cfg = HashMap::new();
        cfg.insert("dir".to_string(), dir.to_str().unwrap().to_string());
        host.init(&handle, cfg).await.unwrap();

        let started = std::time::Instant::now();
        let err = host.run(&handle, "spin", b"x").await.unwrap_err();
        let elapsed = started.elapsed();

        let msg = err.to_string();
        assert!(
            msg.contains("deadline") || msg.contains("interrupted"),
            "the error must say it ran out of time, not blame the data: {msg}"
        );
        assert!(elapsed < Duration::from_secs(5), "deadline not enforced: took {elapsed:?}");

        // The host is still usable afterwards — that is the claim being tested.
        assert_eq!(host.loaded(), vec!["spin".to_string()]);
    }
}
