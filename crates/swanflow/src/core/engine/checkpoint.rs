//! Engine-level execution checkpoint system.
//!
//! After each execution level completes, a checkpoint is saved to the
//! metadata DuckDB. On restart, the engine can detect incomplete workflows
//! and resume from the last completed level.

use duckdb::{params, Connection};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A snapshot of workflow execution progress.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EngineCheckpoint {
    /// Unique identifier for this execution run.
    pub run_id: String,
    /// Hash of the workflow definition (used to match resumed workflows).
    pub workflow_hash: String,
    /// Human-readable workflow name for display.
    pub workflow_name: String,
    /// 0-indexed list of completed execution levels.
    pub completed_levels: Vec<usize>,
    /// Total number of levels in this workflow.
    pub total_levels: usize,
    /// Map of node_id → materialized table name.
    pub materialized_tables: HashMap<String, String>,
    /// Flow variable values at checkpoint time.
    pub flow_vars: HashMap<String, serde_json::Value>,
    /// ISO 8601 timestamp when execution started.
    pub started_at: String,
    /// ISO 8601 timestamp when execution completed (None if still running).
    pub completed_at: Option<String>,
    /// Digest algorithm version that produced this checkpoint's skipped
    /// levels. When the asset digest algorithm changes (e.g. v1 -> v2 in
    /// `d774c25`), pre-upgrade checkpoints may reference materializations
    /// recorded under the old algorithm — resuming them would bypass the
    /// v2 invalidation. A version mismatch makes the checkpoint non-
    /// resumable (the workflow re-runs from level 0, one time).
    #[serde(default)]
    pub digest_version: u32,
}

/// Manages reading and writing execution checkpoints to the metadata store.
pub struct CheckpointManager {
    /// Path to the metadata DuckDB file.
    metadata_path: String,
}

impl CheckpointManager {
    /// Create a new CheckpointManager backed by the given cache directory.
    pub fn new(cache_dir: &str) -> Self {
        let metadata_path = format!("{}/metadata.duckdb", cache_dir);
        Self::ensure_table(&metadata_path);
        Self { metadata_path }
    }

    /// Create a CheckpointManager with an explicit metadata path.
    pub fn with_path(metadata_path: &str) -> Self {
        Self::ensure_table(metadata_path);
        Self {
            metadata_path: metadata_path.to_string(),
        }
    }

    /// Ensure the checkpoint table exists in the metadata DB.
    fn ensure_table(metadata_path: &str) {
        if let Ok(conn) = Connection::open(metadata_path) {
            let _ = conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS __execution_checkpoints (
                    run_id TEXT PRIMARY KEY,
                    workflow_hash TEXT NOT NULL,
                    workflow_name TEXT NOT NULL DEFAULT '',
                    checkpoint_json TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT now(),
                    updated_at TIMESTAMP DEFAULT now()
                );",
            );
            let _ = conn.execute_batch(
                "CREATE INDEX IF NOT EXISTS idx_checkpoint_hash
                    ON __execution_checkpoints(workflow_hash);",
            );
        }
    }

    /// Save a checkpoint (insert or update).
    pub fn save_checkpoint(&self, cp: &EngineCheckpoint) -> anyhow::Result<()> {
        let conn = Connection::open(&self.metadata_path)?;
        let json = serde_json::to_string(cp)?;
        conn.execute(
            "INSERT INTO __execution_checkpoints (run_id, workflow_hash, workflow_name, checkpoint_json)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(run_id) DO UPDATE SET
                checkpoint_json = excluded.checkpoint_json,
                workflow_hash = excluded.workflow_hash,
                workflow_name = excluded.workflow_name,
                updated_at = now()",
            params![cp.run_id, cp.workflow_hash, cp.workflow_name, json],
        )?;
        Ok(())
    }

    /// Load a checkpoint by run_id.
    pub fn load_checkpoint(&self, run_id: &str) -> anyhow::Result<Option<EngineCheckpoint>> {
        let conn = Connection::open(&self.metadata_path)?;
        let mut stmt =
            conn.prepare("SELECT checkpoint_json FROM __execution_checkpoints WHERE run_id = ?1")?;
        let mut rows = stmt.query(params![run_id])?;
        match rows.next()? {
            Some(row) => {
                let json: String = row.get(0)?;
                let cp: EngineCheckpoint = serde_json::from_str(&json)?;
                Ok(Some(cp))
            }
            None => Ok(None),
        }
    }

    /// Delete a checkpoint by run_id.
    pub fn delete_checkpoint(&self, run_id: &str) -> anyhow::Result<()> {
        let conn = Connection::open(&self.metadata_path)?;
        conn.execute(
            "DELETE FROM __execution_checkpoints WHERE run_id = ?1",
            params![run_id],
        )?;
        Ok(())
    }

    /// Find an incomplete (resumable) checkpoint matching the given workflow hash.
    /// Returns None if the workflow has never been started, the last run completed,
    /// or the checkpoint predates the current digest algorithm version
    /// (codex-review P1 r2: resuming a pre-v2 checkpoint would bypass the
    /// v2 digest invalidation and reuse stale v1-era materializations).
    pub fn find_resumable(&self, workflow_hash: &str) -> anyhow::Result<Option<EngineCheckpoint>> {
        let conn = Connection::open(&self.metadata_path)?;
        let mut stmt = conn.prepare(
            "SELECT checkpoint_json FROM __execution_checkpoints
             WHERE workflow_hash = ?1
             ORDER BY updated_at DESC
             LIMIT 1",
        )?;
        let mut rows = stmt.query(params![workflow_hash])?;
        match rows.next()? {
            Some(row) => {
                let json: String = row.get(0)?;
                let cp: EngineCheckpoint = serde_json::from_str(&json)?;
                // Only return if NOT completed
                // Both conditions mean "this checkpoint is not reusable": a
                // completed run, or one written by a different digest scheme.
                // Collapsed into one arm — the two branches were identical.
                if cp.completed_at.is_some()
                    || cp.digest_version != crate::core::engine::assets::DIGEST_VERSION
                {
                    Ok(None)
                } else {
                    Ok(Some(cp))
                }
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_serialization_roundtrip() {
        let cp = EngineCheckpoint {
            run_id: "ser-test".to_string(),
            workflow_hash: "hash-xyz".to_string(),
            workflow_name: "ser-test-wf".to_string(),
            completed_levels: vec![0, 1, 2],
            total_levels: 5,
            materialized_tables: {
                let mut m = HashMap::new();
                m.insert("n1".to_string(), "t1".to_string());
                m
            },
            flow_vars: HashMap::new(),
            started_at: "now".to_string(),
            completed_at: None,
            digest_version: crate::core::engine::assets::DIGEST_VERSION,
        };

        let json = serde_json::to_string(&cp).unwrap();
        let deserialized: EngineCheckpoint = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.run_id, "ser-test");
        assert_eq!(deserialized.completed_levels, vec![0, 1, 2]);
        assert_eq!(
            deserialized.materialized_tables.get("n1"),
            Some(&"t1".to_string())
        );
    }

    #[test]
    fn test_checkpoint_save_and_load() {
        // Use a temp dir to avoid cross-test pollution
        let tmp = std::env::temp_dir().join(format!("checkpoint_test_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&tmp);
        let cm = CheckpointManager::new(tmp.to_str().unwrap());

        let cp = EngineCheckpoint {
            run_id: "test-run-1".to_string(),
            workflow_hash: "hash-1".to_string(),
            workflow_name: "wf1".to_string(),
            completed_levels: vec![0],
            total_levels: 3,
            materialized_tables: HashMap::new(),
            flow_vars: HashMap::new(),
            started_at: "2026-01-01T00:00:00Z".to_string(),
            completed_at: None,
            digest_version: crate::core::engine::assets::DIGEST_VERSION,
        };

        cm.save_checkpoint(&cp).unwrap();
        let loaded = cm.load_checkpoint("test-run-1").unwrap().unwrap();
        assert_eq!(loaded.run_id, "test-run-1");
        assert_eq!(loaded.completed_levels, vec![0]);

        cm.delete_checkpoint("test-run-1").unwrap();
        let deleted = cm.load_checkpoint("test-run-1").unwrap();
        assert!(deleted.is_none());

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn test_find_resumable_skips_completed() {
        let tmp = std::env::temp_dir().join(format!("checkpoint_resume_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&tmp);
        let cm = CheckpointManager::new(tmp.to_str().unwrap());

        // Save incomplete
        let cp = EngineCheckpoint {
            run_id: "run-incomplete".to_string(),
            workflow_hash: "hash-resume".to_string(),
            workflow_name: "resume-test".to_string(),
            completed_levels: vec![0],
            total_levels: 3,
            materialized_tables: HashMap::new(),
            flow_vars: HashMap::new(),
            started_at: "2026-01-01T00:00:00Z".to_string(),
            completed_at: None,
            digest_version: crate::core::engine::assets::DIGEST_VERSION,
        };
        cm.save_checkpoint(&cp).unwrap();

        let found = cm.find_resumable("hash-resume").unwrap();
        assert!(found.is_some(), "incomplete run should be resumable");

        // Now mark as completed
        let mut completed = cp;
        completed.completed_at = Some("2026-01-01T01:00:00Z".to_string());
        cm.save_checkpoint(&completed).unwrap();

        let not_found = cm.find_resumable("hash-resume").unwrap();
        assert!(not_found.is_none(), "completed run should not be resumable");

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn test_find_resumable_rejects_old_digest_version() {
        // codex-review P1 r2: a pre-v2 checkpoint must not be resumable —
        // resuming it would skip levels whose materializations were
        // recorded under the old digest algorithm, bypassing the v2
        // invalidation. The workflow simply re-runs from level 0 once.
        let tmp = std::env::temp_dir().join(format!("checkpoint_resume_v1_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&tmp);
        let cm = CheckpointManager::new(tmp.to_str().unwrap());

        let cp = EngineCheckpoint {
            run_id: "run-v1".to_string(),
            workflow_hash: "hash-v1".to_string(),
            workflow_name: "v1-wf".to_string(),
            completed_levels: vec![0, 1],
            total_levels: 3,
            materialized_tables: HashMap::new(),
            flow_vars: HashMap::new(),
            started_at: "2026-01-01T00:00:00Z".to_string(),
            completed_at: None,
            // Simulate a checkpoint written before DIGEST_VERSION existed
            // (serde default) or under an older algorithm.
            digest_version: crate::core::engine::assets::DIGEST_VERSION - 1,
        };
        cm.save_checkpoint(&cp).unwrap();

        let found = cm.find_resumable("hash-v1").unwrap();
        assert!(
            found.is_none(),
            "pre-v2 checkpoint must not be resumable (would bypass v2 digest invalidation)"
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }
}
