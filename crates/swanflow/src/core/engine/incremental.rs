//! Incremental execution — resume incomplete workflows from checkpoint.
//!
//! If an execution was interrupted mid-way (e.g., crash, timeout), the engine
//! can detect the last completed level and skip it on re-run, starting from
//! the next uncompleted level.

use crate::core::engine::checkpoint::{CheckpointManager, EngineCheckpoint};

/// Given a list of resumable checkpoints, find the one that matches the
/// given workflow name and return the next level to execute.
///
/// Returns `None` if no resumable checkpoint exists for this workflow
/// (i.e., execute from level 0).
pub fn find_resume_level(
    checkpoints: &[EngineCheckpoint],
    workflow_name: &str,
) -> Option<usize> {
    for cp in checkpoints {
        if cp.workflow_name == workflow_name && cp.completed_at.is_none() {
            // Return the next level after the highest completed level
            return cp.completed_levels.iter().max().map(|l| l + 1);
        }
    }
    None
}

/// Compute the list of levels that have already been completed for a workflow.
/// Returns an empty vec if no resumable checkpoint exists.
pub fn completed_levels(
    checkpoints: &[EngineCheckpoint],
    workflow_name: &str,
) -> Vec<usize> {
    for cp in checkpoints {
        if cp.workflow_name == workflow_name && cp.completed_at.is_none() {
            return cp.completed_levels.clone();
        }
    }
    vec![]
}

/// Check if a given level is already completed and can be skipped.
pub fn is_level_completed(
    checkpoints: &[EngineCheckpoint],
    workflow_name: &str,
    level: usize,
) -> bool {
    for cp in checkpoints {
        if cp.workflow_name == workflow_name && cp.completed_at.is_none() {
            return cp.completed_levels.contains(&level);
        }
    }
    false
}

/// Get the skip count — number of already-completed levels to fast-forward past.
pub fn skip_count(
    checkpoints: &[EngineCheckpoint],
    workflow_name: &str,
) -> usize {
    find_resume_level(checkpoints, workflow_name).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_cp(name: &str, completed: Vec<usize>, total: usize, done: bool) -> EngineCheckpoint {
        EngineCheckpoint {
            run_id: "test-run".to_string(),
            workflow_hash: name.to_string(),
            workflow_name: name.to_string(),
            completed_levels: completed,
            total_levels: total,
            materialized_tables: HashMap::new(),
            flow_vars: HashMap::new(),
            started_at: "".to_string(),
            completed_at: if done { Some("done".to_string()) } else { None },
            digest_version: crate::core::engine::assets::DIGEST_VERSION,
        }
    }

    #[test]
    fn test_find_resume_level_with_incomplete_workflow() {
        let cps = vec![make_cp("wf-a", vec![0, 1], 4, false)];
        assert_eq!(find_resume_level(&cps, "wf-a"), Some(2));
    }

    #[test]
    fn test_find_resume_level_with_completed_workflow() {
        let cps = vec![make_cp("wf-a", vec![0, 1, 2, 3], 4, true)];
        assert_eq!(find_resume_level(&cps, "wf-a"), None);
    }

    #[test]
    fn test_find_resume_level_no_checkpoint() {
        let cps: Vec<EngineCheckpoint> = vec![];
        assert_eq!(find_resume_level(&cps, "wf-a"), None);
    }

    #[test]
    fn test_find_resume_level_wrong_name() {
        let cps = vec![make_cp("wf-b", vec![0], 3, false)];
        assert_eq!(find_resume_level(&cps, "wf-a"), None);
    }

    #[test]
    fn test_find_resume_level_first_level_done() {
        let cps = vec![make_cp("wf-a", vec![0], 5, false)];
        assert_eq!(find_resume_level(&cps, "wf-a"), Some(1));
    }

    #[test]
    fn test_skip_count_no_checkpoint() {
        let cps: Vec<EngineCheckpoint> = vec![];
        assert_eq!(skip_count(&cps, "wf-a"), 0);
    }

    #[test]
    fn test_skip_count_with_two_levels_done() {
        let cps = vec![make_cp("wf-a", vec![0, 1], 5, false)];
        assert_eq!(skip_count(&cps, "wf-a"), 2);
    }

    #[test]
    fn test_completed_levels_returns_empty_for_completed_workflow() {
        let cps = vec![make_cp("wf-a", vec![0, 1, 2], 3, true)];
        let levels = completed_levels(&cps, "wf-a");
        assert!(levels.is_empty(), "completed workflow should return empty");
    }
}
