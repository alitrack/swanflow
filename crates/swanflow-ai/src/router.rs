//! Model router — heuristic complexity scoring → budget-aware model selection.
//!
//! Inspired by OpenFang's `routing.rs:51-61` heuristic scoring pattern,
//! adapted for SwanFlow's `NodeDiagnostic` error classification.

use crate::ModelConfig;
use serde::{Deserialize, Serialize};

/// Fix complexity tier — determines which model to use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FixComplexity {
    /// Simple fixes: parameter typos, missing defaults — use cheapest model.
    Simple,
    /// Medium fixes: port type mismatch, schema drift — use mid-tier model.
    Medium,
    /// Complex fixes: DAG topology error, multi-node reconfiguration — use best model.
    Complex,
}

/// Router configuration — maps complexity tiers to model configs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouterConfig {
    pub cheap: ModelConfig,
    pub mid: ModelConfig,
    pub best: ModelConfig,
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            cheap: ModelConfig {
                model: "deepseek-v4-flash".into(),
                ..Default::default()
            },
            mid: ModelConfig {
                model: "gemini-2.5-flash".into(),
                endpoint: "https://generativelanguage.googleapis.com/v1beta/openai".into(),
                ..Default::default()
            },
            best: ModelConfig {
                model: "deepseek-v4-pro".into(),
                ..Default::default()
            },
        }
    }
}

/// Error classification signal used by the router.
#[derive(Debug, Clone)]
pub struct ErrorSignal {
    /// The error class from the node diagnostic.
    pub error_kind: ErrorKind,
    /// Number of nodes affected by this error.
    pub affected_nodes: u32,
    /// Whether the error involves schema/type mismatch.
    pub schema_mismatch: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Configuration errors: wrong param value, missing field.
    Config,
    /// Data errors: null values, type conversion, unexpected format.
    Data,
    /// Schema errors: column mismatch, type drift.
    Schema,
    /// Topology errors: broken connection, DAG cycle, missing node.
    Topology,
}

/// Model router — scores errors and picks the cheapest capable model.
pub struct ModelRouter {
    config: RouterConfig,
}

impl ModelRouter {
    pub fn new(config: RouterConfig) -> Self {
        Self { config }
    }

    /// Route an error signal to the appropriate model.
    pub fn route(&self, signal: &ErrorSignal) -> ModelConfig {
        match self.score(signal) {
            FixComplexity::Simple => self.config.cheap.clone(),
            FixComplexity::Medium => self.config.mid.clone(),
            FixComplexity::Complex => self.config.best.clone(),
        }
    }

    /// Score an error signal and determine its fix complexity.
    ///
    /// Heuristics (inspired by OpenFang's `TaskComplexity::score()`):
    /// 1. Base score from error kind: Config=1, Data=2, Schema=2, Topology=3
    /// 2. +1 if multiple nodes affected
    /// 3. +1 if schema mismatch involved
    ///
    /// Thresholds: ≤1 → Simple, 2 → Medium, ≥3 → Complex
    pub fn score(&self, signal: &ErrorSignal) -> FixComplexity {
        let base = match signal.error_kind {
            ErrorKind::Config => 1,
            ErrorKind::Data => 2,
            ErrorKind::Schema => 2,
            ErrorKind::Topology => 3,
        };

        let mut score = base;
        if signal.affected_nodes > 1 {
            score += 1;
        }
        if signal.schema_mismatch {
            score += 1;
        }

        match score {
            0..=1 => FixComplexity::Simple,
            2 => FixComplexity::Medium,
            _ => FixComplexity::Complex,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signal(kind: ErrorKind, affected: u32, schema: bool) -> ErrorSignal {
        ErrorSignal {
            error_kind: kind,
            affected_nodes: affected,
            schema_mismatch: schema,
        }
    }

    #[test]
    fn config_error_single_node_is_simple() {
        let router = ModelRouter::new(RouterConfig::default());
        assert_eq!(
            router.score(&signal(ErrorKind::Config, 1, false)),
            FixComplexity::Simple
        );
    }

    #[test]
    fn data_error_single_node_is_medium() {
        let router = ModelRouter::new(RouterConfig::default());
        assert_eq!(
            router.score(&signal(ErrorKind::Data, 1, false)),
            FixComplexity::Medium
        );
    }

    #[test]
    fn topology_error_is_complex() {
        let router = ModelRouter::new(RouterConfig::default());
        assert_eq!(
            router.score(&signal(ErrorKind::Topology, 1, false)),
            FixComplexity::Complex
        );
    }

    #[test]
    fn config_with_multi_node_is_medium() {
        let router = ModelRouter::new(RouterConfig::default());
        assert_eq!(
            router.score(&signal(ErrorKind::Config, 2, false)),
            FixComplexity::Medium
        );
    }

    #[test]
    fn data_with_schema_and_multi_node_is_complex() {
        let router = ModelRouter::new(RouterConfig::default());
        assert_eq!(
            router.score(&signal(ErrorKind::Schema, 2, true)),
            FixComplexity::Complex
        );
    }

    #[test]
    fn route_selects_correct_model() {
        let config = RouterConfig {
            cheap: ModelConfig {
                model: "cheap-model".into(),
                ..Default::default()
            },
            mid: ModelConfig {
                model: "mid-model".into(),
                ..Default::default()
            },
            best: ModelConfig {
                model: "best-model".into(),
                ..Default::default()
            },
        };
        let router = ModelRouter::new(config);

        assert_eq!(
            router.route(&signal(ErrorKind::Config, 1, false)).model,
            "cheap-model"
        );
        assert_eq!(
            router.route(&signal(ErrorKind::Data, 1, false)).model,
            "mid-model"
        );
        assert_eq!(
            router.route(&signal(ErrorKind::Topology, 1, false)).model,
            "best-model"
        );
    }
}
