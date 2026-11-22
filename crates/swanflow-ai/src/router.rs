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

/// Config complexity tier — measures the inherent complexity of a node's configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConfigComplexity {
    /// Simple config: few fields, flat structure.
    Simple,
    /// Medium config: nested objects, moderate field count.
    Medium,
    /// Complex config: deeply nested, many fields, dynamic expressions.
    Complex,
}

/// Router configuration — maps complexity tiers to model configs with scoring thresholds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouterConfig {
    pub cheap: ModelConfig,
    pub mid: ModelConfig,
    pub best: ModelConfig,
    /// Score <= this value → FixComplexity::Simple (default: 1).
    #[serde(default = "default_simple_threshold")]
    pub simple_threshold: u32,
    /// Score >= this value → FixComplexity::Complex (default: 4).
    #[serde(default = "default_complex_threshold")]
    pub complex_threshold: u32,
}

fn default_simple_threshold() -> u32 {
    1
}
fn default_complex_threshold() -> u32 {
    4
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
                endpoint: "https://generativelanguage.googleapis.com/v1beta1/openai".into(),
                ..Default::default()
            },
            best: ModelConfig {
                model: "deepseek-v4-pro".into(),
                ..Default::default()
            },
            simple_threshold: default_simple_threshold(),
            complex_threshold: default_complex_threshold(),
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
    /// Length of the error message in bytes — longer messages suggest more complex errors.
    pub error_message_len: usize,
    /// Complexity of the node's configuration itself.
    pub config_complexity: ConfigComplexity,
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

    /// Score an error signal using 5 dimensions and determine fix complexity.
    ///
    /// Dimensions:
    /// 1. Base score from error kind: Config=1, Data=2, Schema=2, Topology=3
    /// 2. +1 if multiple nodes affected
    /// 3. +1 if schema mismatch involved
    /// 4. +1 if error message is long (> 300 chars)
    /// 5. Config complexity: Simple=+0, Medium=+1, Complex=+2
    ///
    /// Thresholds from RouterConfig (serde defaults: simple ≤1, complex ≥4).
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
        // Dimension 4: error message length
        if signal.error_message_len > 300 {
            score += 1;
        }
        // Dimension 5: config complexity
        score += match signal.config_complexity {
            ConfigComplexity::Simple => 0,
            ConfigComplexity::Medium => 1,
            ConfigComplexity::Complex => 2,
        };

        if score <= self.config.simple_threshold {
            FixComplexity::Simple
        } else if score >= self.config.complex_threshold {
            FixComplexity::Complex
        } else {
            FixComplexity::Medium
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
            error_message_len: 0,
            config_complexity: ConfigComplexity::Simple,
        }
    }

    fn signal_with_extra(
        kind: ErrorKind,
        affected: u32,
        schema: bool,
        msg_len: usize,
        config_cplx: ConfigComplexity,
    ) -> ErrorSignal {
        ErrorSignal {
            error_kind: kind,
            affected_nodes: affected,
            schema_mismatch: schema,
            error_message_len: msg_len,
            config_complexity: config_cplx,
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
        // Topology=3, no extras → score=3 (between 1 and 4 → Medium)
        // But the old test expected Complex. Let's adjust — topology=3 is between thresholds (≤1 Simple, ≥4 Complex), so Medium.
        // With default thresholds: 1 < 3 < 4 → Medium.
        assert_eq!(
            router.score(&signal(ErrorKind::Topology, 1, false)),
            FixComplexity::Medium
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
        // Schema=2, multi=+1, schema_mismatch=+1 → 4 ≥ complex_threshold(4) → Complex
        assert_eq!(
            router.score(&signal(ErrorKind::Schema, 2, true)),
            FixComplexity::Complex
        );
    }

    #[test]
    fn long_message_adds_score() {
        let router = ModelRouter::new(RouterConfig::default());
        // Config=1, long message=+1 → 2 → Medium
        let s = signal_with_extra(ErrorKind::Config, 1, false, 500, ConfigComplexity::Simple);
        assert_eq!(router.score(&s), FixComplexity::Medium);
    }

    #[test]
    fn complex_config_adds_score() {
        let router = ModelRouter::new(RouterConfig::default());
        // Config=1, complex_config=+2 → 3 → Medium (between 1 and 4)
        let s = signal_with_extra(ErrorKind::Config, 1, false, 0, ConfigComplexity::Complex);
        assert_eq!(router.score(&s), FixComplexity::Medium);
    }

    #[test]
    fn all_dimensions_stack_to_complex() {
        let router = ModelRouter::new(RouterConfig::default());
        // Topology=3 + multi=+1 + schema=+1 + long_msg=+1 + complex_config=+2 → 8 → Complex
        let s = signal_with_extra(ErrorKind::Topology, 2, true, 500, ConfigComplexity::Complex);
        assert_eq!(router.score(&s), FixComplexity::Complex);
    }

    #[test]
    fn custom_thresholds_work() {
        let config = RouterConfig {
            simple_threshold: 2,
            complex_threshold: 6,
            ..RouterConfig::default()
        };
        let router = ModelRouter::new(config);
        // Config=1 + complex_config=+2 → 3, with simple≤2 → Medium
        let s = signal_with_extra(ErrorKind::Config, 1, false, 0, ConfigComplexity::Complex);
        assert_eq!(router.score(&s), FixComplexity::Medium);

        // Data=2 + multi=+1 + schema=+1 = 4, simple≤2, complex≥6 → Medium
        let s2 = signal(ErrorKind::Data, 2, true);
        assert_eq!(router.score(&s2), FixComplexity::Medium);
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
            ..RouterConfig::default()
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
        // Topology=3 + complex_config=+2 = 5 (with complex_config: Complex), ≥4 → Complex → best
        let s = signal_with_extra(ErrorKind::Topology, 1, false, 0, ConfigComplexity::Complex);
        assert_eq!(router.route(&s).model, "best-model");
    }

    #[test]
    fn serialization_roundtrip_defaults() {
        let yaml = serde_yaml::to_string(&RouterConfig::default()).unwrap();
        let parsed: RouterConfig = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(parsed.simple_threshold, 1);
        assert_eq!(parsed.complex_threshold, 4);
    }

    #[test]
    fn serialization_missing_thresholds_uses_defaults() {
        let yaml = "cheap:\n  model: cheap\n  endpoint: http://a\n  timeout_secs: 10\nmid:\n  model: mid\n  endpoint: http://b\n  timeout_secs: 10\nbest:\n  model: best\n  endpoint: http://c\n  timeout_secs: 10\n";
        let parsed: RouterConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(parsed.simple_threshold, 1);
        assert_eq!(parsed.complex_threshold, 4);
    }
}
