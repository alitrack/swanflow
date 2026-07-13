//! Router configuration — loading from settings and validation.
//!
//! Provides `RouterConfig::from_settings()` to construct a `RouterConfig` from
//! YAML/JSON settings, and `validate()` to check configuration correctness.

use crate::router::RouterConfig;
use crate::AiError;

impl RouterConfig {
    /// Build a `RouterConfig` from YAML settings string.
    ///
    /// Accepts YAML (preferred) or JSON. Missing fields use `Default` values.
    /// Thresholds default to `simple_threshold: 1` and `complex_threshold: 4`
    /// if not specified.
    ///
    /// # Errors
    ///
    /// Returns `AiError::ParseError` if the YAML/JSON is malformed.
    pub fn from_settings(settings: &str) -> Result<Self, AiError> {
        // Try YAML first
        match serde_yaml::from_str::<RouterConfig>(settings) {
            Ok(cfg) => return Ok(cfg),
            Err(_) => {}
        }
        // Fallback to JSON
        serde_json::from_str(settings)
            .map_err(|e| AiError::ParseError(format!("Failed to parse RouterConfig: {}", e)))
    }

    /// Validate the router configuration.
    ///
    /// Checks:
    /// - Model names are non-empty
    /// - Endpoints are non-empty
    /// - Timeouts are reasonable (1–300 seconds)
    /// - Thresholds are in valid range: `simple_threshold < complex_threshold`
    ///
    /// # Errors
    ///
    /// Returns `AiError::InvalidResponse` with a description of the first validation failure.
    pub fn validate(&self) -> Result<(), AiError> {
        // Model names must be non-empty
        if self.cheap.model.is_empty() {
            return Err(AiError::InvalidResponse(
                "cheap model name is empty".into(),
            ));
        }
        if self.mid.model.is_empty() {
            return Err(AiError::InvalidResponse(
                "mid model name is empty".into(),
            ));
        }
        if self.best.model.is_empty() {
            return Err(AiError::InvalidResponse(
                "best model name is empty".into(),
            ));
        }

        // Endpoints must be non-empty
        if self.cheap.endpoint.is_empty() {
            return Err(AiError::InvalidResponse(
                "cheap model endpoint is empty".into(),
            ));
        }
        if self.mid.endpoint.is_empty() {
            return Err(AiError::InvalidResponse(
                "mid model endpoint is empty".into(),
            ));
        }
        if self.best.endpoint.is_empty() {
            return Err(AiError::InvalidResponse(
                "best model endpoint is empty".into(),
            ));
        }

        // Timeouts must be in reasonable range
        for (label, timeout) in &[
            ("cheap", self.cheap.timeout_secs),
            ("mid", self.mid.timeout_secs),
            ("best", self.best.timeout_secs),
        ] {
            if *timeout == 0 || *timeout > 300 {
                return Err(AiError::InvalidResponse(format!(
                    "{} model timeout_secs {} is out of range (1–300)",
                    label, timeout
                )));
            }
        }

        // Thresholds: simple_threshold must be less than complex_threshold
        if self.simple_threshold >= self.complex_threshold {
            return Err(AiError::InvalidResponse(format!(
                "simple_threshold ({}) must be less than complex_threshold ({})",
                self.simple_threshold, self.complex_threshold
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_settings_parses_yaml() {
        let yaml = r#"
cheap:
  model: gpt-3.5-turbo
  endpoint: https://api.openai.com/v1
  timeout_secs: 30
mid:
  model: gpt-4
  endpoint: https://api.openai.com/v1
  timeout_secs: 60
best:
  model: gpt-4-turbo
  endpoint: https://api.openai.com/v1
  timeout_secs: 120
"#;
        let config = RouterConfig::from_settings(yaml).unwrap();
        assert_eq!(config.cheap.model, "gpt-3.5-turbo");
        assert_eq!(config.simple_threshold, 1); // default
        assert_eq!(config.complex_threshold, 4); // default
    }

    #[test]
    fn from_settings_parses_yaml_with_thresholds() {
        let yaml = r#"
cheap:
  model: cheap
  endpoint: http://a
  timeout_secs: 10
mid:
  model: mid
  endpoint: http://b
  timeout_secs: 10
best:
  model: best
  endpoint: http://c
  timeout_secs: 10
simple_threshold: 2
complex_threshold: 5
"#;
        let config = RouterConfig::from_settings(yaml).unwrap();
        assert_eq!(config.simple_threshold, 2);
        assert_eq!(config.complex_threshold, 5);
    }

    #[test]
    fn from_settings_parses_json() {
        let json = r#"{"cheap":{"model":"c","endpoint":"http://a","timeout_secs":10},"mid":{"model":"m","endpoint":"http://b","timeout_secs":10},"best":{"model":"b","endpoint":"http://c","timeout_secs":10}}"#;
        let config = RouterConfig::from_settings(json).unwrap();
        assert_eq!(config.cheap.model, "c");
    }

    #[test]
    fn from_settings_rejects_bad_input() {
        let result = RouterConfig::from_settings("{{{ not valid yaml or json");
        assert!(result.is_err());
    }

    #[test]
    fn validate_passes_for_good_config() {
        let config = RouterConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn validate_fails_on_empty_model_name() {
        let mut config = RouterConfig::default();
        config.cheap.model = String::new();
        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_fails_on_empty_endpoint() {
        let mut config = RouterConfig::default();
        config.mid.endpoint = String::new();
        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_fails_on_zero_timeout() {
        let mut config = RouterConfig::default();
        config.best.timeout_secs = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_fails_on_excessive_timeout() {
        let mut config = RouterConfig::default();
        config.cheap.timeout_secs = 500;
        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_fails_on_inverted_thresholds() {
        let mut config = RouterConfig::default();
        config.simple_threshold = 5;
        config.complex_threshold = 3;
        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_fails_on_equal_thresholds() {
        let mut config = RouterConfig::default();
        config.simple_threshold = 3;
        config.complex_threshold = 3;
        assert!(config.validate().is_err());
    }
}
