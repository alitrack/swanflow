use anyhow::{anyhow, Result};
use serde_json::Value as JsonValue;

pub fn required_string(config: &JsonValue, key: &str) -> Result<String> {
    config[key]
        .as_str()
        .map(ToString::to_string)
        .ok_or_else(|| anyhow!("'{}' is required and must be a string", key))
}

pub fn optional_string(config: &JsonValue, key: &str) -> Option<String> {
    config[key].as_str().map(ToString::to_string)
}

pub fn required_number(config: &JsonValue, key: &str) -> Result<f64> {
    config[key]
        .as_f64()
        .ok_or_else(|| anyhow!("'{}' is required and must be a number", key))
}

pub fn optional_number(config: &JsonValue, key: &str) -> Option<f64> {
    config[key].as_f64()
}

pub fn required_bool(config: &JsonValue, key: &str) -> Result<bool> {
    config[key]
        .as_bool()
        .ok_or_else(|| anyhow!("'{}' is required and must be a bool", key))
}

pub fn optional_bool(config: &JsonValue, key: &str) -> Option<bool> {
    config[key].as_bool()
}

pub fn required_array<'a>(config: &'a JsonValue, key: &str) -> Result<&'a Vec<JsonValue>> {
    config[key]
        .as_array()
        .ok_or_else(|| anyhow!("'{}' is required and must be an array", key))
}

pub fn optional_array<'a>(config: &'a JsonValue, key: &str) -> Option<&'a Vec<JsonValue>> {
    config[key].as_array()
}

pub fn optional_string_array(config: &JsonValue, key: &str) -> Result<Vec<String>> {
    match config[key].as_array() {
        Some(arr) => arr
            .iter()
            .map(|v| {
                v.as_str()
                    .map(ToString::to_string)
                    .ok_or_else(|| anyhow!("'{}' must contain only strings", key))
            })
            .collect(),
        None => Ok(Vec::new()),
    }
}

pub fn required_string_array(config: &JsonValue, key: &str) -> Result<Vec<String>> {
    required_array(config, key)?
        .iter()
        .map(|v| {
            v.as_str()
                .map(ToString::to_string)
                .ok_or_else(|| anyhow!("'{}' must contain only strings", key))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── required_string ──

    #[test]
    fn required_string_ok() {
        let cfg = json!({"name": "hello"});
        assert_eq!(required_string(&cfg, "name").unwrap(), "hello");
    }

    #[test]
    fn required_string_missing_key() {
        let cfg = json!({});
        assert!(required_string(&cfg, "name").is_err());
    }

    #[test]
    fn required_string_wrong_type() {
        let cfg = json!({"name": 42});
        assert!(required_string(&cfg, "name").is_err());
    }

    #[test]
    fn required_string_null() {
        let cfg = json!({"name": null});
        assert!(required_string(&cfg, "name").is_err());
    }

    #[test]
    fn required_string_empty() {
        let cfg = json!({"name": ""});
        assert_eq!(required_string(&cfg, "name").unwrap(), "");
    }

    // ── optional_string ──

    #[test]
    fn optional_string_present() {
        let cfg = json!({"tag": "v1"});
        assert_eq!(optional_string(&cfg, "tag"), Some("v1".to_string()));
    }

    #[test]
    fn optional_string_missing() {
        let cfg = json!({});
        assert_eq!(optional_string(&cfg, "tag"), None);
    }

    #[test]
    fn optional_string_null() {
        let cfg = json!({"tag": null});
        assert_eq!(optional_string(&cfg, "tag"), None);
    }

    // ── required_number ──

    #[test]
    fn required_number_int() {
        let cfg = json!({"count": 42});
        assert_eq!(required_number(&cfg, "count").unwrap(), 42.0);
    }

    #[test]
    fn required_number_float() {
        let cfg = json!({"ratio": 3.14});
        assert_eq!(required_number(&cfg, "ratio").unwrap(), 3.14);
    }

    #[test]
    fn required_number_missing() {
        let cfg = json!({});
        assert!(required_number(&cfg, "count").is_err());
    }

    #[test]
    fn required_number_string() {
        let cfg = json!({"count": "42"});
        assert!(required_number(&cfg, "count").is_err());
    }

    // ── optional_number ──

    #[test]
    fn optional_number_present() {
        let cfg = json!({"limit": 100.0});
        assert_eq!(optional_number(&cfg, "limit"), Some(100.0));
    }

    #[test]
    fn optional_number_missing() {
        let cfg = json!({});
        assert_eq!(optional_number(&cfg, "limit"), None);
    }

    // ── required_bool ──

    #[test]
    fn required_bool_true() {
        let cfg = json!({"flag": true});
        assert!(required_bool(&cfg, "flag").unwrap());
    }

    #[test]
    fn required_bool_false() {
        let cfg = json!({"flag": false});
        assert!(!required_bool(&cfg, "flag").unwrap());
    }

    #[test]
    fn required_bool_missing() {
        let cfg = json!({});
        assert!(required_bool(&cfg, "flag").is_err());
    }

    #[test]
    fn required_bool_string() {
        let cfg = json!({"flag": "true"});
        assert!(required_bool(&cfg, "flag").is_err());
    }

    // ── optional_bool ──

    #[test]
    fn optional_bool_present() {
        let cfg = json!({"debug": true});
        assert_eq!(optional_bool(&cfg, "debug"), Some(true));
    }

    #[test]
    fn optional_bool_missing() {
        let cfg = json!({});
        assert_eq!(optional_bool(&cfg, "debug"), None);
    }

    // ── required_array ──

    #[test]
    fn required_array_ok() {
        let cfg = json!({"items": [1, 2, 3]});
        assert_eq!(required_array(&cfg, "items").unwrap().len(), 3);
    }

    #[test]
    fn required_array_missing() {
        let cfg = json!({});
        assert!(required_array(&cfg, "items").is_err());
    }

    #[test]
    fn required_array_not_array() {
        let cfg = json!({"items": "not-an-array"});
        assert!(required_array(&cfg, "items").is_err());
    }

    #[test]
    fn required_array_empty() {
        let cfg = json!({"items": []});
        assert!(required_array(&cfg, "items").unwrap().is_empty());
    }

    // ── optional_array ──

    #[test]
    fn optional_array_present() {
        let cfg = json!({"tags": ["a", "b"]});
        assert_eq!(optional_array(&cfg, "tags").unwrap().len(), 2);
    }

    #[test]
    fn optional_array_missing() {
        let cfg = json!({});
        assert!(optional_array(&cfg, "tags").is_none());
    }

    // ── optional_string_array ──

    #[test]
    fn optional_string_array_ok() {
        let cfg = json!({"cols": ["a", "b"]});
        let v = optional_string_array(&cfg, "cols").unwrap();
        assert_eq!(v, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn optional_string_array_missing_returns_empty() {
        let cfg = json!({});
        let v = optional_string_array(&cfg, "cols").unwrap();
        assert!(v.is_empty());
    }

    #[test]
    fn optional_string_array_mixed_types() {
        let cfg = json!({"cols": ["a", 42]});
        assert!(optional_string_array(&cfg, "cols").is_err());
    }

    // ── required_string_array ──

    #[test]
    fn required_string_array_ok() {
        let cfg = json!({"names": ["x", "y"]});
        let v = required_string_array(&cfg, "names").unwrap();
        assert_eq!(v, vec!["x".to_string(), "y".to_string()]);
    }

    #[test]
    fn required_string_array_missing() {
        let cfg = json!({});
        assert!(required_string_array(&cfg, "names").is_err());
    }

    #[test]
    fn required_string_array_mixed_types() {
        let cfg = json!({"names": ["x", 1]});
        assert!(required_string_array(&cfg, "names").is_err());
    }

    #[test]
    fn required_string_array_empty() {
        let cfg = json!({"names": []});
        let v = required_string_array(&cfg, "names").unwrap();
        assert!(v.is_empty());
    }
}
