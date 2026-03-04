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
