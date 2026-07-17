//! Flow variable resolution with default values.
//!
//! KNIME-style flow variables use `%%var_name%%` syntax in node configurations.
//! SwanFlow extends this with:
//! - `{{var_name}}` — simple variable reference
//! - `{{var_name:default_value}}` — variable with fallback default
//! - `%%var_name:default_value%%` — KNIME-compatible with default

use serde_json::Value as JsonValue;

/// Resolve template variables in a JSON config value in-place.
///
/// Supports:
/// - `{{var_name}}` → context["var_name"]
/// - `{{var_name:default}}` → context["var_name"] or "default" if missing
/// - `%%var_name%%` → same as `{{var_name}}`
/// - `%%var_name:default%%` → same as `{{var_name:default}}`
///
/// If a variable is missing and has no default, the template is left as-is
/// (to allow downstream handling).
pub fn resolve_template_vars(
    config: &mut JsonValue,
    context: &std::collections::HashMap<String, JsonValue>,
) {
    match config {
        JsonValue::String(s) => {
            *s = resolve_string(s, context);
        }
        JsonValue::Object(obj) => {
            for val in obj.values_mut() {
                resolve_template_vars(val, context);
            }
        }
        JsonValue::Array(arr) => {
            for val in arr.iter_mut() {
                resolve_template_vars(val, context);
            }
        }
        _ => {}
    }
}

fn resolve_string(s: &str, context: &std::collections::HashMap<String, JsonValue>) -> String {
    // Try %%var%% syntax first, then {{var}}
    let mut result = s.to_string();

    // Resolve %%var_name%% or %%var_name:default%%
    loop {
        let start = result.find("%%");
        if start.is_none() {
            break;
        }
        let start = start.unwrap();
        let end = result[start + 2..].find("%%");
        if end.is_none() {
            break;
        }
        let end = start + 2 + end.unwrap();
        let inner = &result[start + 2..end];

        let replacement = resolve_variable(inner, context);
        let mut new_result = String::with_capacity(result.len());
        new_result.push_str(&result[..start]);
        new_result.push_str(&replacement);
        new_result.push_str(&result[end + 2..]);
        result = new_result;
    }

    // Resolve {{var_name}} or {{var_name:default}}
    loop {
        let start = result.find("{{");
        if start.is_none() {
            break;
        }
        let start = start.unwrap();
        let end = result[start + 2..].find("}}");
        if end.is_none() {
            break;
        }
        let end = start + 2 + end.unwrap();
        let inner = &result[start + 2..end];

        let replacement = resolve_variable(inner, context);
        let mut new_result = String::with_capacity(result.len());
        new_result.push_str(&result[..start]);
        new_result.push_str(&replacement);
        new_result.push_str(&result[end + 2..]);
        result = new_result;
    }

    result
}

/// Resolve a single variable reference like `"var_name"` or `"var_name:default_value"`.
fn resolve_variable(
    inner: &str,
    context: &std::collections::HashMap<String, JsonValue>,
) -> String {
    if let Some(colon_pos) = inner.find(':') {
        let var_name = &inner[..colon_pos];
        let default_val = &inner[colon_pos + 1..];
        match context.get(var_name) {
            Some(val) => val_as_string(val),
            None => default_val.to_string(),
        }
    } else {
        match context.get(inner) {
            Some(val) => val_as_string(val),
            None => {
                // Variable not found — leave the template unresolved
                format!("{{{{{}}}}}", inner)
            }
        }
    }
}

fn val_as_string(val: &JsonValue) -> String {
    match val {
        JsonValue::String(s) => s.clone(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Null => String::new(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    fn ctx() -> HashMap<String, JsonValue> {
        let mut m = HashMap::new();
        m.insert("threshold".to_string(), json!(0.5));
        m.insert("name".to_string(), json!("test-wf"));
        m.insert("rows".to_string(), json!(1000));
        m.insert("enabled".to_string(), json!(true));
        m
    }

    #[test]
    fn test_resolve_simple_variable() {
        let mut m = ctx();
        let mut config = json!({ "query": "SELECT * FROM {{name}}" });
        resolve_template_vars(&mut config, &m);
        assert_eq!(config["query"], "SELECT * FROM test-wf");
    }

    #[test]
    fn test_resolve_variable_with_default() {
        let mut m = ctx();
        // missing_var not in context — should use default "10"
        let mut config = json!({ "limit": "{{missing_var:10}}" });
        resolve_template_vars(&mut config, &m);
        assert_eq!(config["limit"], "10");
    }

    #[test]
    fn test_resolve_variable_missing_no_default_keeps_template() {
        let mut m = ctx();
        let mut config = json!({ "filter": "{{undefined_var}}" });
        resolve_template_vars(&mut config, &m);
        // Template left as-is for downstream handling
        assert_eq!(config["filter"], "{{undefined_var}}");
    }

    #[test]
    fn test_resolve_numeric_and_bool_vars() {
        let mut m = ctx();
        let mut config = json!({
            "min_val": "{{threshold}}",
            "max_rows": "{{rows}}",
            "flag": "{{enabled}}"
        });
        resolve_template_vars(&mut config, &m);
        assert_eq!(config["min_val"], "0.5");
        assert_eq!(config["max_rows"], "1000");
        assert_eq!(config["flag"], "true");
    }

    #[test]
    fn test_resolve_knime_style_percent_vars() {
        let mut m = ctx();
        let mut config = json!({ "path": "/data/%%name%%/output.csv" });
        resolve_template_vars(&mut config, &m);
        assert_eq!(config["path"], "/data/test-wf/output.csv");
    }

    #[test]
    fn test_resolve_knime_style_with_default() {
        let mut m = ctx();
        let mut config = json!({ "timeout": "%%timeout_sec:3600%%" });
        resolve_template_vars(&mut config, &m);
        assert_eq!(config["timeout"], "3600");
    }

    #[test]
    fn test_resolve_in_nested_object() {
        let mut m = ctx();
        let mut config = json!({
            "source": {
                "table": "{{name}}",
                "filter": {
                    "col": "threshold > {{threshold}}"
                }
            }
        });
        resolve_template_vars(&mut config, &m);
        assert_eq!(config["source"]["table"], "test-wf");
        assert_eq!(config["source"]["filter"]["col"], "threshold > 0.5");
    }

    #[test]
    fn test_resolve_in_array() {
        let mut m = ctx();
        let mut config = json!({
            "columns": ["{{name}}_a", "{{name}}_b"]
        });
        resolve_template_vars(&mut config, &m);
        assert_eq!(config["columns"][0], "test-wf_a");
        assert_eq!(config["columns"][1], "test-wf_b");
    }

    #[test]
    fn test_resolve_no_templates_unchanged() {
        let mut m = ctx();
        let mut config = json!({ "query": "SELECT 1" });
        resolve_template_vars(&mut config, &m);
        assert_eq!(config["query"], "SELECT 1");
    }

    #[test]
    fn test_resolve_empty_context_keeps_templates() {
        let empty = HashMap::new();
        let mut config = json!({ "val": "{{something}}" });
        resolve_template_vars(&mut config, &empty);
        assert_eq!(config["val"], "{{something}}");
    }
}
