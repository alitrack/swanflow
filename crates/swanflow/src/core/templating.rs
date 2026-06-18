use serde_json::Value as JsonValue;
use std::collections::HashMap;

pub struct TemplateEngine;

impl TemplateEngine {
    fn resolve_exact_context_placeholder(
        input: &str,
        context: &HashMap<String, JsonValue>,
    ) -> Option<JsonValue> {
        const PREFIX: &str = "${context.";
        if !(input.starts_with(PREFIX) && input.ends_with('}')) {
            return None;
        }

        let key = &input[PREFIX.len()..input.len() - 1];
        context.get(key).cloned()
    }

    fn resolve_mustache_placeholder(input: &str, context: &HashMap<String, JsonValue>) -> String {
        // Replace {{input}} and {{var_name}} with context values
        let mut output = input.to_string();
        for (key, value) in context {
            let placeholder = format!("{{{{{}}}}}", key);
            let replacement = match value {
                JsonValue::String(s) => s.clone(),
                JsonValue::Number(n) => n.to_string(),
                JsonValue::Bool(b) => b.to_string(),
                _ => value.to_string(),
            };
            output = output.replace(&placeholder, &replacement);
        }
        output
    }

    /// Replaces placeholders like ${context.key} in a string with values from the context.
    pub fn render_string(input: &str, context: &HashMap<String, JsonValue>) -> String {
        let mut output = input.to_string();

        for (key, value) in context {
            let placeholder = format!("${{context.{}}}", key);
            let replacement = match value {
                JsonValue::String(s) => s.clone(),
                JsonValue::Number(n) => n.to_string(),
                JsonValue::Bool(b) => b.to_string(),
                _ => value.to_string(),
            };
            output = output.replace(&placeholder, &replacement);
        }

        output
    }

    /// Recursively renders a JSON value (e.g., node configuration).
    pub fn render_json(input: &JsonValue, context: &HashMap<String, JsonValue>) -> JsonValue {
        match input {
            JsonValue::String(s) => {
                // First try exact ${context.key} placeholder
                if let Some(value) = Self::resolve_exact_context_placeholder(s, context) {
                    return value;
                }
                // Also resolve {{mustache}} style placeholders
                let rendered = Self::resolve_mustache_placeholder(s, context);
                // Then resolve ${context.key} style within the string
                let rendered = Self::render_string(&rendered, context);
                JsonValue::String(rendered)
            }
            JsonValue::Array(arr) => {
                JsonValue::Array(arr.iter().map(|v| Self::render_json(v, context)).collect())
            }
            JsonValue::Object(map) => {
                let mut new_map = serde_json::Map::new();
                for (k, v) in map {
                    new_map.insert(k.clone(), Self::render_json(v, context));
                }
                JsonValue::Object(new_map)
            }
            _ => input.clone(),
        }
    }

    /// Resolve a single value against context, supporting {{input}} and {{var_name}} placeholders.
    /// Returns None if the value is a string that is purely a placeholder reference,
    /// and the actual context value is the desired output.
    pub fn resolve_value(value: &JsonValue, context: &HashMap<String, JsonValue>) -> JsonValue {
        match value {
            JsonValue::String(s) => {
                let trimmed = s.trim();
                // Check if the entire string is a single {{placeholder}}
                if trimmed.starts_with("{{") && trimmed.ends_with("}}") {
                    let key = trimmed[2..trimmed.len() - 2].trim();
                    if let Some(context_val) = context.get(key) {
                        return context_val.clone();
                    }
                }
                // Otherwise render any placeholders within the string
                Self::render_json(value, context)
            }
            _ => Self::render_json(value, context),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_render_string() {
        let mut ctx = HashMap::new();
        ctx.insert("dept".to_string(), json!("IT"));
        ctx.insert("min_val".to_string(), json!(100));

        let input =
            "SELECT * FROM data WHERE dept = '${context.dept}' AND val > ${context.min_val}";
        let output = TemplateEngine::render_string(input, &ctx);
        assert_eq!(output, "SELECT * FROM data WHERE dept = 'IT' AND val > 100");
    }

    #[test]
    fn test_render_string_multiple_occurrences() {
        let mut ctx = HashMap::new();
        ctx.insert("table".to_string(), json!("users"));

        let input = "SELECT * FROM ${context.table} JOIN ${context.table}_roles ON ...";
        let output = TemplateEngine::render_string(input, &ctx);
        assert_eq!(output, "SELECT * FROM users JOIN users_roles ON ...");
    }

    #[test]
    fn test_render_string_missing_key() {
        let ctx = HashMap::new();
        let input = "SELECT * FROM ${context.missing}";
        let output = TemplateEngine::render_string(input, &ctx);
        // Missing keys should not be replaced
        assert_eq!(output, "SELECT * FROM ${context.missing}");
    }

    #[test]
    fn test_render_json_object() {
        let mut ctx = HashMap::new();
        ctx.insert("table".to_string(), json!("sales"));
        ctx.insert("limit".to_string(), json!(1000));

        let input = json!({
            "table": "${context.table}",
            "limit": "${context.limit}"
        });

        let output = TemplateEngine::render_json(&input, &ctx);
        assert_eq!(output["table"], "sales");
        assert_eq!(output["limit"], 1000);
    }

    #[test]
    fn test_render_json_array() {
        let mut ctx = HashMap::new();
        ctx.insert("col".to_string(), json!("price"));

        let input = json!(["${context.col}", "quantity", "total"]);
        let output = TemplateEngine::render_json(&input, &ctx);
        assert_eq!(output[0], "price");
        assert_eq!(output[1], "quantity");
    }

    #[test]
    fn test_render_json_preserves_non_string() {
        let ctx = HashMap::new();

        let input = json!({
            "count": 42,
            "active": true,
            "ratio": std::f64::consts::PI
        });

        let output = TemplateEngine::render_json(&input, &ctx);
        assert_eq!(output["count"], 42);
        assert_eq!(output["active"], true);
        assert!(output["ratio"].is_number());
    }

    #[test]
    fn test_mustache_placeholder_input() {
        let mut ctx = HashMap::new();
        ctx.insert("input".to_string(), json!("hello world"));

        let input = "{{input}}";
        let output = TemplateEngine::render_string(input, &ctx);
        assert_eq!(output, "hello world");
    }

    #[test]
    fn test_mustache_placeholder_custom_var() {
        let mut ctx = HashMap::new();
        ctx.insert("my_var".to_string(), json!("42"));

        let input = "{{my_var}}";
        let output = TemplateEngine::render_string(input, &ctx);
        assert_eq!(output, "42");
    }

    #[test]
    fn test_resolve_value_pure_placeholder() {
        let mut ctx = HashMap::new();
        ctx.insert("input".to_string(), json!({"key": "value"}));

        let input = json!("{{input}}");
        let resolved = TemplateEngine::resolve_value(&input, &ctx);
        // A pure placeholder should return the context value directly (preserving type)
        assert_eq!(resolved, json!({"key": "value"}));
    }

    #[test]
    fn test_resolve_value_mixed_string() {
        let mut ctx = HashMap::new();
        ctx.insert("name".to_string(), json!("Alice"));

        let input = json!("Hello, {{name}}!");
        let resolved = TemplateEngine::resolve_value(&input, &ctx);
        assert_eq!(resolved, json!("Hello, Alice!"));
    }
}
