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
    /// Also resolves {{mustache}} style placeholders (same semantics as render_json).
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

        // Also resolve {{mustache}} style placeholders
        Self::resolve_mustache_placeholder(&output, context)
    }

    /// Recursively renders a JSON value (e.g., node configuration).
    pub fn render_json(input: &JsonValue, context: &HashMap<String, JsonValue>) -> JsonValue {
        match input {
            JsonValue::String(s) => {
                // First try exact ${context.key} placeholder
                if let Some(value) = Self::resolve_exact_context_placeholder(s, context) {
                    return value;
                }
                // Resolve ${context.key} and {{mustache}} placeholders in one pass
                let rendered = Self::render_string(s, context);
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

    /// Extract the set of variable names referenced by a node config via
    /// `{{var}}` or `${context.var}` placeholders (anywhere in the JSON tree).
    ///
    /// Used by the topological re-ordering pass (variable-management 3.1) to
    /// detect *implicit* dependencies: a node whose config consumes a variable
    /// produced by another node's `output_var` must run after the producer even
    /// when no explicit data edge connects the two.
    pub fn extract_variable_refs(value: &JsonValue, out: &mut std::collections::HashSet<String>) {
        match value {
            JsonValue::String(s) => {
                // Scan for {{key}} and ${context.key} placeholders.
                let bytes = s.as_bytes();
                let mut i = 0;
                while i < bytes.len() {
                    if bytes[i] == b'{' && i + 1 < bytes.len() && bytes[i + 1] == b'{' {
                        // {{key}} — key ends at first '}'
                        if let Some(end) = s[i + 2..].find('}') {
                            let key = &s[i + 2..i + 2 + end];
                            let key = key.trim();
                            // Placeholder keys are identifier-like; skip empty/multi-token.
                            if !key.is_empty()
                                && !key.contains('}')
                                && !key.contains(' ')
                                && key.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.')
                            {
                                // {{a.b}} style: take the full dotted path as the key
                                // (mustache resolution in this engine is literal on the key).
                                out.insert(key.to_string());
                            }
                            i += 2 + end + 1;
                            continue;
                        }
                    }
                    if s[i..].starts_with("${context.") {
                        let rest = &s[i + "${context.".len()..];
                        if let Some(end) = rest.find('}') {
                            let key = &rest[..end];
                            if !key.is_empty()
                                && key.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.')
                            {
                                out.insert(key.to_string());
                            }
                            i += "${context.".len() + end + 1;
                            continue;
                        }
                    }
                    i += 1;
                }
            }
            JsonValue::Array(arr) => {
                for v in arr {
                    Self::extract_variable_refs(v, out);
                }
            }
            JsonValue::Object(map) => {
                for v in map.values() {
                    Self::extract_variable_refs(v, out);
                }
            }
            _ => {}
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

    #[test]
    fn test_extract_variable_refs_mustache_and_context() {
        let mut refs = std::collections::HashSet::new();
        let input = json!({
            "sql": "SELECT * FROM t WHERE dept = '${context.dept}' AND tag = '{{tag}}'",
            "nested": ["{{other}}", "${context.limit}", "no placeholder here"]
        });
        TemplateEngine::extract_variable_refs(&input, &mut refs);
        assert!(refs.contains("dept"), "context-style ref must be found: {refs:?}");
        assert!(refs.contains("tag"), "mustache ref must be found: {refs:?}");
        assert!(refs.contains("other"));
        assert!(refs.contains("limit"));
        assert_eq!(refs.len(), 4, "exactly 4 refs: {refs:?}");
    }

    #[test]
    fn test_extract_variable_refs_ignores_junk() {
        let mut refs = std::collections::HashSet::new();
        let input = json!({
            "a": "{{ }",            // unclosed
            "b": "100% done",       // not a placeholder
            "c": 42,                // non-string
            "d": null
        });
        TemplateEngine::extract_variable_refs(&input, &mut refs);
        assert!(refs.is_empty(), "junk should not be collected: {refs:?}");
    }
}
