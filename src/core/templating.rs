use std::collections::HashMap;
use serde_json::Value as JsonValue;
use anyhow::Result;

pub struct TemplateEngine;

impl TemplateEngine {
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
            JsonValue::String(s) => JsonValue::String(Self::render_string(s, context)),
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

        let input = "SELECT * FROM data WHERE dept = '${context.dept}' AND val > ${context.min_val}";
        let output = TemplateEngine::render_string(input, &ctx);
        assert_eq!(output, "SELECT * FROM data WHERE dept = 'IT' AND val > 100");
    }
}
