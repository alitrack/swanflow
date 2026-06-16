use crate::core::engine::IntelligenceHook;
use async_trait::async_trait;
use serde_json::json;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use uuid::Uuid;

pub struct DefaultIntelligenceHook {
    pub wizard: Arc<crate::nodes::ai::ConfigWizard>,
}

/// Categorize the error type for targeted self-healing.
#[derive(Debug)]
enum ErrorCategory {
    /// Column not found / missing column reference
    ColumnNotFound,
    /// SQL syntax error
    SqlSyntax,
    /// Data source / connection error
    DataSource,
    /// General / unclassified error
    Other,
}

fn classify_error(error: &str) -> ErrorCategory {
    let lower = error.to_ascii_lowercase();

    // Column-related errors
    if (lower.contains("column")
        && (lower.contains("not found")
            || lower.contains("not exist")
            || lower.contains("doesn't exist")
            || lower.contains("cannot be found")
            || lower.contains("missing")
            || lower.contains("unknown column")))
        || lower.contains("no column")
    {
        return ErrorCategory::ColumnNotFound;
    }

    // SQL syntax errors
    if (lower.contains("syntax") && (lower.contains("error") || lower.contains("exception")))
        || lower.contains("parse error")
        || lower.contains("unexpected token")
        || lower.contains("sql error")
        || lower.contains("failed to parse sql")
    {
        return ErrorCategory::SqlSyntax;
    }

    // Data source / connection errors
    if (lower.contains("connection")
        && (lower.contains("refused")
            || lower.contains("failed")
            || lower.contains("timeout")
            || lower.contains("reset")))
        || lower.contains("data source")
        || lower.contains("datasource")
        || lower.contains("could not connect")
        || lower.contains("authentication failed")
        || lower.contains("permission denied")
        || lower.contains("file not found")
    {
        return ErrorCategory::DataSource;
    }

    ErrorCategory::Other
}

/// Extract a referenced name (e.g., column name, table name) from an error message
/// by looking for patterns like "'xxx'" or quoting.
fn extract_referenced_name(error: &str) -> Option<String> {
    // Try patterns: 'xxx' (single quotes), "xxx" (double quotes), `xxx` (backticks)
    for quote_char in &['\'', '"', '`'] {
        if let Some(start) = error.find(*quote_char) {
            let after = &error[start + 1..];
            if let Some(end) = after.find(*quote_char) {
                let name = after[..end].to_string();
                if !name.is_empty() && name.len() < 100 {
                    return Some(name);
                }
            }
        }
    }
    None
}

#[async_trait]
impl IntelligenceHook for DefaultIntelligenceHook {
    async fn on_error(
        &self,
        node_id: Uuid,
        type_id: &str,
        _config: &JsonValue,
        error: &str,
    ) -> Option<JsonValue> {
        println!(
            "[Intelligence] Analyzing error for node {} (type: {}): {}",
            node_id, type_id, error
        );

        let category = classify_error(error);

        match category {
            ErrorCategory::ColumnNotFound => {
                let col_name = extract_referenced_name(error);
                println!(
                    "[Intelligence] Column '{}' not found for node {} — suggesting column fix",
                    col_name.as_deref().unwrap_or("<unknown>"),
                    node_id
                );

                Some(json!({
                    "remediation": "column_not_found",
                    "suggestion": format!(
                        "The column '{}' does not exist. Available columns can be checked \
                         via the upstream node's output schema. Consider using SELECT * or \
                         listing valid column names from the upstream data source.",
                        col_name.unwrap_or_else(|| "<unknown>".to_string())
                    ),
                    "retry": false,
                    "confidence": "medium"
                }))
            }
            ErrorCategory::SqlSyntax => {
                println!(
                    "[Intelligence] SQL syntax error on node {} — suggesting SQL fix",
                    node_id
                );
                Some(json!({
                    "remediation": "sql_syntax",
                    "suggestion": "SQL syntax error detected. Rewrite the SQL query with proper syntax. \
                                   Common issues: missing commas, unmatched parentheses, incorrect JOIN \
                                   syntax, wrong keyword ordering, or invalid function names.",
                    "retry": false,
                    "confidence": "medium"
                }))
            }
            ErrorCategory::DataSource => {
                println!(
                    "[Intelligence] Data source error on node {} — suggesting connection fix",
                    node_id
                );
                Some(json!({
                    "remediation": "datasource_error",
                    "suggestion": "Data source connection error. Verify: \
                                   1) The connection URL/configuration is correct \
                                   2) The target database or file is accessible \
                                   3) Authentication credentials are valid \
                                   4) Network connectivity is available",
                    "retry": false,
                    "confidence": "medium"
                }))
            }
            ErrorCategory::Other => {
                println!(
                    "[Intelligence] Unrecognized error for node {} — no automatic fix available",
                    node_id
                );
                None
            }
        }
    }

    async fn on_data_quality(
        &self,
        _node_id: Uuid,
        _sample: &datafusion::arrow::record_batch::RecordBatch,
    ) -> Option<JsonValue> {
        // Quality checks could trigger semantic cleaning
        None
    }
}
