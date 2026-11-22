//! P4 acceptance: versioned nodes and config migration.
//!
//! The chain itself lives in `WorkflowCompiler::run_preflight` (parse versions →
//! refuse anything newer than the runtime → apply migrations one step at a time
//! → stamp the target versions → validate). It was written before this module
//! existed and had never actually run, because no node declared a version above
//! 1 — so it was code that looked right rather than behaviour known to hold.
//!
//! The spec's acceptance sentence, verbatim: "构造一个老版本工作流，加载后自动迁移
//! 且结果与新版一致；不迁移则必须报错而不是静默算。" That is three properties, and
//! the three tests below are one each.

use super::WorkflowCompiler;
use crate::core::graph::Workflow;
use crate::nodes::{NodeBehavior, NodeCapability, NodeErrorClass, NodeSpec, PortSpec};
use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use serde_json::{json, Value as JsonValue};
use uuid::Uuid;

fn spec(type_id: &str) -> NodeSpec {
    NodeSpec {
        type_id: type_id.to_string(),
        display_name: "Versioned fixture".to_string(),
        description: "test fixture; not registered anywhere".to_string(),
        input_ports: vec![PortSpec {
            id: 0,
            name: "in".to_string(),
            description: "input".to_string(),
        }],
        output_ports: vec![PortSpec {
            id: 0,
            name: "out".to_string(),
            description: "output".to_string(),
        }],
        config_schema: json!({ "type": "object" }),
    }
}

/// v2 renamed the config key `source_file` → `path`. A v1 workflow still says
/// `source_file`, so this is a migration that has to do real work.
struct RenamedKeyNode {
    version: u32,
    /// When false the node bumps its version but implements no migration, which
    /// is exactly the case the spec says must fail loudly.
    implements_migration: bool,
}

#[async_trait]
impl NodeBehavior for RenamedKeyNode {
    fn spec(&self) -> NodeSpec {
        spec("test.renamed_key")
    }

    fn validate(&self, config: &JsonValue) -> anyhow::Result<()> {
        let obj = config
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("config must be an object"))?;
        let key = if self.version >= 2 {
            "path"
        } else {
            "source_file"
        };
        if !obj.contains_key(key) {
            anyhow::bail!("missing '{key}' for v{}", self.version);
        }
        Ok(())
    }

    fn config_version(&self) -> u32 {
        self.version
    }

    fn migrate_config(
        &self,
        config: JsonValue,
        from_version: u32,
        to_version: u32,
    ) -> anyhow::Result<JsonValue> {
        if !self.implements_migration {
            // No override in spirit: hand it to the default, which must refuse.
            return crate::nodes::NodeBehavior::migrate_config(
                &UnmigratedNode {
                    version: self.version,
                },
                config,
                from_version,
                to_version,
            );
        }
        if (from_version, to_version) != (1, 2) {
            anyhow::bail!("unexpected step {from_version}->{to_version}");
        }
        let mut obj = config
            .as_object()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("config must be an object"))?;
        if let Some(v) = obj.remove("source_file") {
            obj.insert("path".to_string(), v);
        }
        Ok(JsonValue::Object(obj))
    }

    fn capability(&self) -> NodeCapability {
        NodeCapability::ReadOnly
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        _config: &JsonValue,
    ) -> anyhow::Result<Vec<LogicalPlan>> {
        anyhow::bail!("planning is not part of the migration contract")
    }
}

/// Bumps its version and inherits the default `migrate_config` unchanged.
struct UnmigratedNode {
    version: u32,
}

#[async_trait]
impl NodeBehavior for UnmigratedNode {
    fn spec(&self) -> NodeSpec {
        spec("test.unmigrated")
    }

    fn validate(&self, config: &JsonValue) -> anyhow::Result<()> {
        if !config.is_object() {
            anyhow::bail!("config must be an object");
        }
        // Deliberately permissive: the old shape *does* validate here. That is
        // the whole danger — without a migration error, this config would sail
        // through and compute something plausible and wrong.
        Ok(())
    }

    fn config_version(&self) -> u32 {
        self.version
    }

    fn capability(&self) -> NodeCapability {
        NodeCapability::ReadOnly
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        _config: &JsonValue,
    ) -> anyhow::Result<Vec<LogicalPlan>> {
        anyhow::bail!("planning is not part of the migration contract")
    }
}

fn run(
    behavior: &dyn NodeBehavior,
    node_type: &str,
    config: JsonValue,
) -> Result<JsonValue, crate::nodes::NodeDiagnostic> {
    let mut compiler = WorkflowCompiler::new(false);
    let workflow = Workflow::new("migration-test");
    compiler.run_preflight(&workflow, Uuid::new_v4(), node_type, behavior, &config)
}

/// Property 1: an old workflow migrates, and the result is what the new version
/// would have been given directly.
#[test]
fn an_old_config_migrates_to_the_new_shape() {
    let node = RenamedKeyNode {
        version: 2,
        implements_migration: true,
    };
    let out = run(
        &node,
        "test.renamed_key",
        json!({ "source_file": "orders.csv", "config_version": 1 }),
    )
    .expect("a v1 config must migrate to v2");

    assert_eq!(out["path"], json!("orders.csv"), "key must be renamed");
    assert!(
        out.get("source_file").is_none(),
        "the old key must not survive the migration: {out}"
    );
    assert_eq!(out["config_version"], json!(2), "version must be stamped");
    assert_eq!(
        out["impl_version"],
        json!(1),
        "impl version must be stamped"
    );
}

/// Property 1b: "结果与新版一致" — the migrated config must equal the config a
/// v2 workflow would carry, which is the only way "same result" can be checked
/// without running a plan.
#[test]
fn a_migrated_config_equals_the_native_v2_config() {
    let node = RenamedKeyNode {
        version: 2,
        implements_migration: true,
    };
    let migrated = run(
        &node,
        "test.renamed_key",
        json!({ "source_file": "orders.csv", "config_version": 1 }),
    )
    .expect("migration");

    let native = run(
        &node,
        "test.renamed_key",
        json!({ "path": "orders.csv", "config_version": 2 }),
    )
    .expect("native v2");

    assert_eq!(migrated, native, "migrated config must match native v2");
}

/// Property 2: bumping the version without a migration must fail, not silently
/// hand the old config onward. The fixture's `validate` accepts anything, so
/// only the migration step can catch this.
#[test]
fn a_version_bump_without_a_migration_is_refused() {
    let node = UnmigratedNode { version: 2 };
    let err = run(
        &node,
        "test.unmigrated",
        json!({ "source_file": "orders.csv", "config_version": 1 }),
    )
    .expect_err("a bump with no migration must not pass silently");

    assert_eq!(err.class, NodeErrorClass::Migration);
    assert!(
        err.message.contains("implements no migration 1->2"),
        "the error must name the missing step, got: {}",
        err.message
    );
    assert_eq!(err.failed_step.as_deref(), Some("1->2"));
}

/// Property 3: a config from the future is refused rather than half-applied.
#[test]
fn a_config_newer_than_the_node_is_refused() {
    let node = RenamedKeyNode {
        version: 2,
        implements_migration: true,
    };
    let err = run(
        &node,
        "test.renamed_key",
        json!({ "path": "orders.csv", "config_version": 3 }),
    )
    .expect_err("config_version=3 on a v2 node must be refused");

    assert_eq!(err.class, NodeErrorClass::Migration);
    assert!(
        err.message.contains("newer than supported target"),
        "got: {}",
        err.message
    );
}

/// A node that legitimately bumps its version without changing the config shape
/// may say so explicitly; that must still work.
#[test]
fn an_explicit_identity_migration_is_allowed() {
    struct IdentityNode;

    #[async_trait]
    impl NodeBehavior for IdentityNode {
        fn spec(&self) -> NodeSpec {
            spec("test.identity")
        }

        fn validate(&self, _config: &JsonValue) -> anyhow::Result<()> {
            Ok(())
        }

        fn config_version(&self) -> u32 {
            2
        }

        fn migrate_config(
            &self,
            config: JsonValue,
            from_version: u32,
            to_version: u32,
        ) -> anyhow::Result<JsonValue> {
            // The shape did not change in 1->2; the bump is semantic only.
            assert_eq!((from_version, to_version), (1, 2));
            Ok(config)
        }

        fn capability(&self) -> NodeCapability {
            NodeCapability::ReadOnly
        }

        async fn propose_plan(
            &self,
            _inputs: Vec<LogicalPlan>,
            _config: &JsonValue,
        ) -> anyhow::Result<Vec<LogicalPlan>> {
            anyhow::bail!("planning is not part of the migration contract")
        }
    }

    let out = run(
        &IdentityNode,
        "test.identity",
        json!({ "anything": 1, "config_version": 1 }),
    )
    .expect("an explicit identity migration is legitimate");
    assert_eq!(out["anything"], json!(1));
    assert_eq!(out["config_version"], json!(2));
}
