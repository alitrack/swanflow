# Spec: Connector Layer

## Purpose

将 SwanFlow 从"仅有 DuckDB + SQLx 扫描器"升级为"50+ 数据库通用连接器层"，复用 DBX 的连接配置生态。

## Contract

- `Connector` trait 定义清晰的连接生命周期
- `ConnConfig` 格式与 DBX 配置格式兼容
- 每个连接器必须通过 T1（单连接）和 T2（连表查询）测试
- Late-Binding 引用在所有连接器上统一工作

## Trait Definition

```rust
#[async_trait]
pub trait Connector: Send + Sync {
    /// 根据配置建立连接
    async fn connect(config: ConnConfig) -> Result<Self>;
    /// 列出可用表
    async fn list_tables(&self) -> Result<Vec<String>>;
    /// 获取表 schema
    async fn get_schema(&self, table: &str) -> Result<SchemaRef>;
    /// 扫描表数据，返回 Arrow RecordBatch 流
    async fn scan(&self, table: &str, projection: &[usize])
        -> Result<SendableRecordBatchStream>;
}
```

## Phase 1 Priority

| 数据库 | 驱动方式 | 优先级 | 理由 |
|--------|----------|--------|------|
| PostgreSQL | SQLx | P0 | 已有、最常用 |
| MySQL | SQLx | P0 | 已有、最常用 |
| ClickHouse | HTTP | P0 | 性能分析场景核心 |
| SQL Server | TDS | P1 | 企业用户 |
| MongoDB | Arrow/原生 | P1 | 文档数据库 |
| SQLite | DuckDB Scanner | P0 | 已有 |
| Oracle | JDBC bridge | P2 | 需额外依赖 |
| TiDB | MySQL 兼容 | P1 | 中国生态 |
| OceanBase | MySQL 兼容 | P1 | 中国生态 |
