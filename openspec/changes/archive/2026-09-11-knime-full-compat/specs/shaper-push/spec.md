# Spec: Shaper Push

## Purpose

将 KNIME 可视化节点和执行结果推送到 Shaper 仪表板，通过 DuckDB 直接写入 Shaper 的 SQLite 数据库（`apps` 表）。

## Contract

### 输入

- `Vec<DashboardDef>` — 每个包含 name, sql_query, chart_type
- `shaper_db_path: PathBuf` — 默认 `~/.shaper/shaper_internal.sqlite`
- `overwrite: bool` — 是否覆盖同名仪表板

### 输出

```rust
Result<Vec<String>, ShaperPushError>
```

返回已创建的仪表板 UUID 列表。

### DashboardDef

```rust
pub struct DashboardDef {
    pub name: String,
    pub sql: String,          // Shaper chart SQL (SELECT col::CHARTTYPE)
    pub chart_type: ChartType,
    pub knime_node_id: i64,   // 用于错误追踪
}

pub enum ChartType {
    Bar, Line, Pie, Scatter, Bubble,
    Heatmap, Sunburst, Parallel, Table, EChart,
}
```

### Shaper SQL 生成规则

| KNIME Viz Node | Shaper SQL 模式 |
|---|---|
| Bar Chart | `SELECT {x}::XAXIS, {y}::BARCHART FROM "{table}"` |
| Pie Chart | `SELECT {cat}::XAXIS, {val}::PIECHART FROM "{table}"` |
| Line Chart | `SELECT {x}::XAXIS, {y}::LINECHART FROM "{table}" ORDER BY {x}` |
| Scatter Plot | `SELECT {x}::XAXIS, {y}::SCATTERPLOT FROM "{table}"` |
| Bubble Chart | `SELECT {x}::XAXIS, {y}::BUBBLECHART, {size}::SIZE FROM "{table}"` |
| Heatmap | `SELECT {rows}::XAXIS, {cols}::XAXIS, {val}::HEATMAP FROM "{table}"` |
| Table View | `SELECT *::TABLE FROM "{table}"` |
| EChart (通用) | `SELECT *::ECHART FROM "{table}"` |

### 写入实现

```rust
pub fn push_dashboards(
    db_path: &Path,
    dashboards: &[DashboardDef],
    overwrite: bool,
) -> Result<Vec<String>> {
    // 1. 使用 DuckDB 打开内存数据库
    let conn = Connection::open_in_memory()?;
    
    // 2. 安装并加载 SQLite 扩展
    conn.execute_batch("INSTALL sqlite; LOAD sqlite;")?;
    
    // 3. ATTACH Shaper 数据库
    conn.execute_batch(&format!(
        "ATTACH '{}' AS shaper (TYPE SQLITE);",
        db_path.display()
    ))?;
    
    // 4. 确保 apps 表存在（Shaper 已创建，这里做防御性检查）
    let table_exists: bool = conn.query_row(
        "SELECT COUNT(*) > 0 FROM sqlite_master WHERE name = 'apps'",
        [],
        |row| row.get(0),
    )?;
    if !table_exists {
        return Err(anyhow!("Shaper apps table not found at {}", db_path.display()));
    }
    
    // 5. 检查/删除同名仪表板
    if overwrite {
        for dash in dashboards {
            conn.execute(
                "DELETE FROM shaper.apps WHERE name = ?",
                params![dash.name],
            )?;
        }
    }
    
    // 6. 插入仪表板定义
    let mut ids = Vec::new();
    for dash in dashboards {
        let id = Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO shaper.apps (id, name, content, type) VALUES (?, ?, ?, 'dashboard')",
            params![id, dash.name, dash.sql],
        )?;
        ids.push(id);
    }
    
    Ok(ids)
}
```

## Details

### Shaper 数据库路径发现

```rust
fn resolve_shaper_path(explicit_path: Option<&Path>) -> PathBuf {
    explicit_path.map(|p| p.to_path_buf()).unwrap_or_else(|| {
        let home = dirs::home_dir().expect("Cannot find home directory");
        home.join(".shaper").join("shaper_internal.sqlite")
    })
}
```

### 图表类型回退

如果 KNIME 图表类型无法映射到 Shaper 支持的 10 种类型：

```
KNIME Type          → Shaper Fallback
─────────────────────────────────────
Box Plot            → BARCHART (with percentiles)
Density Plot        → LINECHART (with bin aggregation)
Histogram           → BARCHART (with bin count)
Parallel Coords     → TABLE (too complex for BARCHART)
Sunburst            → TABLE (hierarchical data in tabular form)
```

### 数据表引用

Shaper SQL 中的表名引用 SwanFlow 物化表：

```
SELECT region::XAXIS, SUM(sales)::BARCHART FROM node_abc123_output
```

Shaper 需要能访问这个表。如果 Shaper 是独立的 DuckDB 实例：
- **选项 A**: 将数据写入 Shaper 的 DuckDB 数据库
- **选项 B**: 将数据物化为 Parquet，Shaper SQL 引用 Parquet 路径
- **默认**: 推送到 Shaper 时，数据也物化到 Shaper SQLite 的同目录下

## Boundary Cases

- **Shaper 数据库不存在**: 报错 `ShaperDBNotFound`，提示用户先启动 Shaper
- **apps 表 schema 不匹配**: DuckDB ATTACH 时检查 schema，不支持则报错
- **同名仪表板 + overwrite=false**: 跳过该仪表板，返回 Warning
- **SQL 中包含特殊字符**: 使用参数化查询（`?` 占位），避免 SQL 注入
- **Shaper 正在运行且锁定 SQLite**: DuckDB ATTACH 支持 WAL 模式下的并发读，写入可能失败 → 重试 3 次
- **空仪表板列表**: 直接返回 `Ok(vec![])`，不操作数据库
