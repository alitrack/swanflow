# Spec: OpenFang Workflow Patterns

## Purpose

从 OpenFang workflow engine 中提取 5 种 step mode 和错误处理模式，适配到 SwanFlow 现有的 YAML DSL 和执行引擎中。**不做直接代码 fork，只吸收设计模式**。

## Contract

- YAML schema 扩展支持 5 种 `mode`: `sequential` / `fan_out` / `collect` / `conditional` / `loop`
- YAML schema 扩展支持 `error_mode`: `fail` / `skip` / `retry`
- YAML schema 扩展支持 `when` 条件守卫
- 所有新模式向后兼容（不设 mode 时默认 sequential）

## YAML Schema 扩展

```yaml
tasks:
  - id: step1
    kind: sql
    mode: fan_out          # ← 并发执行
    params: { query: "SELECT * FROM src" }

  - id: step2
    kind: filter
    mode: fan_out          # ← 与 step1 同时执行
    params: { condition: "amount > 1000" }

  - id: step3
    mode: collect          # ← 收集 fan-out 输出

  - id: step4
    kind: sql
    mode: conditional      # ← 条件执行
    when: ${{ tasks.step3.output contains 'outlier' }}

  - id: step5
    kind: review
    mode: loop             # ← 循环直到条件
    until: "APPROVED"
    max_iterations: 5
    error_mode: retry
    max_retries: 3
```

## Execution Model

```
fan_out_group = consecutive tasks with mode: fan_out
                 ↓ tokio::join_all()
            all run in parallel
                 ↓
collect task = merge all outputs with "\n---\n" separator
                 ↓
conditional task = skip if when condition not met
                 ↓
loop task = repeat until condition, max_iterations cap
                 ↓
error_mode = fail (abort) | skip (continue) | retry (N attempts)
```

## Trigger Engine (Light)

```yaml
triggers:
  - event: file.created
    pattern: "data/*.csv"
    workflow: ingest-csv
  - event: cron
    schedule: "0 6 * * *"
    workflow: daily-report
```
