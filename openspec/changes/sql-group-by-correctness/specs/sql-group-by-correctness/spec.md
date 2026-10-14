## ADDED Requirements

### Requirement: GROUP BY partitions by all distinct keys
The `core.sql` node SHALL produce GROUP BY output that partitions rows into one
group per distinct key value, preserving total row count, for both string-typed
and integer-typed grouping keys.

#### Scenario: String (Utf8View) key groups correctly
- **WHEN** a `core.sql` node runs `SELECT <string_col>, COUNT(*) FROM input_0 GROUP BY <string_col>` over 5 rows where the string column has 3 distinct values (2+2+1)
- **THEN** the output SHALL contain exactly 3 rows with counts 2, 2, 1 (total 5)
- **AND** it SHALL NOT collapse to a single NULL or empty-key row.

#### Scenario: Integer key groups correctly (sanity parity)
- **WHEN** the same node runs GROUP BY over an integer column with 3 distinct values (2+2+1)
- **THEN** the output SHALL contain exactly 3 rows with counts 2, 2, 1 (total 5)
- **AND** string-keyed and integer-keyed GROUP BY SHALL behave equivalently (no type-dependent collapse).

### Requirement: Collapse is reproduced before it is fixed
A change claiming to fix GROUP BY collapse SHALL include an end-to-end reproduction
that exercises the real workflow shape (staged materialization and/or fan_out) and
fails (asserts red) on the pre-fix binary, before the fix is applied.

#### Scenario: Reproduction fixture is the fix gate
- **WHEN** the fix commit is prepared
- **THEN** a workflow fixture reproducing the collapse at the minimal triggering shape SHALL exist and SHALL assert red on the pre-fix binary and green on the post-fix binary.
- **AND** if no such fixture can be produced (the historical collapse no longer occurs), the change SHALL close by promoting the unit guardrail to a permanent regression test rather than inventing a fix for a non-reproducible bug.

### Requirement: Guardrail test pins GROUP BY correctness
The unit-level guardrail test SHALL remain a permanent part of the test suite and
SHALL assert that string and integer GROUP BY both yield the expected 3-group
partition with no empty/NULL keys.

#### Scenario: Guardrail stays green
- **WHEN** `cargo test -p swanflow --lib sql_collapse_probe` is run
- **THEN** it SHALL pass (string and int each 3 groups, no empty key, total rows preserved)
- **AND** the test SHALL carry no unused imports (CI enforces `-D warnings`).
