## ADDED Requirements

### Requirement: Self-contained single-file HTML export
The `swanflow docs` command SHALL render a workflow's lineage into a single
self-contained HTML file with no local resource dependencies.

#### Scenario: Default export has zero external references
- **WHEN** `swanflow docs <workflow.yaml>` is run without `--mermaid`
- **THEN** the output file SHALL be a single `.html` file
- **AND** it SHALL contain no `http://` or `https://` references (no CDN, no external CSS/JS/images).

#### Scenario: Default export shows lineage, nodes, and edges
- **WHEN** the generated HTML is opened in a browser
- **THEN** it SHALL show the node set (each with `type_id`, `mode`, `error_mode`, `state`, and `config` as pretty JSON), the edge list (source → target with ports), and a lineage view ordered by topological depth.

### Requirement: Mermaid is an opt-in enhancement with offline degradation
The `--mermaid` flag SHALL add a Mermaid lineage diagram; without network the
page SHALL degrade gracefully to the raw diagram text without erroring.

#### Scenario: Mermaid enhancement includes a renderable diagram
- **WHEN** `swanflow docs <workflow.yaml> --mermaid` is run
- **THEN** the output SHALL contain a `<pre class="mermaid">` diagram (Sources/Steps/Sinks subgraphs) and a CDN `<script>` for Mermaid.
- **AND** if the CDN is unreachable, the page SHALL still render the rest and display the raw Mermaid text rather than failing.

### Requirement: All workflow data is HTML-escaped
The command SHALL HTML-escape all workflow-derived values (node `type_id`,
`config` JSON, and workflow `name`) before writing them to the document.

#### Scenario: A node config containing markup is escaped
- **WHEN** a node's `config` JSON or `type_id` contains `<`, `>`, `&`, `"` or `'`
- **THEN** the rendered HTML SHALL contain the escaped entities, not raw markup (no injection, no broken layout).

### Requirement: Large graphs degrade to a list, not a broken layout
When the graph exceeds a node-count threshold, the lineage view SHALL degrade to
a topological list plus adjacency table rather than an unwieldy visual.

#### Scenario: A graph over the threshold renders as a list
- **WHEN** the workflow has more than the configured node threshold
- **THEN** the lineage view SHALL render a topological-order list with an adjacency table and a hint to use `--mermaid`, without producing a broken or unusable layout.
