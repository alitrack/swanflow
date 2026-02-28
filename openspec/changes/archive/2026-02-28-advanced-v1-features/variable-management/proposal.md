## Why
SwanFlow needs a flexible way to parameterize workflows without the visual clutter of KNIME's "red lines." A centralized variable system allows AI and users to inject dynamic values (like thresholds, file paths, or dates) into any node's configuration seamlessly.

## What Changes
- Add a **Global Context** to the `Workflow` structure to store key-value parameters.
- Implement **String Interpolation** in the `WorkflowCompiler`: support `${context.key}` and `${node_id.output}` placeholders.
- Introduce `VariableCreatorNode` to define new variables within the DAG.
- Introduce `TableToVariableNode` to capture data from a table (e.g., first row) and promote it to a flow variable.

## Capabilities

### New Capabilities
- `parameterized-workflows`: Centralized management of global and local variables.
- `configuration-templating`: Dynamic rendering of node configs during the compilation pass.

### Modified Capabilities
- `workflow-compilation`: Updated to perform a pre-pass for variable resolution.

## Impact
- **Compiler**: New logic for recursive template resolution.
- **Node Protocol**: Updated to allow late-binding of configuration values.
- **UX**: Simpler, cleaner workflows with hidden "shadow connections" for data parameters.
