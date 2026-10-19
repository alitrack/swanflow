# Security Policy

## Supported Versions

SwanFlow is pre-1.0. Only the `main` branch and the latest tag receive
security fixes. Version support windows will be published here after the
1.0 release.

## Reporting a Vulnerability

**Do not open a public GitHub issue for security problems.**

Please report privately via either channel:

1. **GitHub Security Advisories** (preferred): use
   "Report a vulnerability" on the Security tab of this repository.
2. **Email**: contact the maintainer directly. Include
   `[swanflow security]` in the subject line.

Please include:

- Description of the issue and its impact
- Steps to reproduce or a proof of concept
- Affected versions / commits
- Any suggested mitigation

## Response SLA

- Acknowledgement: within **7 days**
- Severity triage: within **14 days**

## Scope

In scope: the engine crates, plugin SDK, workflow parsing, connector
layer, WASM UDF sandbox, Python bridge, and anything reachable from a
workflow YAML executed by the engine. Out of scope: vulnerabilities in
third-party dependencies (report those upstream) and issues requiring
already-privileged local access to the operator's own machine.

## Hardening Notes for Reviewers

Areas that historically receive the most security scrutiny in this
codebase — useful starting points for audits:

- WASM UDF sandbox boundary
- Python bridge (PyO3) input marshalling
- `${...}` substitution in workflow YAML and connection profiles
- File/URL inputs parsed from untrusted workflow files
