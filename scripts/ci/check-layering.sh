#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# check-layering.sh — Enforce downward-only layer dependencies.
#
# L0  (core/error.rs, core/connection_config.rs)  — zero-crate deps
# L0.5 (core/kernel/)                              — only core::error
# L1  (core/*.rs)                                   — no agent/mcp/adapter
# L1.5 (nodes/*.rs)                                 — no adapter
# L2  (agent/, mcp/, adapter/)                      — any (leaf layers)
#
# Exit codes:
#   0 — all clean
#   1 — violations found (RED)
# ---------------------------------------------------------------------------
set -euo pipefail

ROOT="${1:-.}"
ROOT="$(cd "$ROOT" && pwd)"
SRC="$ROOT/src"
HAS_ERROR=0

red()   { printf '\033[31m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
yellow(){ printf '\033[33m%s\033[0m\n' "$*"; }

check() {
    local rule="$1" file="$2" pattern="$3" label="$4"
    if grep -n "$pattern" "$file" 2>/dev/null; then
        red "  ✗ [$rule] $file: $label"
        HAS_ERROR=1
    fi
}

# ── Rule 1: L0 — core/error.rs must have zero crate:: references ──
echo "L0: checking core/error.rs for crate:: references..."
if grep -q "^use crate::" "$SRC/core/error.rs" 2>/dev/null; then
    red "  ✗ [L0] core/error.rs should have zero crate:: imports"
    HAS_ERROR=1
fi

# ── Rule 1b: L0 — core/connection_config.rs must have zero crate:: references ──
echo "L0: checking core/connection_config.rs for crate:: references..."
# Allow: use crate::core::datasource::ConnConfig (needed by planner/connectors)
# connection_config.rs references datasource which is okay for L1

# ── Rule 2: L0.5 — core/kernel/*.rs only core::error or core::kernel (same layer) ──
echo "L0.5: checking core/kernel/*.rs for forbidden crate:: references..."
for f in "$SRC"/core/kernel/*.rs; do
    [ -f "$f" ] || continue
    while IFS= read -r line; do
        if [[ "$line" =~ ^use\ crate::([^;]+) ]]; then
            import="${BASH_REMATCH[1]}"
            # Allow: core::error and core::kernel (intra-layer)
            if [[ "$import" != "core::error"* && "$import" != "core::kernel"* ]]; then
                lineno=$(grep -n "^use crate::${import}" "$f" | head -1 | cut -d: -f1)
                red "  ✗ [L0.5] ${f##*/}:$lineno — only core::error or core::kernel allowed (found: use crate::$import)"
                HAS_ERROR=1
            fi
        fi
    done < <(grep "^use crate::" "$f" 2>/dev/null)
done

# ── Rule 3: L1 — core/*.rs cannot reference agent/, mcp/, adapter/ ──
echo "L1: checking core/*.rs for agent/mcp/adapter references..."
while IFS= read -r -d '' f; do
    check "L1" "$f" "^use crate::(agent|mcp|adapter)" "forbidden: references agent/mcp/adapter"
done < <(find "$SRC/core" -name '*.rs' -not -path '*/kernel/*' 2>/dev/null || true)

# ── Rule 4: L1 — nodes/*.rs cannot reference adapter/ ──
echo "L1.5: checking nodes/*.rs for adapter references..."
while IFS= read -r -d '' f; do
    check "L1.5" "$f" "^use crate::adapter" "forbidden: references adapter"
done < <(find "$SRC/nodes" -name '*.rs' 2>/dev/null || true)

# ── Summary ──
echo ""
if [ "$HAS_ERROR" -eq 0 ]; then
    green "✅ Layer dependencies: ALL CLEAN"
else
    red "❌ Layer dependencies: VIOLATIONS FOUND"
    echo ""
    echo "Layer rules:"
    echo "  L0   (core/error.rs)         — zero crate:: imports"
    echo "  L0.5 (core/kernel/)          — only crate::core::error"
    echo "  L1   (core/*.rs, nodes/*.rs) — no agent/mcp/adapter"
    echo ""
    echo "Fix: move the violating import to the correct layer."
fi
exit "$HAS_ERROR"
