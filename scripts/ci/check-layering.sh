#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# check-layering.sh — Enforce downward-only layer dependencies.
#
# L0  (error.rs, connection_config.rs)  — zero crate:: deps
# L0.5 (kernel/)                        — only the error module + intra-kernel
# L1  (core/*.rs)                       — no agent/mcp/adapter
# L1.5 (nodes/*.rs)                     — no adapter
# L2  (agent/, mcp/, adapter/)          — any (leaf layers)
#
# Exit codes:
#   0 — all clean
#   1 — violations found (RED)
#   2 — the check itself could not run (missing/empty source tree) — RED
#
# Two defects made this gate unable to fail before, and both are worth keeping
# in mind because they are the same failure mode as the swallowed clippy step
# (see design.md §18.1: "CI 里有这个步骤" ≠ "CI 会因它失败"):
#
#   1. It checked `$ROOT/src`, which in this repo is a fossil of the old
#      single-crate layout — 66 tracked files that are NOT in
#      [workspace].members, so cargo never compiles them. The job printed
#      ALL CLEAN while enforcing nothing on the code that ships. It now checks
#      the compiled trees and refuses to run (exit 2) if a tree is missing or
#      has no `.rs` files, so "checked nothing" can never look like "clean".
#
#   2. L1/L1.5 used `^use crate::(agent|mcp|adapter)` under plain `grep`, where
#      `(...)` and `|` are LITERAL — the pattern could never match. Silently
#      dead rules. They now use `grep -E`.
#
# Falsification (how we know the gate is real, not merely present):
#   mkdir -p /tmp/lt/src/core/kernel
#   echo 'use crate::nodes::x;' > /tmp/lt/src/core/error.rs        # trips L0
#   echo 'use crate::adapter::x;' > /tmp/lt/src/core/kernel/m.rs   # trips L0.5
#   echo 'use crate::agent::x;' > /tmp/lt/src/core/compiler.rs     # trips L1
#   LAYERING_SRC=/tmp/lt bash scripts/ci/check-layering.sh; echo $?  # => 1, 3 rules
# ---------------------------------------------------------------------------
set -euo pipefail

ROOT="${1:-.}"
# NOTE: `ROOT` is resolved to an absolute path only *after* the self-test block,
# because `--selftest` arrives in $1 and `cd --selftest` would fail.

# ── Self-test: prove the gate can go red ──────────────────────────────────
# A green gate that has never been seen to fail is indistinguishable from a
# broken one — that is exactly how the swept-away clippy step survived
# (design.md §18.1). `--selftest` builds a throwaway tree with one violation
# per rule and asserts exit 1 with all four rules firing.
if [ "${1:-}" = "--selftest" ]; then
  st="$(mktemp -d)"
  trap 'rm -rf "$st"' EXIT
  mkdir -p "$st/src/core/kernel" "$st/src/nodes"
  printf 'use crate::nodes::x;\n'  > "$st/src/core/error.rs"        # L0
  printf 'use crate::adapter::x;\n' > "$st/src/core/kernel/m.rs"     # L0.5
  printf 'use crate::agent::y;\n'   > "$st/src/core/compiler.rs"     # L1
  printf 'use crate::adapter::z;\n' > "$st/src/nodes/n.rs"           # L1.5
  out="$(LAYERING_SRC="$st/src" bash "$0" . 2>&1)" && rc=0 || rc=$?
  n=$(printf '%s\n' "$out" | grep -c '✗' || true)
  if [ "$rc" -ne 1 ] || [ "$n" -ne 4 ]; then
    # `red` is defined further down (after the roots config), so use printf here.
    printf '\033[31m%s\033[0m\n' "❌ selftest FAILED: expected exit 1 with 4 violations, got exit $rc with $n"
    printf '%s\n' "$out" | sed 's/^/    /'
    exit 1
  fi
  echo "✅ selftest: 4 injected violations, 4 rules fired, exit 1"
  exit 0
fi

ROOT="$(cd "$ROOT" && pwd)"

# Source trees to check, one per line, as "<dir>|<error-module>|<intra-layer>".
# The two right-hand fields are the import prefixes that L0.5 allows: they
# differ per crate because the duplicated kernel tree (§17.2b) sits at
# `core::kernel` in swanflow and at `kernel` in swanflow-core.
ROOTS_FILE="${LAYERING_SRC:-}"
DEFAULT_ROOTS="crates/swanflow/src|core::error|core::kernel
crates/swanflow-core/src|error|kernel"

HAS_ERROR=0
CHECKED=0
red()   { printf '\033[31m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
info()  { printf '\033[36m%s\033[0m\n' "$*"; }

# A rule that finds nothing because the file is absent is not a pass.
need_dir() {
  local d="$1"
  [ -d "$d" ] || return 1
  # `find` rather than a glob: a glob that matches nothing expands to itself and
  # every later grep then fails silently against a nonexistent path.
  [ -n "$(find "$d" -name '*.rs' -type f -print -quit 2>/dev/null)" ]
}

check_one_root() {
  local src="$1" err_mod="$2" intra="$3"
  local n_kernel=0

  if ! need_dir "$src"; then
    red "  ✗ [setup] $src — missing or contains no .rs files; refusing to report a pass"
    HAS_ERROR=1
    return 0
  fi
  CHECKED=$((CHECKED + 1))
  info "── $src"

  # ── L0: the error module must not depend on anything else in the crate ──
  local f
  for f in "$src/error.rs" "$src/core/error.rs"; do
    [ -f "$f" ] || continue
    if grep -q '^use crate::' "$f"; then
      red "  ✗ [L0] ${f#"$ROOT"/} should have zero crate:: imports"
      grep -n '^use crate::' "$f" | sed 's/^/       /'
      HAS_ERROR=1
    fi
  done

  # ── L0.5: kernel may only reach the error module and itself ──
  local kdir
  for kdir in "$src/kernel" "$src/core/kernel"; do
    [ -d "$kdir" ] || continue
    local kf
    while IFS= read -r kf; do
      n_kernel=$((n_kernel + 1))
      local line
      while IFS= read -r line; do
        # `use crate::a::b;` -> import = a::b
        local import="${line#use crate::}"
        import="${import%%;*}"
        import="${import%% as *}"
        case "$import" in
          "$err_mod"|"$err_mod"::*) continue ;;
          "$intra"|"$intra"::*)     continue ;;
        esac
        red "  ✗ [L0.5] ${kf#"$ROOT"/} — only $err_mod or $intra allowed (found: use crate::$import)"
        HAS_ERROR=1
      done < <(grep '^use crate::' "$kf" 2>/dev/null || true)
    done < <(find "$kdir" -name '*.rs' -type f 2>/dev/null)
  done

  # ── L1: the core layer must not reach up into agent/mcp/adapter ──
  if [ -d "$src/core" ] || [ -d "$src/nodes" ]; then
    local vf
    while IFS= read -r vf; do
      if grep -nE '^use crate::(agent|mcp|adapter)\b' "$vf"; then
        red "  ✗ [L1] ${vf#"$ROOT"/} — forbidden: references agent/mcp/adapter"
        HAS_ERROR=1
      fi
    done < <(find "$src/core" -name '*.rs' -type f -not -path '*/kernel/*' 2>/dev/null || true)
  fi

  # ── L1.5: nodes must not reach up into adapter ──
  if [ -d "$src/nodes" ]; then
    local nf
    while IFS= read -r nf; do
      if grep -nE '^use crate::adapter\b' "$nf"; then
        red "  ✗ [L1.5] ${nf#"$ROOT"/} — forbidden: references adapter"
        HAS_ERROR=1
      fi
    done < <(find "$src/nodes" -name '*.rs' -type f 2>/dev/null || true)
  fi

  info "   kernel files checked: $n_kernel"
}

while IFS='|' read -r src err_mod intra; do
  [ -n "${src:-}" ] || continue
  case "$src" in \#*) continue ;; esac
  # A bare path (no `|` fields) gets the swanflow-shaped defaults, so the
  # falsification recipe at the top of this file works with nothing but a path.
  [ -n "${err_mod:-}" ] || err_mod="core::error"
  [ -n "${intra:-}" ] || intra="core::kernel"
  # A root may be given absolute (LAYERING_SRC) or relative to ROOT.
  case "$src" in
    /*) check_one_root "$src" "$err_mod" "$intra" ;;
    *)  check_one_root "$ROOT/$src" "$err_mod" "$intra" ;;
  esac
done <<< "${ROOTS_FILE:-$DEFAULT_ROOTS}"

# ── Summary ──
echo ""
if [ "$CHECKED" -eq 0 ]; then
  red "❌ Layer dependencies: NO SOURCE TREE WAS CHECKED — this is a failure, not a pass"
  exit 2
fi
if [ "$HAS_ERROR" -eq 0 ]; then
  green "✅ Layer dependencies: ALL CLEAN ($CHECKED source tree(s) checked)"
  exit 0
fi
red "❌ Layer dependencies: VIOLATIONS FOUND"
echo ""
echo "Layer rules:"
echo "  L0   (error.rs)              — zero crate:: imports"
echo "  L0.5 (kernel/)               — only the error module and intra-kernel"
echo "  L1   (core/*.rs)             — no agent/mcp/adapter"
echo "  L1.5 (nodes/*.rs)            — no adapter"
echo ""
echo "Fix: move the violating import to the correct layer."
exit 1
