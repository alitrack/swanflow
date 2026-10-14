#!/usr/bin/env bash
# no-unwrap hygiene for the BUILT source (crates/*/src), non-test only.
#
# Ratchet: violations may only DECREASE from the pinned baseline; adding NEW
# ones is a hard failure. Zero is the eventual goal, but pinning the current
# count keeps the gate green-able while the backlog is paid down.
#
# Why a rewrite (2026-10-14): the original script grepped the top-level `src/`
# — dead code from before the workspace split (root Cargo.toml has NO
# [package]; members = crates/*). That dead tree carries ~234 violations and
# was being scanned, while the real built code (crates/*/src) was NEVER
# scanned. "0 violations, zero cost to harden" was a false negative.
#
# Test code is exempt (unwrap is idiomatic in tests): files under tests/ /
# benches/ / examples/, named *_test.rs / test_*.rs, and lines inside a
# `#[cfg(test)]` block. The attribute and `mod tests {` braces are on separate
# lines, so awk carries a `pending` flag from the attribute to the next `{`.
set -uo pipefail

cd "$(git rev-parse --show-toplevel)"

SRC_GLOB=(crates/swanflow/src crates/swanflow-core/src crates/swanflow-ai/src \
          crates/swanflow-knime/src crates/swanflow-plugin-sdk/src)

AWK_PROG='
BEGIN { in_test=0; pending=0; depth=0 }
{
  line=$0
  cand = (line ~ /\.unwrap\(\)/ || line ~ /\.expect\(/)
  if (cand && line ~ /unwrap_or|unwrap_err|unwrap_unchecked/) cand=0
  if (cand && !in_test) printf "%s:%d: %s\n", P, FNR, line
  # See the #[cfg(test)] attribute -> expect the module brace next.
  if (!in_test && line ~ /#\[cfg\(test\)\]/) pending=1
  # Walk braces; the very first `{` after a pending attribute opens the block.
  for (i=1; i<=length(line); i++) {
    c=substr(line,i,1)
    if (c=="{") {
      if (in_test) { depth++ }
      else if (pending) { pending=0; in_test=1; depth=1 }
    } else if (c=="}") {
      if (in_test) { depth--; if (depth<=0) { in_test=0; depth=0 } }
    }
  }
}
'

tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT
for d in "${SRC_GLOB[@]}"; do
  [ -d "$d" ] || continue
  while IFS= read -r f; do
    base=$(basename "$f")
    case "$base" in *_test.rs|test_*.rs) continue ;; esac
    case "$f" in */tests/*|*/benches/*|*/examples/*) continue ;; esac
    awk -v P="$f" "$AWK_PROG" "$f"
  done < <(find "$d" -name '*.rs' -type f)
done | sort -u > "$tmp"

count=$(wc -l < "$tmp")
# Ratchet baseline = the count the script itself reports on the tree as of the
# rewrite (2026-10-14). Re-pin by running the script and reading "current".
BASELINE=234

echo "no-unwrap hygiene (built crates/*/src, non-test):"
echo "  current : $count"
echo "  baseline: $BASELINE"
if [ "$count" -gt "$BASELINE" ]; then
  echo "❌ $((count-BASELINE)) NEW unwrap/expect violation(s) above baseline — reduce to at most $BASELINE:"
  sed -n '1,60p' "$tmp"
  exit 1
fi
if [ "$count" -gt 0 ]; then
  echo "⚠️  $count pre-existing violation(s) within baseline. Target: zero — only ADDING is blocked."
  exit 0
fi
echo "✅ No unwrap()/expect() in built non-test source."
exit 0
