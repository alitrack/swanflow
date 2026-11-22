#!/usr/bin/env bash
# no-unwrap hygiene for the BUILT source (crates/*/src), non-test only.
#
# Ratchet: violations may only DECREASE from the pinned baseline; adding NEW
# ones is a hard failure. Zero is the eventual goal, but pinning the current
# count keeps the gate green-able while the backlog is paid down.
#
# Test code is exempt (unwrap is idiomatic in tests). Rust-aware exemption:
#   1. files under tests/ / benches/ / examples/
#   2. files named *_test.rs / test_*.rs
#   3. lines inside an INLINE `#[cfg(test)] mod <name> { ... }` block
#   4. an ENTIRE file that is a separate-file test module, i.e. declared by a
#      `#[cfg(test)]` attribute immediately followed by a semicolon-terminated
#      `mod <name>;` in the parent (its body is <name>.rs — no brace to track).
#      The old awk missed #4: it keyed on `#[cfg(test)]` being in the FILE, but
#      for a separate-file module the attribute lives in the parent, so the
#      test file's unwraps were counted. It also lingered its `pending` flag
#      past a `mod x;` (no brace) into the next production fn — over-exempting.
#
# Fail-closed (was `set -uo pipefail`, no -e): an mktemp / scanner / count
# failure must NOT fall through to exit 0.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

SRC_GLOB=(crates/swanflow/src crates/swanflow-core/src crates/swanflow-ai/src \
          crates/swanflow-knime/src crates/swanflow-plugin-sdk/src)

# Print, for a file, the names of separate-file TEST modules it declares:
# a `#[cfg(test)]` attribute (own line or same line) immediately followed by a
# semicolon-terminated `mod <name>;`. (An inline `mod <name> {` has a brace,
# not a semicolon, and is handled by the per-file awk instead.)
extract_test_mods() {
  awk '
    {
      t=$0; sub(/^[ \t]+/,"",t); sub(/[ \t]+$/,"",t)
      if (t == "#[cfg(test)]") { pend=1; next }
      if (t ~ /^#\[cfg\(test\)\][ \t]*mod[ \t]+[A-Za-z0-9_]+[ \t]*;/) {
        m=t; sub(/^#\[cfg\(test\)\][ \t]*/,"",m); sub(/^[ \t]*mod[ \t]+/,"",m); sub(/[ \t]*;.*$/,"",m); print m; next
      }
      if (pend) {
        if (t ~ /^mod[ \t]+[A-Za-z0-9_]+[ \t]*;/) {
          m=t; sub(/^[ \t]*mod[ \t]+/,"",m); sub(/[ \t]*;.*$/,"",m); print m
        }
        pend=0
      }
    }
  ' "$1"
}

# Discover all separate-file test-module bodies up front (Rust-aware #4).
declare -A TEST_MOD_FILE=()
for d in "${SRC_GLOB[@]}"; do
  [ -d "$d" ] || continue
  while IFS= read -r f; do
    dir=$(dirname "$f")
    while IFS= read -r mname; do
      [ -n "$mname" ] || continue
      for cand in "$dir/$mname.rs" "$dir/$mname/mod.rs"; do
        [ -f "$cand" ] && TEST_MOD_FILE["$cand"]=1
      done
    done < <(extract_test_mods "$f")
  done < <(find "$d" -name '*.rs' -type f)
done

# Scan one file, printing its NON-test unwrap/expect lines. In-test exemption
# tracks only INLINE `#[cfg(test)] mod <name> { ... }` blocks by brace depth;
# a semicolon-terminated `mod x;` never opens a tracked block (no brace), so
# pending is cleared rather than lingers into later production code.
AWK_PROG='
BEGIN { in_test=0; pending=0; depth=0 }
{
  line=$0
  cand = (line ~ /\.unwrap\(\)/ || line ~ /\.expect\(/)
  if (cand && line ~ /unwrap_or|unwrap_err|unwrap_unchecked/) cand=0
  if (cand && !in_test) printf "%s:%d: %s\n", P, FNR, line

  if (in_test) {
    for (i=1;i<=length(line);i++){ c=substr(line,i,1)
      if (c=="{") depth++
      else if (c=="}") { depth--; if (depth<=0){ in_test=0; depth=0; break } } }
    next
  }
  t=line; sub(/^[ \t]+/,"",t); sub(/[ \t]+$/,"",t)
  # same-line attr + mod + open-brace
  if (t ~ /^#\[cfg\(test\)\][ \t]*mod[ \t]+[A-Za-z0-9_]+[ \t]*\{/) {
    in_test=1; depth=0
    for (i=1;i<=length(t);i++){ if (substr(t,i,1)=="{") depth++ }
    next
  }
  if (t == "#[cfg(test)]") { pending=1; next }
  if (pending) {
    # only an inline `mod <name> {` (with a brace) opens a tracked block.
    if (t ~ /^mod[ \t]+[A-Za-z0-9_]+[ \t]*\{/) {
      in_test=1; depth=0
      for (i=1;i<=length(t);i++){ if (substr(t,i,1)=="{") depth++ }
    }
    pending=0
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
    [ -n "${TEST_MOD_FILE[$f]:-}" ] && continue
    awk -v P="$f" "$AWK_PROG" "$f"
  done < <(find "$d" -name '*.rs' -type f)
done | sort -u > "$tmp"

count=$(wc -l < "$tmp")
# Baseline = the sound (Rust-aware) non-test count of the tree at the gate's
# introduction, independently verified by BOTH the pre-rewrite scanner and this
# one at commit c2a5888 (probe + docs_portal did not yet exist there: both
# report 234). On the current tree this scanner also reports 234 (the
# separate-file test module's 10 unwraps are now correctly exempt and the 3
# new production unwraps in docs_portal::build_doc were removed).
#
# This is a MONOTONIC ratchet with a MANUAL re-pin: after any clean-up that
# lowers the count you MUST re-pin BASELINE to the new lower value in the SAME
# commit ("re-pin by running the script on a clean tree and reading current").
# Leaving BASELINE at a high value after a reduction is a process violation —
# the gate is a ceiling, not an inventory, so 230-then-+4 would pass unless
# re-pinned. Pay the reduction down AND re-pin together.
#
# Re-pin history:
#   234  the gate's original calibration (commit c2a5888).
#   354  drifted +120 over the ceiling while §18.1's `|| true` + `| head -50`
#        made the check job green no matter what. The check job had been RED
#        the whole time; nobody could see it because clippy's step was swallowed
#        first. 114 of the 120 were the two copies of kernel/mock.rs (§17.2b),
#        which is why the drift looked like "nothing changed" per file.
#   230  after paying it down: one `lk()` helper per kernel/mock.rs copy
#        (57 sites each) + 4 real production unwraps turned into plan errors
#        (compiler.rs x3, knime/parser.rs) + 2 `_` arms that hard-cast to
#        StringArray rewritten to arrow's own formatter (excel.rs: Int32 /
#        Utf8View / Boolean / Date columns used to panic mid-export).
BASELINE=230

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
