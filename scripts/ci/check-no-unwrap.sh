#!/usr/bin/env bash
# Check that src/ contains no .unwrap() or .expect( calls.
# Tests/benches/examples are exempt — unwrap is acceptable there.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

# Find .unwrap() calls (not in comments, not "unwrap_" as part of identifier)
violations=$(grep -rn '\.unwrap()' src/ --include='*.rs' 2>/dev/null | grep -v '^src/.*\.rs:.*//' | grep -v 'unwrap_or' | grep -v 'unwrap_err' | grep -v 'unwrap_unchecked' || true)
violations2=$(grep -rn '\.expect(' src/ --include='*.rs' 2>/dev/null | grep -v '^src/.*\.rs:.*//' || true)

count=0
if [ -n "$violations" ]; then
    echo "::warning::unwrap() found in src/ (move to ? propagation):"
    echo "$violations"
    count=$((count + $(echo "$violations" | wc -l)))
fi
if [ -n "$violations2" ]; then
    echo "::warning::expect() found in src/ (move to ? propagation):"
    echo "$violations2"
    count=$((count + $(echo "$violations2" | wc -l)))
fi

if [ "$count" -gt 0 ]; then
    echo "❌ $count unwrap/expect violations in src/"
    echo "   These are warnings-only for now. Target: zero in src/."
    exit 0  # warning, not hard failure yet
fi

echo "✅ No unwrap()/expect() in src/"
