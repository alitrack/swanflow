#!/usr/bin/env python3
"""Build the `ext_functions` catalog consumed by SwanFlow's extfn auto-registration.

Reads DuckDB's own `duckdb_functions()` catalog for every extension that is
loadable on this machine and materializes it into a DuckDB file with these
columns:

    extension_name, function_name, function_type, parameters, parameter_types,
    return_type, description, examples, categories, has_side_effects,
    stability, internal, enabled

`enabled` encodes tier-1 (auto-registered on startup) vs tier-2 (needs human
curation):

    enabled = (function_type = 'table' AND extension_name <> 'core_functions'
               AND NOT internal)

Measured on DuckDB v2.0 with 12 extensions: 2,225 catalogued functions, 66
enabled. Without that filter the canvas would sprout 2,225 nodes. To opt a
scalar/aggregate function in:

    UPDATE ext_functions SET enabled = TRUE WHERE extension_name = 'spatial';

Usage:
    python3 scripts/build_ext_functions.py [--db PATH] [--cli PATH] [--list-only]

Environment:
    SWANFLOW_EXT_FUNCTIONS_DB   default output DB path
    SWANFLOW_DUCKDB_CLI         explicit duckdb CLI to use

Why the CLI is driven as a subprocess instead of using the `duckdb` python
module: on a dev box the module and the CLI are frequently *different* DuckDB
builds with *different* extension directories, and only one of them has the
local extension binaries. Measured 2026-09-21 on this WSL box:

    python module 1.6.0.dev343   -> extension dir empty, 0 extensions loadable
    venv CLI v1.5.4              -> 19 extensions loadable, NO extension_name col
    ~/.local/bin CLI v2.0.0-...  -> 8 extensions loadable, HAS extension_name col

The CLI is the one that can actually LOAD anything, so the CLI it is.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

# Tier-1 predicate. Kept as a single string so the SQL and the docs below can
# never drift apart.
#
# `internal` is deliberately NOT part of this predicate. In DuckDB it means "the
# function is implemented in C++ rather than as a SQL macro", which is TRUE for
# every single row of duckdb_functions() (measured: 3253/3253). It is NOT a
# system-vs-extension discriminator — `extension_name` is. Using it here would
# silently enable zero nodes.
ENABLED_EXPR = "(function_type = 'table' AND extension_name <> 'core_functions')"

# Human-curated opt-ins: extensions whose functions (including scalars) are
# enabled on rebuild. Without this, a catalog rebuild silently resets
# `enabled` for anything not covered by ENABLED_EXPR (this exact trap
# reverted dashboard's 33 scalar functions to 0 on the next rebuild).
CURATED_EXTENSIONS = {"dashboard"}
_curated_list = ", ".join(f"'{e}'" for e in sorted(CURATED_EXTENSIONS))
ENABLED_EXPR_FULL = f"({ENABLED_EXPR[1:-1]} OR extension_name IN ({_curated_list}))"

DEFAULT_DB = os.environ.get(
    "SWANFLOW_EXT_FUNCTIONS_DB", "target/swanflow_cache/ext_functions.duckdb"
)

CLI_CANDIDATES = [
    os.environ.get("SWANFLOW_DUCKDB_CLI", ""),
    str(Path.home() / ".local/bin/duckdb"),
    str(Path.home() / ".hermes/hermes-agent/venv/bin/duckdb"),
    shutil.which("duckdb") or "",
]


def log(msg: str) -> None:
    print(msg, flush=True)


def find_cli(explicit: str | None) -> str:
    cands = ([explicit] if explicit else []) + CLI_CANDIDATES
    for c in cands:
        if c and os.path.isfile(c) and os.access(c, os.X_OK):
            return c
    sys.exit("error: no duckdb CLI found; pass --cli PATH or set SWANFLOW_DUCKDB_CLI")


def run(cli: str, sql: str) -> str:
    """Run SQL through the CLI and return stdout (raises on hard failure)."""
    p = subprocess.run(
        [cli, "-unsigned", "-c", sql],
        capture_output=True,
        text=True,
        timeout=300,
    )
    if p.returncode != 0:
        sys.exit(f"error: duckdb CLI failed ({p.returncode})\n{p.stderr[:2000]}")
    return p.stdout


def cli_version(cli: str) -> str:
    out = subprocess.run([cli, "--version"], capture_output=True, text=True, timeout=60)
    return out.stdout.strip()


def version_token(raw: str) -> str:
    """Pull the bare version out of `--version` output.

    The CLI prints more than a version, e.g.
        `v2.0.0-alpha39998 (Cyanoptera) a00803f768`
    while the extension directory is named exactly `v2.0.0-alpha39998`. Comparing
    the whole string against directory names silently matches nothing, so take
    the first whitespace-delimited token.
    """
    m = re.match(r"\s*(v?\d[^\s]*)", raw)
    return m.group(1) if m else raw.strip().split()[0] if raw.strip() else ""


def _norm_version(s: str) -> str:
    return s[1:] if s.startswith("v") else s


def extension_dir(version: str) -> Path | None:
    """`~/.duckdb/extensions/<version>/<platform>/` for this CLI, if present."""
    root = Path.home() / ".duckdb/extensions"
    if not root.is_dir():
        return None
    want = _norm_version(version)
    for vdir in sorted(root.iterdir(), reverse=True):
        if not vdir.is_dir() or _norm_version(vdir.name) != want:
            continue
        for pdir in sorted(vdir.iterdir()):
            if pdir.is_dir():
                return pdir
    return None


def loadable_extensions(extdir: Path | None) -> list[str]:
    """Extension names with a binary sitting in the CLI's own version dir."""
    if not extdir or not extdir.is_dir():
        return []
    names = []
    for f in sorted(extdir.iterdir()):
        if f.name.endswith(".duckdb_extension"):
            names.append(f.name[: -len(".duckdb_extension")])
    return names


def has_extension_name(cli: str) -> bool:
    """Does this DuckDB expose `duckdb_functions().extension_name`?

    v2.x does; v1.5.x does not (its columns stop at the classic set). This
    decides direct attribution vs load-diff attribution.
    """
    out = run(cli, "DESCRIBE SELECT * FROM duckdb_functions();")
    return "extension_name" in out


FULL_COLS = (
    "extension_name, function_name, function_type, parameters, parameter_types, "
    "return_type, description, examples, categories, has_side_effects, stability, "
    "internal"
)


def sql_direct(exts: list[str]) -> str:
    """v2.x path: `extension_name` exists, so it is one CREATE TABLE."""
    lines = [".bail off"]
    lines += [f"LOAD {e};" for e in exts]
    lines.append(
        f"CREATE OR REPLACE TABLE ext_functions AS "
        f"SELECT {FULL_COLS}, {ENABLED_EXPR_FULL} AS enabled "
        f"FROM duckdb_functions() "
        f"WHERE extension_name IS NOT NULL AND extension_name <> '';"
    )
    return "\n".join(lines) + "\n"


def sql_loaddiff(exts: list[str]) -> str:
    """v1.5.x path: no `extension_name`, so attribute by set difference.

    Snapshot the function set, LOAD one extension, insert whatever appeared,
    re-snapshot, repeat. `seen` is updated after every extension so a later
    extension cannot re-claim an earlier one's functions.
    """
    seen_match = (
        "NOT EXISTS (SELECT 1 FROM seen s "
        "WHERE s.function_name = f.function_name "
        "AND s.parameter_types IS NOT DISTINCT FROM f.parameter_types "
        "AND s.return_type IS NOT DISTINCT FROM f.return_type)"
    )
    has_side = "coalesce(f.has_side_effects, false)"
    lines = [
        ".bail off",
        "CREATE OR REPLACE TABLE seen AS "
        "SELECT function_name, parameter_types, return_type FROM duckdb_functions();",
        "CREATE OR REPLACE TABLE ext_fn_raw("
        "extension_name VARCHAR, function_name VARCHAR, function_type VARCHAR, "
        "parameters VARCHAR[], parameter_types VARCHAR[], return_type VARCHAR, "
        "description VARCHAR, examples VARCHAR[], categories VARCHAR[], "
        "has_side_effects BOOLEAN, stability VARCHAR, internal BOOLEAN);",
    ]
    for e in exts:
        lines.append(f"LOAD {e};")
        lines.append(
            f"INSERT INTO ext_fn_raw SELECT '{e}', f.function_name, f.function_type, "
            f"f.parameters, f.parameter_types, f.return_type, f.description, "
            f"f.examples, f.categories, {has_side}, f.stability, "
            f"coalesce(f.internal, false) FROM duckdb_functions() f WHERE {seen_match};"
        )
        lines.append(
            f"INSERT INTO seen SELECT f.function_name, f.parameter_types, "
            f"f.return_type FROM duckdb_functions() f WHERE {seen_match};"
        )
    lines += [
        "CREATE OR REPLACE TABLE ext_functions AS SELECT *, "
        f"{ENABLED_EXPR_FULL} AS enabled FROM "
        "(SELECT DISTINCT * FROM ext_fn_raw);",
        "DROP TABLE seen;",
        "DROP TABLE ext_fn_raw;",
    ]
    return "\n".join(lines) + "\n"


def build(cli: str, db: str, list_only: bool) -> None:
    raw_version = cli_version(cli)
    vtoken = version_token(raw_version)
    extdir = extension_dir(vtoken)
    exts = loadable_extensions(extdir)
    direct = has_extension_name(cli)

    log(f"duckdb CLI   : {cli}")
    log(f"version      : {raw_version}  (dir token: {vtoken})")
    log(f"ext dir      : {extdir or '(none)'}")
    log(f"loadable     : {len(exts)} extension(s){': ' + ', '.join(exts) if exts else ''}")
    log(f"attribution  : {'direct (extension_name column)' if direct else 'load-diff fallback'}")
    log(f"output DB    : {db}")

    if list_only:
        return

    sql = sql_direct(exts) if direct else sql_loaddiff(exts)

    db_path = Path(db)
    if db_path.parent and str(db_path.parent) not in ("", "."):
        db_path.parent.mkdir(parents=True, exist_ok=True)
    # Start clean so a rebuild never merges with a stale catalog.
    for suffix in ("", ".wal"):
        p = Path(str(db) + suffix)
        if p.exists():
            p.unlink()

    with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False) as fh:
        fh.write(sql)
        sql_file = fh.name
    try:
        with open(sql_file) as fh:
            p = subprocess.run(
                [cli, "-unsigned", db],
                stdin=fh,
                capture_output=True,
                text=True,
                timeout=1800,
            )
        # `.bail off` means a per-extension LOAD failure is not fatal; surface it.
        errs = [ln for ln in p.stderr.splitlines() if "Error" in ln or "error" in ln]
        if errs:
            log(f"note: {len(errs)} non-fatal error line(s) during LOAD:")
            for ln in errs[:10]:
                log(f"  {ln}")
        # A non-zero CLI exit code is EXPECTED here: in non-interactive mode the
        # duckdb CLI exits non-zero if any statement errored, even with `.bail off`
        # (where a failed LOAD must not abort the catalog build — one broken
        # extension must not take the whole catalog down). The authoritative
        # pass/fail signal is verify() below, which inspects what actually
        # landed in the DB. Do NOT exit on p.returncode.
        if p.returncode != 0:
            log(f"note: CLI exited {p.returncode} (expected when an extension failed to LOAD)")
    finally:
        os.unlink(sql_file)

    verify(cli, db)


def verify(cli: str, db: str) -> None:
    """Report what actually landed — this is the build's own acceptance test."""
    q = (
        "SELECT count(*) AS catalogued, "
        "count(DISTINCT (extension_name, function_name)) FILTER (WHERE enabled) AS nodes_enabled, "
        "count(*) FILTER (WHERE enabled) AS rows_enabled, "
        "count(DISTINCT extension_name) AS exts "
        "FROM ext_functions;"
    )
    out = subprocess.run(
        [cli, db, "-c", q], capture_output=True, text=True, timeout=300
    ).stdout
    log("\n=== catalog built ===")
    log(out.strip())
    # `nodes_enabled` is what actually appears on the canvas: the registry
    # de-duplicates by (extension, function) via SELECT DISTINCT.
    per = (
        "SELECT extension_name, count(DISTINCT function_name) AS enabled_nodes "
        "FROM ext_functions WHERE enabled GROUP BY 1 ORDER BY 2 DESC;"
    )
    out2 = subprocess.run(
        [cli, db, "-c", per], capture_output=True, text=True, timeout=300
    ).stdout
    log("\n=== enabled (tier-1) nodes per extension ===")
    log(out2.strip())
    log(
        "\nNext: these become canvas nodes as `ext.<extension>.<function>` at the "
        "next SwanFlow start. Opt scalar/aggregate functions in with:\n"
        f"  {cli} {db} -c \"UPDATE ext_functions SET enabled = TRUE "
        "WHERE extension_name = 'spatial';\""
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=DEFAULT_DB, help=f"output DB (default: {DEFAULT_DB})")
    ap.add_argument("--cli", default=None, help="duckdb CLI to use")
    ap.add_argument(
        "--list-only",
        action="store_true",
        help="show what would be loaded and exit without building",
    )
    args = ap.parse_args()
    build(find_cli(args.cli), args.db, args.list_only)


if __name__ == "__main__":
    main()
