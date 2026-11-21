#!/usr/bin/env python3
"""M1.b smoke test: `swanflow provenance header` on a real DuckDB file.

Checks the three things that matter for the contract:
  1. the header is emitted and parses as JSON with every contract field
  2. the same code + env + inputs yield the SAME artifact_id across runs
  3. changing the data changes the artifact_id (and the input fingerprint)
"""
import json
import os
import subprocess
import sys
import tempfile

import duckdb

BIN = os.environ.get("SWANFLOW_BIN", "/tmp/sb-target/debug/swanflow")
fails = []


def check(cond, label, detail=""):
    print(("PASS " if cond else "FAIL ") + label + (f" | {detail}" if detail else ""))
    if not cond:
        fails.append(label)


def main() -> int:
    if not os.path.exists(BIN):
        print(f"SKIP: binary not built at {BIN}")
        return 0

    work = tempfile.mkdtemp(prefix="sb-smoke-")
    db = os.path.join(work, "demo.duckdb")
    # The Python DuckDB here is 2.0.0 and writes storage version 69; the version
    # statically bundled into the swanflow binary is older and refuses that file.
    # Write in a compatible format so the smoke test exercises OUR code, not the
    # file-format gap (that gap is a separate, real finding).
    con = duckdb.connect(db, config={"storage_compatibility_version": "v1.2.0"})
    con.execute("CREATE TABLE sales(id INT, region VARCHAR)")
    con.execute("INSERT INTO sales VALUES (1,'east'),(2,'west'),(3,'north')")
    con.execute("CREATE TABLE out_region AS SELECT region, count(*) AS n FROM sales GROUP BY 1")
    con.close()

    code = "CREATE TABLE out_region AS SELECT region, count(*) AS n FROM sales GROUP BY 1;"
    base = [
        BIN, "provenance", "header",
        "--db", db, "--table", "out_region",
        "--description", "region totals for smoke test",
        "--code", code, "--input", "sales",
        "--print-id",
    ]

    r1 = subprocess.run(base, capture_output=True, text=True)
    check(r1.returncode == 0, "cli exits 0", (r1.stderr or "").strip()[:120])
    id1 = r1.stdout.strip().splitlines()[-1] if r1.stdout.strip() else ""
    check(id1.startswith("sha256:") and len(id1) == 71, "prints sha256: id", id1)

    r2 = subprocess.run(base, capture_output=True, text=True)
    id2 = r2.stdout.strip().splitlines()[-1] if r2.stdout.strip() else ""
    check(id1 == id2, "same code+env+inputs => same artifact_id", f"{id1[:20]}… vs {id2[:20]}…")

    hdr_path = os.path.join(work, "out_region.sbheader.json")
    if not os.path.exists(hdr_path):
        # default path is relative to cwd
        hdr_path = os.path.join(os.getcwd(), "out_region.sbheader.json")
    check(os.path.exists(hdr_path), "header file written", hdr_path)

    header = {}
    if os.path.exists(hdr_path):
        with open(hdr_path, encoding="utf-8") as f:
            header = json.load(f)
        for key in ["artifact_id", "version", "created_at", "description", "code",
                    "conversation_ref", "env", "inputs", "outputs", "approvals"]:
            check(key in header, f"header has {key}")

    if header:
        check(header["env"]["engine"]["name"] == "duckdb", "engine recorded",
              header["env"]["engine"]["version"])
        check(len(header["inputs"]) == 1 and header["inputs"][0]["path"] == "sales",
              "input fingerprint recorded", str(header["inputs"][0]["rows"]) if header["inputs"] else "none")
        check(header["outputs"][0]["fingerprint_kind"] == "aggregate",
              "output fingerprint_kind=aggregate")
        check(header["outputs"][0]["rows"] == 3, "output rows counted",
              str(header["outputs"][0]["rows"]))

    # data change must change the id
    con = duckdb.connect(db, config={"storage_compatibility_version": "v1.2.0"})
    con.execute("INSERT INTO sales VALUES (4,'east')")
    con.execute("DROP TABLE out_region")
    con.execute(code)
    con.close()
    os.remove(hdr_path) if os.path.exists(hdr_path) else None
    r3 = subprocess.run(base, capture_output=True, text=True)
    id3 = r3.stdout.strip().splitlines()[-1] if r3.stdout.strip() else ""
    check(id3 and id3 != id1, "data change => different artifact_id",
          f"{id1[:16]}… -> {id3[:16]}…")

    print(f"\n{'ALL PASS' if not fails else 'FAILURES: ' + ', '.join(fails)}")
    print(f"workdir: {work}")
    return 0 if not fails else 1


if __name__ == "__main__":
    sys.exit(main())
