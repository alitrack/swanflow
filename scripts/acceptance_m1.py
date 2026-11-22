#!/usr/bin/env python3
"""M1 acceptance gate: the artifact must be reproducible from its header alone.

This is the exit condition from `alitrack/swanbench` docs/design.md §6:

    take one historical result, hand over **only the header**, re-run it, and get
    the same number back; the artifact_id is stable; when one input row changes,
    `--check` must say "data changed" rather than "wrong answer".

Three parties are simulated, each in its own directory:

    producer/   creates the result and the header, then is never consulted again
    reviewer/   receives a copy of the raw database *with the output table deleted*
                plus the header — nothing else. If the rerun passes here, the
                header alone was sufficient.
    tampered/   same raw data, one input row changed, to test --check

The reviewer's database must be byte-identical before and after the rerun: a
review may never modify the file it is reviewing.
"""
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile

import duckdb

BIN = os.environ.get("SWANFLOW_BIN", "/tmp/sb-target/debug/swanflow")
CODE = "CREATE TABLE out_region AS SELECT region, count(*) AS n FROM sales GROUP BY 1;"
fails = []


def check(cond, label, detail=""):
    print(("PASS " if cond else "FAIL ") + label + (f" | {detail}" if detail else ""))
    if not cond:
        fails.append(label)


def run(*args, **kw):
    return subprocess.run([BIN, *args], capture_output=True, text=True, **kw)


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def seed(path):
    """Create the raw input table in a format the statically linked engine reads.

    Python's duckdb here is 2.0.0 and writes storage v69, which the version inside
    the swanflow binary refuses; that interop gap is a separate documented finding
    (design.md §8), not what this gate is testing.
    """
    con = duckdb.connect(path, config={"storage_compatibility_version": "v1.2.0"})
    con.execute("CREATE TABLE sales(id INT, region VARCHAR)")
    con.execute("INSERT INTO sales VALUES (1,'east'),(2,'west'),(3,'north')")
    con.close()


def main() -> int:
    if not os.path.exists(BIN):
        print(f"SKIP: binary not built at {BIN}")
        return 0

    root = tempfile.mkdtemp(prefix="sb-m1-accept-")
    producer = os.path.join(root, "producer")
    reviewer = os.path.join(root, "reviewer")
    tampered = os.path.join(root, "tampered")
    for d in (producer, reviewer, tampered):
        os.makedirs(d)

    # ── 1. producer: make the result and the header ──────────────────────────
    p_db = os.path.join(producer, "analysis.duckdb")
    seed(p_db)
    con = duckdb.connect(p_db, config={"storage_compatibility_version": "v1.2.0"})
    con.execute(CODE)
    con.close()

    p_hdr = os.path.join(producer, "out_region.sbheader.json")
    r = run("provenance", "header", "--db", p_db, "--table", "out_region",
            "--description", "region totals, M1 acceptance",
            "--code", CODE, "--input", "sales", "--out", p_hdr)
    check(r.returncode == 0, "producer emits a header", (r.stderr or "").strip()[:140])
    if not os.path.exists(p_hdr):
        print("\nFAILURES: header was never written")
        return 1
    with open(p_hdr, encoding="utf-8") as f:
        hdr = json.load(f)
    id_produced = hdr["artifact_id"]
    rows_recorded = hdr["outputs"][0]["rows"]
    hash_recorded = hdr["outputs"][0]["content_sha256"]
    check(rows_recorded == 3, "header records 3 output rows", str(rows_recorded))

    # ── 2. reviewer: raw data only — the output table is deleted ─────────────
    r_db = os.path.join(reviewer, "analysis.duckdb")
    shutil.copyfile(p_db, r_db)
    r_hdr = os.path.join(reviewer, os.path.basename(p_hdr))
    shutil.copyfile(p_hdr, r_hdr)
    con = duckdb.connect(r_db, config={"storage_compatibility_version": "v1.2.0"})
    con.execute("DROP TABLE out_region")
    left = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
    ).fetchall()
    con.close()
    check([t[0] for t in left] == ["sales"],
          "reviewer has the raw input only (no output table to peek at)", str(left))
    before = sha256_file(r_db)

    # ── 3. THE GATE: rerun from the header alone ────────────────────────────
    r = run("provenance", "rerun", "--header", r_hdr, "--db", r_db, "--print-verdict")
    verdict = (r.stdout or "").strip().splitlines()[-1] if (r.stdout or "").strip() else ""
    check(r.returncode == 0 and verdict == "pass",
          "rerun from the header alone reproduces the result",
          f"verdict={verdict!r} {(r.stderr or '').strip()[:120]}")
    check(sha256_file(r_db) == before,
          "the review did not modify the file it reviewed")

    # Full report, for the record: rows/hash as observed by the reviewer.
    r = run("provenance", "rerun", "--header", r_hdr, "--db", r_db)
    out = r.stdout or ""
    check("header-only review" in out,
          "the report says there was no stored table to compare (header-only review)")
    check(hash_recorded[:12] in out, "observed fingerprint equals the recorded one")

    # ── 4. id stability: same code+env+inputs ⇒ same id, twice ──────────────
    second = os.path.join(producer, "again.sbheader.json")
    r = run("provenance", "header", "--db", p_db, "--table", "out_region",
            "--description", "region totals, M1 acceptance",
            "--code", CODE, "--input", "sales", "--out", second, "--print-id")
    id2 = (r.stdout or "").strip().splitlines()[-1] if (r.stdout or "").strip() else ""
    check(id2 == id_produced, "artifact_id is stable across runs",
          f"{id_produced[:18]}… vs {id2[:18]}…")

    # ── 5. --check on intact data passes ────────────────────────────────────
    r = run("provenance", "check", "--header", r_hdr, "--db", r_db)
    check(r.returncode == 0 and "PASS" in (r.stdout or ""),
          "--check passes while the inputs are untouched", (r.stderr or "").strip()[:120])

    # ── 6. one input row changes ⇒ --check says "data changed" ──────────────
    t_db = os.path.join(tampered, "analysis.duckdb")
    seed(t_db)
    con = duckdb.connect(t_db, config={"storage_compatibility_version": "v1.2.0"})
    con.execute(CODE)
    con.execute("UPDATE sales SET region='south' WHERE id=2")
    con.close()
    t_hdr = os.path.join(tampered, os.path.basename(p_hdr))
    shutil.copyfile(p_hdr, t_hdr)
    r = run("provenance", "check", "--header", t_hdr, "--db", t_db)
    out = r.stdout or ""
    check(r.returncode != 0 and "DATA_CHANGED" in out,
          "one changed input row ⇒ --check reports DATA_CHANGED",
          f"exit={r.returncode}")
    check("sales" in out, "--check names the input that changed")

    # ── 7. a hand-edited header must not pass ───────────────────────────────
    e_hdr = os.path.join(tampered, "edited.sbheader.json")
    edited = dict(hdr)
    edited["description"] = "region totals, but the description was rewritten later"
    with open(e_hdr, "w", encoding="utf-8") as f:
        json.dump(edited, f, ensure_ascii=False, indent=2)
    r = run("provenance", "check", "--header", e_hdr, "--db", t_db)
    out = r.stdout or ""
    check(r.returncode != 0 and "does NOT re-derive" in out,
          "an edited header no longer re-derives its artifact_id", f"exit={r.returncode}")

    # ── 8. an incomplete header is refused by name, not by "failed" ─────────
    b_hdr = os.path.join(tampered, "broken.sbheader.json")
    broken = json.loads(json.dumps(hdr))
    broken["code"]["text"] = ""
    broken["inputs"] = []
    with open(b_hdr, "w", encoding="utf-8") as f:
        json.dump(broken, f, ensure_ascii=False, indent=2)
    r = run("provenance", "rerun", "--header", b_hdr, "--db", r_db)
    out = (r.stdout or "") + (r.stderr or "")
    check("code.text" in out and "inputs" in out,
          "an incomplete header is refused by field name", out.strip().splitlines()[0][:80] if out.strip() else "")

    # ── 9. the audit trail is recorded without moving the identity ──────────
    r = run("provenance", "rerun", "--header", r_hdr, "--db", r_db,
            "--record", "--by", "acceptance-script", "--print-verdict")
    check(r.returncode == 0, "rerun --record exits 0", (r.stderr or "").strip()[:120])
    with open(r_hdr, encoding="utf-8") as f:
        after = json.load(f)
    reps = after.get("replications", [])
    check(len(reps) == 1 and reps[0]["level"] == "rerun" and reps[0]["verdict"] == "pass",
          "the rerun is in the audit trail", json.dumps(reps, ensure_ascii=False)[:120])
    check(after["artifact_id"] == id_produced,
          "recording an audit entry does not change the artifact_id")
    r = run("provenance", "rerun", "--header", r_hdr, "--db", r_db, "--print-verdict")
    check((r.stdout or "").strip() == "pass",
          "a second rerun still passes after the audit record was added")

    # ── 10. hand-edited shipped table ⇒ the rerun must NOT report a clean pass ──
    # The code still reproduces the header, so a hash-only comparison would say
    # "pass" while the table in the database is wrong. This is the regression
    # guard for exactly that trap.
    x_db = os.path.join(root, "edited_table", "analysis.duckdb")
    os.makedirs(os.path.dirname(x_db), exist_ok=True)
    shutil.copyfile(p_db, x_db)
    con = duckdb.connect(x_db, config={"storage_compatibility_version": "v1.2.0"})
    con.execute("UPDATE out_region SET n = 99 WHERE region='east'")
    con.close()
    x_hdr = os.path.join(os.path.dirname(x_db), os.path.basename(p_hdr))
    shutil.copyfile(p_hdr, x_hdr)
    r = run("provenance", "rerun", "--header", x_hdr, "--db", x_db)
    out = r.stdout or ""
    check(r.returncode != 0 and "DIFF" in out and "altered after the fact" in out,
          "a hand-edited shipped table is reported as DIFF, not PASS", f"exit={r.returncode}")
    check("column   n" in out, "the hand edit is localised to the edited column")

    print(f"\n{'ALL PASS' if not fails else 'FAILURES: ' + ', '.join(fails)}")
    print(f"workdir: {root}")
    return 0 if not fails else 1


if __name__ == "__main__":
    sys.exit(main())