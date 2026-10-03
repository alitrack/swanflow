#!/usr/bin/env python3
"""Zero-dependency stdio probe for swanflow-docs-mcp (like testing `bruin mcp`)."""
import json
import os
import subprocess
import sys

PROC = [sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), "server.py"), "--repo", "/mnt/d/wsl2/swanflow"]


def rpc(p, msg):
    p.stdin.write(json.dumps(msg) + "\n")
    p.stdin.flush()
    line = p.stdout.readline()
    return json.loads(line)


def main():
    p = subprocess.Popen(PROC, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    checks = []

    # 1. initialize
    r = rpc(p, {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}})
    ok = r["result"]["serverInfo"]["name"] == "swanflow-docs-mcp" and r["result"]["protocolVersion"] == "2024-11-05"
    checks.append(("initialize", ok, r["result"]["serverInfo"]))

    # 2. initialized notification (no response expected)
    p.stdin.write(json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}) + "\n")
    p.stdin.flush()

    # 3. tools/list
    r = rpc(p, {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})
    names = [t["name"] for t in r["result"]["tools"]]
    checks.append(("tools/list 3 tools", sorted(names) == sorted([
        "swanflow_get_overview", "swanflow_get_docs_tree", "swanflow_get_doc_content"]), names))
    # schema sanity: unknown-arg rejection surface exists
    schema = r["result"]["tools"][2]["inputSchema"]
    checks.append(("doc_content schema requires filename", schema["required"] == ["filename"], ""))

    # 4. overview returns real README content
    r = rpc(p, {"jsonrpc": "2.0", "id": 3, "method": "tools/call",
                "params": {"name": "swanflow_get_overview", "arguments": {}}})
    text = r["result"]["content"][0]["text"]
    checks.append(("overview contains SwanFlow README", "Rust-native Data Science DAG Engine" in text, len(text)))

    # 5. docs tree lists files
    r = rpc(p, {"jsonrpc": "2.0", "id": 4, "method": "tools/call",
                "params": {"name": "swanflow_get_docs_tree", "arguments": {}}})
    text = r["result"]["content"][0]["text"]
    checks.append(("tree has ADR-004", "docs/adr/ADR-004-duckdb-mandatory.md" in text, text.splitlines()[0]))

    # 6. single doc read
    r = rpc(p, {"jsonrpc": "2.0", "id": 5, "method": "tools/call",
                "params": {"name": "swanflow_get_doc_content",
                           "arguments": {"filename": "docs/adr/ADR-004-duckdb-mandatory.md"}}})
    text = r["result"]["content"][0]["text"]
    checks.append(("read ADR-004 content", "DuckDB" in text and len(text) > 200, len(text)))

    # 7. error path: unknown doc must NOT be empty success
    r = rpc(p, {"jsonrpc": "2.0", "id": 6, "method": "tools/call",
                "params": {"name": "swanflow_get_doc_content",
                           "arguments": {"filename": "docs/nonexistent.md"}}})
    checks.append(("unknown doc flagged isError", r["result"].get("isError") is True, ""))

    # 8. error path: unknown tool
    r = rpc(p, {"jsonrpc": "2.0", "id": 7, "method": "tools/call",
                "params": {"name": "delete_everything", "arguments": {}}})
    checks.append(("unknown tool -> -32602", r.get("error", {}).get("code") == -32602, ""))

    p.stdin.close()
    p.wait(timeout=10)

    failed = [c for c in checks if not c[1]]
    for name, ok, info in checks:
        print("%s %s  %s" % ("PASS" if ok else "FAIL", name, info if info else ""))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
