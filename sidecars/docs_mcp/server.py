#!/usr/bin/env python3
"""swanflow-docs-mcp: Bruin-style docs MCP server for the SwanFlow repo.

Tools (all read-only, documentation only — no execution, like `bruin mcp`):
  - swanflow_get_overview      : README.md content
  - swanflow_get_docs_tree     : full markdown tree under docs/ + tutorials/
  - swanflow_get_doc_content   : any single doc file by relative path

Protocol: JSON-RPC 2.0 over stdio (MCP 2024-11-05: initialize/initialized/tools/list/tools/call).
Run: python3 swanflow_docs_mcp.py --repo /path/to/swanflow
"""
import argparse
import json
import os
import sys

PROTO_VERSION = "2024-11-05"
SERVER_INFO = {"name": "swanflow-docs-mcp", "version": "0.1.0"}
DOC_DIRS = ("docs", "tutorials")
ROOT_DOCS = ("README.md", "TUTORIALS.md", "CONTRIBUTING.md")

g_repo = None


def doc_files():
    out = []
    for name in ROOT_DOCS:
        p = os.path.join(g_repo, name)
        if os.path.isfile(p):
            out.append(name)
    for d in DOC_DIRS:
        base = os.path.join(g_repo, d)
        for root, _dirs, files in os.walk(base):
            for f in sorted(files):
                if f.endswith((".md", ".yaml", ".yml", ".toml")):
                    out.append(os.path.relpath(os.path.join(root, f), g_repo))
    return sorted(out)


def read_doc(rel):
    if rel not in doc_files():
        return None, "unknown doc: %r (use swanflow_get_docs_tree to list valid files)" % rel
    p = os.path.join(g_repo, rel)
    try:
        with open(p, encoding="utf-8") as fh:
            return fh.read(), None
    except OSError as e:
        return None, "read error: %s" % e


def tool_overview(_args):
    text, err = read_doc("README.md")
    if err:
        return {"content": [{"type": "text", "text": err}], "isError": True}
    return {"content": [{"type": "text", "text": text}]}


def tool_tree(_args):
    files = doc_files()
    tree = "\n".join(files)
    return {"content": [{"type": "text", "text": "SwanFlow docs (%d files):\n%s" % (len(files), tree)}]}


def tool_content(args):
    rel = (args or {}).get("filename", "")
    text, err = read_doc(rel)
    if err:
        return {"content": [{"type": "text", "text": err}], "isError": True}
    return {"content": [{"type": "text", "text": text}]}


TOOLS = [
    {
        "name": "swanflow_get_overview",
        "description": "Read the SwanFlow README: what the engine is, capabilities, quickstart.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "swanflow_get_docs_tree",
        "description": "List every documentation file in the SwanFlow repo (docs/, tutorials/, root md).",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "swanflow_get_doc_content",
        "description": "Read one SwanFlow doc file by repo-relative path (e.g. 'docs/adr/ADR-004-duckdb-mandatory.md').",
        "inputSchema": {
            "type": "object",
            "properties": {"filename": {"type": "string", "description": "repo-relative path from swanflow_get_docs_tree"}},
            "required": ["filename"],
            "additionalProperties": False,
        },
    },
]

HANDLERS = {
    "swanflow_get_overview": tool_overview,
    "swanflow_get_docs_tree": tool_tree,
    "swanflow_get_doc_content": tool_content,
}


def result(req_id, payload):
    return {"jsonrpc": "2.0", "id": req_id, "result": payload}


def error(req_id, code, message):
    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}


def handle(msg):
    method = msg.get("method", "")
    req_id = msg.get("id")
    if method == "initialize":
        return result(req_id, {
            "protocolVersion": PROTO_VERSION,
            "capabilities": {"tools": {}},
            "serverInfo": SERVER_INFO,
        })
    if method == "notifications/initialized" or method == "initialized":
        return None
    if method == "tools/list":
        return result(req_id, {"tools": TOOLS})
    if method == "tools/call":
        params = msg.get("params") or {}
        name = params.get("name", "")
        fn = HANDLERS.get(name)
        if fn is None:
            return error(req_id, -32602, "unknown tool: %s" % name)
        try:
            return result(req_id, fn(params.get("arguments") or {}))
        except Exception as e:  # noqa: BLE001
            return result(req_id, {"content": [{"type": "text", "text": "handler error: %s" % e}], "isError": True})
    if method == "ping":
        return result(req_id, {})
    return error(req_id, -32601, "method not found: %s" % method)


def main():
    global g_repo
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True, help="path to the swanflow repo")
    ns = ap.parse_args()
    g_repo = os.path.abspath(ns.repo)
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            resp = error(None, -32700, "parse error")
        else:
            resp = handle(msg)
        if resp is not None:
            sys.stdout.write(json.dumps(resp) + "\n")
            sys.stdout.flush()


if __name__ == "__main__":
    main()
