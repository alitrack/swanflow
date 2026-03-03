import json
import sys

def main():
    while True:
        line = sys.stdin.readline()
        if not line:
            break
        try:
            request = json.loads(line)
            method = request.get("method")
            req_id = request.get("id")

            if method == "initialize":
                response = {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": "mock-server", "version": "1.0"}
                    }
                }
            elif method == "tools/call":
                tool_name = request["params"]["name"]
                args = request["params"]["arguments"]
                if tool_name == "echo":
                    response = {
                        "jsonrpc": "2.0",
                        "id": req_id,
                        "result": {
                            "content": [{"type": "text", "text": f"Echo: {args.get('msg', '')}"}]
                        }
                    }
                else:
                    response = {
                        "jsonrpc": "2.0",
                        "id": req_id,
                        "error": {"code": -32601, "message": "Method not found"}
                    }
            else:
                # Notifications like initialized
                continue

            sys.stdout.write(json.dumps(response) + "\n")
            sys.stdout.flush()
        except Exception as e:
            sys.stderr.write(f"Error: {str(e)}\n")
            sys.stderr.flush()

if __name__ == "__main__":
    main()
