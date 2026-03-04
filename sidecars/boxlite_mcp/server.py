import asyncio
import os
import json
import boxlite
from mcp.server.fastmcp import FastMCP

# Initialize FastMCP server
mcp = FastMCP("BoxLite Sandbox")

@mcp.tool()
async def run_code(code: str, image: str = "python:3.11-slim", timeout: int = 60) -> str:
    """
    Executes Python code inside a hardware-isolated BoxLite micro-VM.
    
    :param code: The Python source code to execute.
    :param image: The OCI/Docker image to use as base (default python:3.11-slim).
    :param timeout: Maximum execution time in seconds.
    """
    print(f"[BoxLite] Starting sandbox with image: {image}")
    
    try:
        async with boxlite.SimpleBox(image) as box:
            # We wrap the code in a script and run it
            # In a more advanced version, we'd use the bootstrap script here
            result = await asyncio.wait_for(
                box.exec("python3", "-c", code),
                timeout=timeout
            )
            
            return json.dumps({
                "stdout": result.stdout,
                "stderr": result.stderr,
                "exit_code": result.exit_code,
                "status": "success"
            })
    except asyncio.TimeoutError:
        return json.dumps({
            "error": "Execution timed out",
            "status": "timeout"
        })
    except Exception as e:
        return json.dumps({
            "error": str(e),
            "status": "error"
        })

if __name__ == "__main__":
    mcp.run()
