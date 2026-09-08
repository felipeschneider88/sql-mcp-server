import os
import sys

# Allow local package install: pip install --target=./packages -r requirements.txt
_local_packages = os.path.join(os.path.dirname(__file__), "packages")
if os.path.isdir(_local_packages) and _local_packages not in sys.path:
    sys.path.insert(0, _local_packages)

import json
from contextlib import asynccontextmanager

import uvicorn
from datetime import datetime, timezone
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

from connection import init_instances, list_instances
from tools import register_tools, get_tool_registry
from prompts import register_prompts

load_dotenv()
init_instances()

PORT = int(os.getenv("PORT", "3000"))

mcp = FastMCP("sql-server-dba")
register_tools(mcp)
register_prompts(mcp)


async def health(_: Request) -> JSONResponse:
    return JSONResponse({
        "status": "ok",
        "server": "sql-server-dba-mcp",
        "instances": [i.name for i in list_instances()],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })


async def run_list(_: Request) -> JSONResponse:
    """GET /run — list available tools."""
    registry = get_tool_registry()
    return JSONResponse({"tools": sorted(registry.keys())})


async def run_tool(request: Request) -> JSONResponse:
    """
    POST /run — call any registered tool directly over HTTP.

    Body:
      {
        "tool":     "get_active_sessions",
        "instance": "prod",           // shorthand for params.instance_name
        "params":   { "include_sleeping": true }   // optional
      }

    Returns 200 with tool result, 202 when Azure AD auth is pending,
    400 on bad input, 404 for unknown tool.
    """
    registry = get_tool_registry()

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)

    tool_name = body.get("tool")
    if not tool_name:
        return JSONResponse({"error": "Missing 'tool' field"}, status_code=400)

    fn = registry.get(tool_name)
    if fn is None:
        return JSONResponse(
            {"error": f"Unknown tool '{tool_name}'", "available": sorted(registry.keys())},
            status_code=404,
        )

    params: dict = dict(body.get("params") or {})
    # Top-level "instance" key maps to instance_name for convenience
    if "instance" in body and "instance_name" not in params:
        params["instance_name"] = body["instance"]

    try:
        raw = await fn(**params)
    except TypeError as exc:
        return JSONResponse({"error": f"Invalid params: {exc}"}, status_code=400)
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)

    try:
        result = json.loads(raw)
    except Exception:
        result = raw

    if isinstance(result, dict) and result.get("auth_required"):
        return JSONResponse(result, status_code=202)

    return JSONResponse({"tool": tool_name, "result": result})


# VS Code / Claude Code mcp.json:
#   { "sql-dba": { "type": "http", "url": "http://localhost:3000/mcp" } }

if __name__ == "__main__":
    mcp_app = mcp.streamable_http_app()

    # Starlette Mount does not trigger sub-app lifespan; propagate it explicitly
    # so FastMCP's internal task group is initialized before requests arrive.
    @asynccontextmanager
    async def lifespan(app: Starlette):
        async with mcp_app.router.lifespan_context(app):
            yield

    app = Starlette(
        lifespan=lifespan,
        routes=[
            Route("/health", endpoint=health),
            Route("/run",    endpoint=run_list,  methods=["GET"]),
            Route("/run",    endpoint=run_tool,  methods=["POST"]),
            Mount("/", app=mcp_app),
        ],
    )
    uvicorn.run(app, host="0.0.0.0", port=PORT)
