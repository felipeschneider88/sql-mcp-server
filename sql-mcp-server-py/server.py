import os
import sys

# Allow local package install: pip install --target=./packages -r requirements.txt
_local_packages = os.path.join(os.path.dirname(__file__), "packages")
if os.path.isdir(_local_packages) and _local_packages not in sys.path:
    sys.path.insert(0, _local_packages)

import uvicorn
from datetime import datetime, timezone
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

from connection import init_instances, list_instances
from tools import register_tools

load_dotenv()
init_instances()

PORT = int(os.getenv("PORT", "3000"))

mcp = FastMCP("sql-server-dba")
register_tools(mcp)


async def health(_: Request) -> JSONResponse:
    return JSONResponse({
        "status": "ok",
        "server": "sql-server-dba-mcp",
        "instances": [i.name for i in list_instances()],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })


# VS Code / Claude Code mcp.json:
#   { "sql-dba": { "type": "http", "url": "http://localhost:3000/mcp" } }

if __name__ == "__main__":
    app = Starlette(routes=[
        Route("/health", endpoint=health),
        Mount("/", app=mcp.streamable_http_app()),
    ])
    uvicorn.run(app, host="0.0.0.0", port=PORT)
