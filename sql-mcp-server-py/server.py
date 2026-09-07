import os

import uvicorn
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

from connection import init_instances
from tools import register_tools

load_dotenv()
init_instances()

PORT = int(os.getenv("PORT", "3000"))

mcp = FastMCP("sql-server-dba")
register_tools(mcp)

# VS Code / Claude Code mcp.json:
#   { "sql-dba": { "type": "http", "url": "http://localhost:3000/mcp" } }

if __name__ == "__main__":
    app = mcp.streamable_http_app()
    uvicorn.run(app, host="0.0.0.0", port=PORT)
