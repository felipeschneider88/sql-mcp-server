# SQL MCP Server — Python

MCP server for Azure SQL / SQL Server DBA tooling. Supports Azure AD MFA (device code) and SQL auth. HTTP transport compatible with Claude Code, VS Code, and Claude Desktop.

---

## Requirements

- Python 3.12+
- [ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)

---

## Install

```bash
cd sql-mcp-server-py
pip install --target=./packages -r requirements.txt
python server.py
```

`server.py` automatically adds `./packages` to `sys.path` if the folder exists, so no environment variables or activation needed.

> **Why `--target`?** Corporate environments with roaming profiles or group policies that block `venv` need a local install folder instead. The `--target` flag installs directly into the project directory, isolated from system/roaming packages.

> **If your machine allows venv** (and you prefer it):
> ```bash
> python -m venv venv
> venv\Scripts\activate
> pip install -r requirements.txt
> python server.py
> ```

---

## Configure

```bash
cp .env.example .env
```

Edit `.env`:

### Multi-instance (recommended)

```env
PORT=3000
INSTANCES=[
  {"name":"prod","host":"prod.database.windows.net","auth":"aad"},
  {"name":"dev","host":"dev-sql","auth":"sql","user":"dba_monitor","password":"secret"}
]
```

`auth` values:
| Value | Method |
|-------|--------|
| `aad` | Azure AD MFA — device code, tenant auto-discovered |
| `sql` | SQL Server auth — `user` + `password` required |

### Single-instance fallback

```env
PORT=3000
SQL_SERVER=prod.database.windows.net
SQL_PORT=1433
SQL_AUTH=aad
```

---

## Run

```bash
python server.py
```

Output:
```
[db] Registered instances: prod, dev
INFO:     Started server process
INFO:     Uvicorn running on http://0.0.0.0:3000
```

---

## Connect to Claude Code / VS Code

Add to your MCP config (`.claude/mcp.json` or VS Code `settings.json`):

```json
{
  "mcpServers": {
    "sql-dba": {
      "type": "http",
      "url": "http://localhost:3000/mcp"
    }
  }
}
```

For Claude Code CLI, you can also run:
```bash
claude mcp add sql-dba --transport http http://localhost:3000/mcp
```

---

## Azure AD MFA flow (on demand)

Auth is triggered the first time a tool is called on an AAD instance. No pre-authentication at startup. Uses interactive browser auth — the server machine's default browser opens automatically.

**Step 1** — Call any tool (e.g. `get_active_sessions` on `prod`). Server returns:

```json
{
  "auth_required": true,
  "message": "A browser window opened on the server machine to authenticate instance 'prod'. Complete the sign-in there, then retry this tool call.",
  "instructions": "Complete authentication in your browser, then retry this tool call."
}
```

**Step 2** — A browser window opens automatically on the machine running the server. Sign in with your Azure AD account (MFA completes normally in the browser).

**Step 3** — Retry the original tool call. Token is now cached in memory.

> **Note:** Token lives only in memory. Server restart requires re-authentication.
> The server must be running on a machine where you can interact with a browser (local machine or RDP session).

---

## Available tools

| Tool | Description |
|------|-------------|
| `list_instances_tool` | List all configured instances — call this first if unsure of instance name |
| `execute_query` | Run a read-only T-SQL SELECT against any instance |
| `get_active_sessions` | Active sessions with CPU, blocking, and current SQL text |

---

## Health check

No dedicated health endpoint. Verify the server is up:

```bash
curl -s http://localhost:3000/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}},"id":1}'
```

---

## Troubleshooting

**`ODBC Driver 18 for SQL Server` not found**
Install it from Microsoft: https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server

Check available drivers:
```python
import pyodbc; print(pyodbc.drivers())
```

**Azure AD auth — code never appears**
The device code is returned as part of the tool call response, not printed to the console. Claude surfaces it in the chat. If using a raw client, parse the `auth_required: true` JSON response.

**Auth failed / expired**
The auth state is cleared automatically on failure. The next tool call restarts the device code flow from scratch.

**`ModuleNotFoundError: No module named 'mcp'`**
```bash
pip install mcp[cli]
```

**Port already in use**
Change `PORT` in `.env` and update the MCP client config accordingly.
