import asyncio
import json
import os
import re
import struct
import threading
from dataclasses import dataclass, field
from typing import Optional

import pyodbc
from azure.identity import InteractiveBrowserCredential

SQL_COPT_SS_ACCESS_TOKEN = 1256
AAD_SCOPE = "https://database.windows.net/.default"
DRIVER = "{ODBC Driver 18 for SQL Server}"

# ─────────────────────────────────────────────────────────────────────
# Instance config
# ─────────────────────────────────────────────────────────────────────

@dataclass
class InstanceConfig:
    name: str
    host: str
    port: int = 1433
    auth: str = "aad"           # "aad" | "sql"
    user: Optional[str] = None
    password: Optional[str] = None
    database: str = "master"


def load_instances() -> dict[str, InstanceConfig]:
    raw = os.getenv("INSTANCES")
    if raw:
        items = json.loads(raw)
        return {
            i["name"]: InstanceConfig(
                name=i["name"],
                host=i["host"],
                port=i.get("port", 1433),
                auth=i.get("auth", "aad"),
                user=i.get("user"),
                password=i.get("password"),
                database=i.get("database", "master"),
            )
            for i in items
        }

    # Single-instance fallback
    return {
        "default": InstanceConfig(
            name="default",
            host=os.getenv("SQL_SERVER", "sqlserver"),
            port=int(os.getenv("SQL_PORT", "1433")),
            auth=os.getenv("SQL_AUTH", "aad"),
            user=os.getenv("SQL_USER"),
            password=os.getenv("SQL_PASSWORD"),
        )
    }


_instances: dict[str, InstanceConfig] = {}


def init_instances() -> None:
    global _instances
    _instances = load_instances()
    print(f"[db] Registered instances: {', '.join(_instances)}")


def list_instances() -> list[InstanceConfig]:
    return list(_instances.values())


def get_instance(name: str) -> Optional[InstanceConfig]:
    return _instances.get(name)


# ─────────────────────────────────────────────────────────────────────
# Auth state — in-memory only, cleared on restart
# ─────────────────────────────────────────────────────────────────────

@dataclass
class AuthState:
    credential: Optional[InteractiveBrowserCredential] = None
    auth_complete: threading.Event = field(default_factory=threading.Event)
    device_code_info: Optional[dict] = None
    error: Optional[str] = None


_auth_states: dict[str, AuthState] = {}
_auth_lock = threading.Lock()


class PendingAuthError(Exception):
    """Raised when AAD auth has started but browser auth not yet completed."""
    def __init__(self, device_code_info: Optional[dict]):
        self.device_code_info = device_code_info
        super().__init__("Authentication pending")


def _start_browser_auth(instance_name: str) -> AuthState:
    state = AuthState()

    # Set the message immediately — browser opens as soon as get_token() is called
    state.device_code_info = {
        "message": (
            f"A browser window opened on the server machine to authenticate instance '{instance_name}'. "
            "Complete the sign-in there, then retry this tool call."
        ),
    }

    state.credential = InteractiveBrowserCredential(
        tenant_id="organizations",      # auto-discovers tenant from login
        # No cache_persistence_options — in-memory only
    )

    def fetch_token():
        try:
            state.credential.get_token(AAD_SCOPE)  # opens browser, blocks until user completes
        except Exception as exc:
            state.error = str(exc)
        finally:
            state.auth_complete.set()

    threading.Thread(target=fetch_token, daemon=True).start()
    # Browser opens immediately on the server machine — no code to wait for
    return state


def _get_or_start_auth(instance_name: str) -> AuthState:
    with _auth_lock:
        state = _auth_states.get(instance_name)
        if state is None:
            state = _start_browser_auth(instance_name)
            _auth_states[instance_name] = state
        return state


def _clear_auth(instance_name: str) -> None:
    with _auth_lock:
        _auth_states.pop(instance_name, None)


# ─────────────────────────────────────────────────────────────────────
# Connection
# ─────────────────────────────────────────────────────────────────────

def _pack_token(token_str: str) -> bytes:
    b = token_str.encode("utf-16-le")
    return struct.pack(f"<I{len(b)}s", len(b), b)


def _open_connection(inst: InstanceConfig) -> pyodbc.Connection:
    base = (
        f"DRIVER={DRIVER};"
        f"SERVER={inst.host},{inst.port};"
        f"DATABASE={inst.database};"
        "Encrypt=yes;TrustServerCertificate=yes;"
    )

    if inst.auth == "sql":
        return pyodbc.connect(base + f"UID={inst.user};PWD={inst.password};")

    # AAD device code flow
    state = _get_or_start_auth(inst.name)

    if not state.auth_complete.is_set():
        raise PendingAuthError(state.device_code_info)

    if state.error:
        _clear_auth(inst.name)      # reset so next call restarts auth
        raise RuntimeError(f"Authentication failed: {state.error}")

    # Silent refresh — azure-identity handles expiry automatically
    token = state.credential.get_token(AAD_SCOPE)
    return pyodbc.connect(base, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: _pack_token(token.token)})


# ─────────────────────────────────────────────────────────────────────
# Query execution
# ─────────────────────────────────────────────────────────────────────

_TOP_RE = re.compile(r"\bTOP\s*\(", re.IGNORECASE)
_ROWCOUNT_RE = re.compile(r"\bSET\s+ROWCOUNT\b", re.IGNORECASE)


def _run_query(inst: InstanceConfig, sql: str, max_rows: int) -> dict:
    conn = _open_connection(inst)
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        if cursor.description is None:
            return {"rows": [], "truncated": False}
        cols = [col[0] for col in cursor.description]
        rows: list[dict] = []
        truncated = False
        for row in cursor:
            if len(rows) >= max_rows:
                truncated = True
                break
            rows.append(dict(zip(cols, row)))
        return {"rows": rows, "truncated": truncated}
    finally:
        conn.close()


async def query_instance(instance_name: str, sql: str, max_rows: int = 200) -> dict:
    inst = _instances.get(instance_name)
    if not inst:
        available = ", ".join(_instances)
        raise ValueError(f'Unknown instance "{instance_name}". Available: {available}')
    # pyodbc is blocking — run in thread pool to not block the event loop
    return await asyncio.to_thread(_run_query, inst, sql, max_rows)
