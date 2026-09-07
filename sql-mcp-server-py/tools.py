import json
from datetime import date, datetime
from decimal import Decimal
from typing import Annotated

from pydantic import Field

from connection import PendingAuthError, list_instances, query_instance
from safety import validate_query

# ─────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────

def _json_default(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, bytes):
        return obj.hex()
    raise TypeError(f"Not JSON serializable: {type(obj)}")


def to_json(value) -> str:
    return json.dumps(value, default=_json_default, indent=2)


def _auth_required(device_code_info: dict | None) -> str:
    if not device_code_info:
        return json.dumps({
            "auth_required": True,
            "message": "Authentication started. Device code not yet available — retry in a moment.",
        })
    return json.dumps({
        "auth_required": True,
        **device_code_info,
        "instructions": "Complete authentication in your browser, then retry this tool call.",
    }, indent=2)


# ─────────────────────────────────────────────────────────────────────
# Tool registration
# ─────────────────────────────────────────────────────────────────────

def register_tools(mcp) -> None:

    @mcp.tool()
    async def list_instances_tool() -> str:
        """List all configured SQL Server instances. Call this first when instance name is unknown."""
        instances = [
            {"name": i.name, "host": i.host, "port": i.port, "auth": i.auth}
            for i in list_instances()
        ]
        return to_json(instances)

    @mcp.tool()
    async def execute_query(
        query: Annotated[str, Field(description=(
            "Read-only T-SQL SELECT statement to execute. "
            "May include CTEs (WITH ...), CROSS APPLY, sub-queries, DECLARE. "
            "INSERT/UPDATE/DELETE/DDL are rejected."
        ))],
        instance_name: Annotated[str, Field(description=(
            "Named SQL Server instance to query. "
            "Call list_instances_tool to see available names."
        ))] = "default",
    ) -> str:
        """
        Execute a read-only T-SQL SELECT statement against a named instance.
        Use for ad-hoc DMV analysis, custom JOINs, CTEs, and queries the
        pre-built tools don't cover. Connects to master by default.
        """
        ok, reason = validate_query(query)
        if not ok:
            return json.dumps({"error": reason})

        try:
            result = await query_instance(instance_name, query, max_rows=500)
        except PendingAuthError as e:
            return _auth_required(e.device_code_info)
        except ValueError as e:
            return json.dumps({"error": str(e)})
        except Exception as e:
            return json.dumps({"error": str(e)})

        note = (
            "\n\n[Result truncated to 500 rows. Use a more specific WHERE clause.]"
            if result["truncated"]
            else ""
        )
        return to_json(result["rows"]) + note

    @mcp.tool()
    async def get_active_sessions(
        instance_name: Annotated[str, Field(description=(
            "Named SQL Server instance to query. "
            "Call list_instances_tool to see available names."
        ))] = "default",
        include_sleeping: Annotated[bool, Field(description=(
            "Include sleeping/idle sessions. Default false — active requests only."
        ))] = False,
    ) -> str:
        """
        Get all active SQL Server sessions with request details, CPU, blocking status,
        and current SQL text. Best starting point for performance investigations.
        Uses CROSS APPLY dm_exec_sql_text to fetch the actual query being run.
        """
        sleep_filter = (
            ""
            if include_sleeping
            else "AND (r.session_id IS NOT NULL OR s.status NOT IN ('sleeping', 'dormant'))"
        )

        sql = f"""
          SELECT
            s.session_id,
            s.login_name,
            s.host_name,
            s.program_name,
            s.status                                        AS session_status,
            r.status                                        AS request_status,
            r.command,
            r.wait_type,
            r.wait_time                                     AS wait_ms,
            r.blocking_session_id,
            r.total_elapsed_time                            AS elapsed_ms,
            r.cpu_time                                      AS request_cpu_ms,
            s.cpu_time                                      AS session_cpu_ms,
            r.logical_reads                                 AS request_logical_reads,
            s.reads                                         AS session_reads,
            s.writes                                        AS session_writes,
            r.percent_complete,
            DB_NAME(r.database_id)                          AS database_name,
            SUBSTRING(
              t.text,
              (r.statement_start_offset / 2) + 1,
              ((CASE r.statement_end_offset
                  WHEN -1 THEN DATALENGTH(t.text)
                  ELSE r.statement_end_offset
                END - r.statement_start_offset) / 2) + 1
            )                                               AS current_statement,
            s.last_request_start_time,
            s.last_request_end_time
          FROM sys.dm_exec_sessions s
          LEFT JOIN sys.dm_exec_requests r ON s.session_id = r.session_id
          OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) t
          WHERE s.is_user_process = 1
            {sleep_filter}
          ORDER BY
            CASE WHEN r.blocking_session_id > 0 THEN 0 ELSE 1 END,
            COALESCE(r.total_elapsed_time, 0) DESC
        """

        try:
            result = await query_instance(instance_name, sql, max_rows=200)
        except PendingAuthError as e:
            return _auth_required(e.device_code_info)
        except ValueError as e:
            return json.dumps({"error": str(e)})
        except Exception as e:
            return json.dumps({"error": str(e)})

        return to_json({"sessions": result["rows"], "truncated": result["truncated"]})
