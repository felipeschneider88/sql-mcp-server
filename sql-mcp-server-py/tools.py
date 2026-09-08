import asyncio
import json
import time
from datetime import date, datetime
from decimal import Decimal
from typing import Annotated

from pydantic import Field

from connection import PendingAuthError, list_instances, open_connection, query_instance
from safety import validate_query

# Wait types that indicate idle/background activity — not user-facing performance problems.
# Used by both get_wait_stats (delta) and get_wait_stats_since_restart (cumulative).
_BENIGN_WAITS = [
    "SLEEP_TASK","SLEEP_SYSTEMTASK","SLEEP_DBSTARTUP","SLEEP_DBTASK",
    "SLEEP_TEMPDBSTARTUP","SLEEP_MASTERDBREADY","SLEEP_MASTERMDREADY",
    "SLEEP_MASTERUPGRADED","SLEEP_MSDBSTARTUP","SLEEP_REPLICATION_MONITOR",
    "SLEEP_DCOMSTARTUP",
    "BROKER_EVENTHANDLER","BROKER_RECEIVE_WAITFOR","BROKER_TASK_STOP",
    "BROKER_TO_FLUSH","BROKER_TRANSMITTER","BROKER_IOCP",
    "CHECKPOINT_QUEUE",
    "CLR_AUTO_EVENT","CLR_MANUAL_EVENT","CLR_SEMAPHORE",
    "DBMIRROR_DBM_EVENT","DBMIRROR_DBM_MUTEX","DBMIRROR_EVENTS_QUEUE",
    "DBMIRROR_WORKER_QUEUE","DBMIRRORING_CMD","DBMIRROR_SEND",
    "DIRTY_PAGE_POLL","DISPATCHER_QUEUE_SEMAPHORE",
    "FT_IFTS_SCHEDULER_IDLE_WAIT","FT_IFTSHC_MUTEX",
    "HADR_CLUSAPI_CALL","HADR_FABRIC_CALLBACK",
    "HADR_FILESTREAM_IOMGR_IOCOMPLETION","HADR_LOGCAPTURE_WAIT",
    "HADR_WORK_QUEUE",
    "LAZYWRITER_SLEEP","LOGMGR_QUEUE",
    "ONDEMAND_TASK_QUEUE",
    "PARALLEL_REDO_DRAIN_WORKER","PARALLEL_REDO_LOG_CACHE",
    "PARALLEL_REDO_TRAN_LIST","PARALLEL_REDO_TRAN_TURN",
    "PARALLEL_REDO_WORKER_SYNC","PARALLEL_REDO_WORKER_WAIT_WORK",
    "POPULATE_LOCK_ORDINALS",
    "PREEMPTIVE_HADR_LEASE_MECHANISM","PREEMPTIVE_OS_FLUSHFILEBUFFERS",
    "PREEMPTIVE_SP_SERVER_DIAGNOSTICS","PREEMPTIVE_OS_GETQUEUEDCOMPLETIONSTATUS",
    "PVS_PREALLOCATE","PWAIT_EXTENSIBILITY_CLEANUP_TASK",
    "QDS_ASYNC_QUEUE",
    "QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP",
    "QDS_PERSIST_TASK_MAIN_LOOP_SLEEP","QDS_SHUTDOWN_QUEUE",
    "REDO_THREAD_PENDING_WORK","REQUEST_FOR_DEADLOCK_SEARCH",
    "RESOURCE_QUEUE","SERVER_IDLE_CHECK","SNI_HTTP_ACCEPT",
    "SOS_WORK_DISPATCHER","SP_SERVER_DIAGNOSTICS_SLEEP",
    "SQLTRACE_BUFFER_FLUSH","SQLTRACE_INCREMENTAL_FLUSH_SLEEP","SQLTRACE_WAIT_ENTRIES",
    "UCS_SESSION_REGISTRATION",
    "WAIT_XTP_OFFLINE_CKPT_NEW_LOG","WAIT_XTP_ONLINE_CKPT_NEW_LOG",
    "WAITFOR",
    "XE_DISPATCHER_WAIT","XE_LIVE_TARGET_TVF","XE_TIMER_EVENT",
]

_BENIGN_IN = ", ".join(f"'{w}'" for w in _BENIGN_WAITS)


def _run_wait_delta(instance_name: str, window_seconds: int, top_n: int) -> dict:
    """
    Snapshot sys.dm_os_wait_stats into a temp table, sleep window_seconds,
    then return the delta — waits accumulated only during that window.
    Runs entirely in one connection so the temp table survives across statements.
    Blocking here is intentional and runs in asyncio.to_thread().
    """
    sql_baseline = f"""
        SELECT wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms
        INTO #WaitBaseline
        FROM sys.dm_os_wait_stats
        WHERE wait_type NOT IN ({_BENIGN_IN})
          AND wait_time_ms > 0
    """
    sql_delta = f"""
        SELECT TOP {top_n}
            c.wait_type,
            c.waiting_tasks_count - ISNULL(b.waiting_tasks_count, 0) AS delta_tasks,
            c.wait_time_ms        - ISNULL(b.wait_time_ms, 0)        AS delta_wait_ms,
            c.signal_wait_time_ms - ISNULL(b.signal_wait_time_ms, 0) AS delta_signal_ms,
            CAST(
                100.0 * (c.wait_time_ms - ISNULL(b.wait_time_ms, 0))
                / NULLIF(SUM(c.wait_time_ms - ISNULL(b.wait_time_ms, 0)) OVER (), 0)
            AS DECIMAL(5, 2))                                         AS pct_of_total
        FROM sys.dm_os_wait_stats c
        LEFT JOIN #WaitBaseline b ON b.wait_type = c.wait_type
        WHERE c.wait_time_ms > ISNULL(b.wait_time_ms, 0)
          AND c.wait_type NOT IN ({_BENIGN_IN})
        ORDER BY delta_wait_ms DESC
    """
    conn = open_connection(instance_name)
    try:
        cur = conn.cursor()
        cur.execute(sql_baseline)
        time.sleep(window_seconds)
        cur.execute(sql_delta)
        cols = [col[0] for col in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        return {"rows": rows, "window_seconds": window_seconds}
    finally:
        conn.close()

_registry: dict = {}


def get_tool_registry() -> dict:
    return _registry


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

    # ─────────────────────────────────────────────────────────────────────

    @mcp.tool()
    async def get_wait_stats(
        instance_name: Annotated[str, Field(description=(
            "Named SQL Server instance to query. Call list_instances_tool to see available names."
        ))] = "default",
        window_seconds: Annotated[int, Field(description=(
            "Observation window in seconds. Snapshots wait stats before and after, "
            "returns only waits that accumulated during this window. Default 10, max 60."
        ), ge=2, le=60)] = 10,
        top_n: Annotated[int, Field(description=(
            "Number of wait types to return, ranked by delta_wait_ms. Default 10, max 30."
        ), ge=1, le=30)] = 10,
    ) -> str:
        """
        Get wait statistics accumulated during a live observation window (snapshot delta).
        Takes a baseline, waits window_seconds, then returns only the waits that grew
        during that period — immune to historical noise from restart-time or past incidents.
        Use this during active incidents. For historical trending use get_wait_stats_since_restart.
        """
        try:
            result = await asyncio.to_thread(
                _run_wait_delta, instance_name, window_seconds, top_n
            )
        except PendingAuthError as e:
            return _auth_required(e.device_code_info)
        except ValueError as e:
            return json.dumps({"error": str(e)})
        except Exception as e:
            return json.dumps({"error": str(e)})

        return to_json({
            "wait_stats_delta": result["rows"],
            "window_seconds": result["window_seconds"],
            "note": "Only waits that grew during the observation window are shown.",
        })

    @mcp.tool()
    async def get_wait_stats_since_restart(
        instance_name: Annotated[str, Field(description=(
            "Named SQL Server instance to query. Call list_instances_tool to see available names."
        ))] = "default",
        exclude_benign: Annotated[bool, Field(description=(
            "Exclude known idle/background waits to focus on actionable waits. Default true."
        ))] = True,
    ) -> str:
        """
        Get cumulative wait statistics since last SQL Server restart.
        Useful for trending and identifying chronic patterns, but skewed by historical load.
        For live incident triage use get_wait_stats (snapshot delta) instead.
        Key signals: PAGEIOLATCH_* = disk I/O; LCK_* = lock contention;
        CXPACKET/CXCONSUMER = parallelism; SOS_SCHEDULER_YIELD = CPU pressure;
        RESOURCE_SEMAPHORE = memory grants.
        """
        benign_filter = f"AND wait_type NOT IN ({_BENIGN_IN})" if exclude_benign else ""

        sql = f"""
          SELECT
            wait_type,
            waiting_tasks_count,
            wait_time_ms,
            max_wait_time_ms,
            signal_wait_time_ms,
            wait_time_ms - signal_wait_time_ms              AS resource_wait_time_ms,
            CAST(
              100.0 * wait_time_ms / NULLIF(SUM(wait_time_ms) OVER (), 0)
            AS DECIMAL(6, 2))                               AS pct_total
          FROM sys.dm_os_wait_stats
          WHERE wait_time_ms > 0
            {benign_filter}
          ORDER BY wait_time_ms DESC
        """

        try:
            result = await query_instance(instance_name, sql, max_rows=200)
        except PendingAuthError as e:
            return _auth_required(e.device_code_info)
        except ValueError as e:
            return json.dumps({"error": str(e)})
        except Exception as e:
            return json.dumps({"error": str(e)})

        return to_json({"wait_stats": result["rows"], "benign_waits_excluded": exclude_benign})

    # ─────────────────────────────────────────────────────────────────────

    @mcp.tool()
    async def get_top_queries(
        instance_name: Annotated[str, Field(description=(
            "Named SQL Server instance to query. Call list_instances_tool to see available names."
        ))] = "default",
        order_by: Annotated[str, Field(description=(
            "Metric to rank by: cpu (worker time), reads (logical reads), "
            "writes, elapsed, memory (grant KB), or executions. Default: cpu."
        ))] = "cpu",
        top_n: Annotated[int, Field(description=(
            "Number of queries to return. Default 20, max 100."
        ), ge=1, le=100)] = 20,
    ) -> str:
        """
        Get the most expensive queries from the plan cache since last SQL Server restart.
        Use to find worst offenders for CPU, logical I/O, elapsed time, memory, or execution count.
        """
        order_map = {
            "cpu":        "qs.total_worker_time DESC",
            "reads":      "qs.total_logical_reads DESC",
            "writes":     "qs.total_logical_writes DESC",
            "elapsed":    "qs.total_elapsed_time DESC",
            "memory":     "qs.total_grant_kb DESC",
            "executions": "qs.execution_count DESC",
        }
        if order_by not in order_map:
            return json.dumps({"error": f"Invalid order_by '{order_by}'. Choose from: {', '.join(order_map)}"})

        sql = f"""
          SELECT TOP ({top_n})
            qs.execution_count,
            qs.total_worker_time / 1000                     AS total_cpu_ms,
            qs.total_worker_time / qs.execution_count / 1000 AS avg_cpu_ms,
            qs.total_elapsed_time / 1000                    AS total_elapsed_ms,
            qs.total_elapsed_time / qs.execution_count / 1000 AS avg_elapsed_ms,
            qs.total_logical_reads,
            qs.total_logical_reads / qs.execution_count     AS avg_logical_reads,
            qs.total_physical_reads,
            qs.total_logical_writes,
            COALESCE(qs.total_grant_kb, 0)                  AS total_grant_kb,
            COALESCE(qs.total_grant_kb / NULLIF(qs.execution_count, 0), 0) AS avg_grant_kb,
            COALESCE(qs.total_rows / NULLIF(qs.execution_count, 0), 0)     AS avg_rows,
            DB_NAME(t.dbid)                                 AS database_name,
            OBJECT_NAME(t.objectid, t.dbid)                 AS object_name,
            qs.creation_time,
            qs.last_execution_time,
            SUBSTRING(
              t.text,
              (qs.statement_start_offset / 2) + 1,
              ((CASE qs.statement_end_offset
                  WHEN -1 THEN DATALENGTH(t.text)
                  ELSE qs.statement_end_offset
                END - qs.statement_start_offset) / 2) + 1
            )                                               AS query_text
          FROM sys.dm_exec_query_stats qs
          OUTER APPLY sys.dm_exec_sql_text(qs.sql_handle) t
          WHERE t.text IS NOT NULL
          ORDER BY {order_map[order_by]}
        """

        try:
            result = await query_instance(instance_name, sql, max_rows=top_n)
        except PendingAuthError as e:
            return _auth_required(e.device_code_info)
        except ValueError as e:
            return json.dumps({"error": str(e)})
        except Exception as e:
            return json.dumps({"error": str(e)})

        return to_json({"top_queries": result["rows"], "ordered_by": order_by})

    # ─────────────────────────────────────────────────────────────────────

    @mcp.tool()
    async def get_long_running_transactions(
        instance_name: Annotated[str, Field(description=(
            "Named SQL Server instance to query. Call list_instances_tool to see available names."
        ))] = "default",
        min_duration_seconds: Annotated[int, Field(description=(
            "Minimum transaction duration in seconds to report. Default 60."
        ), ge=0)] = 60,
    ) -> str:
        """
        Get long-running open transactions. Open transactions hold locks, prevent log truncation,
        and cause blocking cascades. Shows transaction age, log bytes used, lock count,
        and current SQL text.
        """
        sql = f"""
          SELECT
            st.session_id,
            s.login_name,
            s.host_name,
            s.program_name,
            DB_NAME(sdt.database_id)                      AS database_name,
            at.transaction_id,
            at.name                                       AS transaction_name,
            at.transaction_begin_time,
            DATEDIFF(SECOND, at.transaction_begin_time, GETDATE()) AS duration_seconds,
            CASE at.transaction_type
              WHEN 1 THEN 'Read/write'
              WHEN 2 THEN 'Read-only'
              WHEN 3 THEN 'System'
              WHEN 4 THEN 'Distributed'
              ELSE 'Unknown'
            END                                           AS transaction_type,
            CASE at.transaction_state
              WHEN 0 THEN 'Not initialized'
              WHEN 1 THEN 'Initialized, not started'
              WHEN 2 THEN 'Active'
              WHEN 3 THEN 'Read-only ended'
              WHEN 4 THEN 'Distributed - prepared'
              WHEN 5 THEN 'Distributed - committed'
              WHEN 6 THEN 'Committed'
              WHEN 7 THEN 'Rolling back'
              WHEN 8 THEN 'Rolled back'
              ELSE 'Unknown'
            END                                           AS transaction_state,
            sdt.database_transaction_log_bytes_used / 1048576 AS log_mb_used,
            sdt.database_transaction_log_bytes_reserved / 1048576 AS log_mb_reserved,
            (SELECT COUNT(*)
             FROM sys.dm_tran_locks tl
             WHERE tl.request_session_id = st.session_id
            )                                             AS locks_held,
            r.command,
            r.status                                      AS request_status,
            r.wait_type,
            r.blocking_session_id,
            SUBSTRING(
              sqlt.text,
              (r.statement_start_offset / 2) + 1,
              ((CASE r.statement_end_offset
                  WHEN -1 THEN DATALENGTH(sqlt.text)
                  ELSE r.statement_end_offset
                END - r.statement_start_offset) / 2) + 1
            )                                             AS current_statement
          FROM sys.dm_tran_active_transactions at
          JOIN sys.dm_tran_session_transactions st ON at.transaction_id = st.transaction_id
          JOIN sys.dm_exec_sessions s ON st.session_id = s.session_id
          LEFT JOIN sys.dm_tran_database_transactions sdt
            ON at.transaction_id = sdt.transaction_id
          LEFT JOIN sys.dm_exec_requests r ON st.session_id = r.session_id
          OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) sqlt
          WHERE DATEDIFF(SECOND, at.transaction_begin_time, GETDATE()) >= {min_duration_seconds}
            AND s.is_user_process = 1
          ORDER BY duration_seconds DESC
        """

        try:
            result = await query_instance(instance_name, sql, max_rows=200)
        except PendingAuthError as e:
            return _auth_required(e.device_code_info)
        except ValueError as e:
            return json.dumps({"error": str(e)})
        except Exception as e:
            return json.dumps({"error": str(e)})

        if not result["rows"]:
            return json.dumps({
                "message": f"No transactions running longer than {min_duration_seconds} seconds."
            })
        return to_json({"long_running_transactions": result["rows"]})

    # ─────────────────────────────────────────────────────────────────────

    @mcp.tool()
    async def get_plan_cache_pollution(
        instance_name: Annotated[str, Field(description=(
            "Named SQL Server instance to query. Call list_instances_tool to see available names."
        ))] = "default",
        analysis_type: Annotated[str, Field(description=(
            "What to analyze: 'single_use' (ad-hoc plans wasting memory), "
            "'high_variance' (parameter sniffing candidates), or 'both'. Default: both."
        ))] = "both",
        top_n: Annotated[int, Field(description=(
            "Results per category. Default 30, max 100."
        ), ge=1, le=100)] = 30,
    ) -> str:
        """
        Identify plan cache pollution: single-use ad-hoc plans wasting memory,
        and high-variance queries (parameter sniffing candidates where max elapsed >> min elapsed).
        Works on both Azure SQL and on-prem.
        """
        if analysis_type not in ("single_use", "high_variance", "both"):
            return json.dumps({"error": "analysis_type must be 'single_use', 'high_variance', or 'both'"})

        results: dict = {}

        sql_single = f"""
          SELECT TOP ({top_n})
            DB_NAME(t.dbid)                             AS database_name,
            OBJECT_NAME(t.objectid, t.dbid)             AS object_name,
            cp.size_in_bytes / 1024                     AS plan_size_kb,
            qs.creation_time,
            SUBSTRING(
              t.text,
              (qs.statement_start_offset / 2) + 1,
              ((CASE qs.statement_end_offset
                  WHEN -1 THEN DATALENGTH(t.text)
                  ELSE qs.statement_end_offset
                END - qs.statement_start_offset) / 2) + 1
            )                                           AS query_text
          FROM sys.dm_exec_cached_plans cp
          JOIN sys.dm_exec_query_stats qs ON cp.plan_handle = qs.plan_handle
          OUTER APPLY sys.dm_exec_sql_text(qs.sql_handle) t
          WHERE cp.usecounts = 1
            AND cp.objtype = 'Adhoc'
          ORDER BY cp.size_in_bytes DESC
        """

        sql_variance = f"""
          SELECT TOP ({top_n})
            DB_NAME(t.dbid)                             AS database_name,
            OBJECT_NAME(t.objectid, t.dbid)             AS object_name,
            qs.execution_count,
            qs.min_elapsed_time / 1000                  AS min_elapsed_ms,
            qs.max_elapsed_time / 1000                  AS max_elapsed_ms,
            (qs.max_elapsed_time - qs.min_elapsed_time) / 1000 AS elapsed_variance_ms,
            CAST(
              CASE WHEN qs.min_elapsed_time > 0
                THEN CAST(qs.max_elapsed_time AS FLOAT) / qs.min_elapsed_time
                ELSE 0
              END
            AS DECIMAL(10, 1))                          AS variance_ratio,
            qs.total_worker_time / 1000                 AS total_cpu_ms,
            qs.total_logical_reads,
            qs.last_execution_time,
            SUBSTRING(
              t.text,
              (qs.statement_start_offset / 2) + 1,
              ((CASE qs.statement_end_offset
                  WHEN -1 THEN DATALENGTH(t.text)
                  ELSE qs.statement_end_offset
                END - qs.statement_start_offset) / 2) + 1
            )                                           AS query_text
          FROM sys.dm_exec_query_stats qs
          OUTER APPLY sys.dm_exec_sql_text(qs.sql_handle) t
          WHERE qs.execution_count >= 10
            AND qs.min_elapsed_time > 0
            AND qs.max_elapsed_time >= 1000
            AND CAST(qs.max_elapsed_time AS FLOAT) / qs.min_elapsed_time >= 10
          ORDER BY (qs.max_elapsed_time - qs.min_elapsed_time) * qs.execution_count DESC
        """

        try:
            queries = []
            labels = []
            if analysis_type in ("single_use", "both"):
                queries.append(query_instance(instance_name, sql_single, max_rows=top_n))
                labels.append("single_use_plans")
            if analysis_type in ("high_variance", "both"):
                queries.append(query_instance(instance_name, sql_variance, max_rows=top_n))
                labels.append("high_variance_queries")

            settled = await asyncio.gather(*queries, return_exceptions=True)
            for label, outcome in zip(labels, settled):
                if isinstance(outcome, PendingAuthError):
                    return _auth_required(outcome.device_code_info)
                if isinstance(outcome, Exception):
                    results[label] = {"error": str(outcome)}
                else:
                    results[label] = outcome["rows"]
        except Exception as e:
            return json.dumps({"error": str(e)})

        return to_json(results)

    # ─────────────────────────────────────────────────────────────────────

    @mcp.tool()
    async def get_missing_indexes(
        instance_name: Annotated[str, Field(description=(
            "Named SQL Server instance to query. Call list_instances_tool to see available names."
        ))] = "default",
        min_impact: Annotated[float, Field(description=(
            "Minimum avg_user_impact % threshold. Default 50."
        ), ge=0, le=100)] = 50,
        top_n: Annotated[int, Field(description=(
            "Number of recommendations to return. Default 20, max 50."
        ), ge=1, le=50)] = 20,
    ) -> str:
        """
        Get missing index recommendations from the query optimizer (dm_db_missing_index_*).
        impact_score = user_seeks × avg_user_impact. High score = strong candidate.
        Includes a ready-to-use CREATE INDEX statement. Works on Azure SQL and on-prem.
        Always test index additions in non-production first.
        """
        sql = f"""
          SELECT TOP ({top_n})
            DB_NAME(mid.database_id)                        AS database_name,
            OBJECT_NAME(mid.object_id, mid.database_id)     AS table_name,
            mid.equality_columns,
            mid.inequality_columns,
            mid.included_columns,
            migs.unique_compiles,
            migs.user_seeks,
            migs.user_scans,
            migs.last_user_seek,
            migs.last_user_scan,
            CAST(migs.avg_user_impact AS DECIMAL(5, 1))     AS avg_user_impact_pct,
            CAST(migs.avg_total_user_cost AS DECIMAL(18, 4)) AS avg_total_user_cost,
            CAST(migs.user_seeks * migs.avg_total_user_cost * (migs.avg_user_impact / 100.0) AS DECIMAL(18, 2)) AS impact_score,
            'CREATE INDEX [IX_'
              + OBJECT_NAME(mid.object_id, mid.database_id)
              + '_missing_'
              + CAST(mig.index_group_handle AS VARCHAR(20))
              + '] ON '
              + mid.statement
              + ' ('
              + ISNULL(mid.equality_columns, '')
              + CASE
                  WHEN mid.equality_columns IS NOT NULL
                   AND mid.inequality_columns IS NOT NULL THEN ','
                  ELSE ''
                END
              + ISNULL(mid.inequality_columns, '')
              + ')'
              + ISNULL(' INCLUDE (' + mid.included_columns + ')', '')
                                                            AS suggested_create_index
          FROM sys.dm_db_missing_index_groups mig
          JOIN sys.dm_db_missing_index_group_stats migs
            ON mig.index_group_handle = migs.group_handle
          JOIN sys.dm_db_missing_index_details mid
            ON mig.index_handle = mid.index_handle
          WHERE migs.avg_user_impact >= {min_impact}
          ORDER BY impact_score DESC
        """

        try:
            result = await query_instance(instance_name, sql, max_rows=top_n)
        except PendingAuthError as e:
            return _auth_required(e.device_code_info)
        except ValueError as e:
            return json.dumps({"error": str(e)})
        except Exception as e:
            return json.dumps({"error": str(e)})

        if not result["rows"]:
            return json.dumps({
                "message": f"No missing index recommendations with avg_user_impact >= {min_impact}%."
            })
        return to_json({"missing_indexes": result["rows"]})

    # ─────────────────────────────────────────────────────────────────────

    @mcp.tool()
    async def get_memory_usage(
        instance_name: Annotated[str, Field(description=(
            "Named SQL Server instance to query. Call list_instances_tool to see available names."
        ))] = "default",
    ) -> str:
        """
        Get SQL Server memory breakdown: top memory consumers by clerk and query memory
        grant semaphore status (waiter_count > 0 = memory grant pressure).
        Works on Azure SQL and on-prem. System-level memory (total physical RAM) is
        only available on on-prem — Azure SQL manages that at the platform level.
        """
        sql_clerks = """
          SELECT TOP 30
            type,
            name,
            pages_kb,
            virtual_memory_reserved_kb,
            virtual_memory_committed_kb,
            shared_memory_committed_kb
          FROM sys.dm_os_memory_clerks
          WHERE pages_kb > 0
          ORDER BY pages_kb DESC
        """

        sql_semaphores = """
          SELECT
            resource_semaphore_id,
            pool_id,
            target_memory_kb / 1024             AS target_memory_mb,
            available_memory_kb / 1024          AS available_memory_mb,
            granted_memory_kb / 1024            AS granted_memory_mb,
            used_memory_kb / 1024               AS used_memory_mb,
            grantee_count,
            waiter_count,
            timeout_error_count
          FROM sys.dm_exec_query_resource_semaphores
        """

        # sys.dm_os_sys_memory is on-prem only — attempt it, skip gracefully on Azure SQL
        sql_sys_memory = """
          SELECT
            total_physical_memory_kb / 1024     AS total_physical_mb,
            available_physical_memory_kb / 1024 AS available_physical_mb,
            system_memory_state_desc
          FROM sys.dm_os_sys_memory
        """

        clerks_task, semaphores_task, sys_mem_task = await asyncio.gather(
            query_instance(instance_name, sql_clerks, max_rows=30),
            query_instance(instance_name, sql_semaphores, max_rows=20),
            query_instance(instance_name, sql_sys_memory, max_rows=1),
            return_exceptions=True,
        )

        # Surface auth error from any sub-query
        for outcome in (clerks_task, semaphores_task, sys_mem_task):
            if isinstance(outcome, PendingAuthError):
                return _auth_required(outcome.device_code_info)

        output: dict = {}
        output["top_memory_clerks"] = (
            clerks_task["rows"] if not isinstance(clerks_task, Exception) else {"error": str(clerks_task)}
        )
        output["resource_semaphores"] = (
            semaphores_task["rows"] if not isinstance(semaphores_task, Exception) else {"error": str(semaphores_task)}
        )
        if not isinstance(sys_mem_task, Exception):
            output["system_memory"] = sys_mem_task["rows"]
        else:
            output["system_memory"] = "Not available on Azure SQL — managed at platform level."

        return to_json(output)

    # ─────────────────────────────────────────────────────────────────────

    @mcp.tool()
    async def get_database_info(
        instance_name: Annotated[str, Field(description=(
            "Named SQL Server instance to query. Call list_instances_tool to see available names."
        ))] = "default",
    ) -> str:
        """
        Get all databases with state, recovery model, compatibility level, and key settings.
        Use for capacity planning, identifying databases in SIMPLE recovery, or spotting
        non-normal states. Works on Azure SQL and on-prem.
        File sizes are only shown on on-prem (sys.master_files not available in Azure SQL).
        """
        # Core query — works everywhere
        sql_dbs = """
          SELECT
            d.database_id,
            d.name,
            d.state_desc,
            d.recovery_model_desc,
            d.compatibility_level,
            d.is_read_only,
            d.is_auto_close_on,
            d.is_auto_shrink_on,
            d.log_reuse_wait_desc,
            d.create_date,
            CAST(DATABASEPROPERTYEX(d.name, 'Collation') AS NVARCHAR(256)) AS collation
          FROM sys.databases d
          ORDER BY d.name
        """

        # File size join — on-prem only via sys.master_files
        sql_files = """
          SELECT
            database_id,
            SUM(CAST(size AS BIGINT)) * 8 / 1024          AS total_size_mb,
            SUM(CASE WHEN type = 0 THEN CAST(size AS BIGINT) ELSE 0 END) * 8 / 1024 AS data_mb,
            SUM(CASE WHEN type = 1 THEN CAST(size AS BIGINT) ELSE 0 END) * 8 / 1024 AS log_mb,
            COUNT(file_id) AS file_count
          FROM sys.master_files
          GROUP BY database_id
        """

        dbs_task, files_task = await asyncio.gather(
            query_instance(instance_name, sql_dbs, max_rows=500),
            query_instance(instance_name, sql_files, max_rows=500),
            return_exceptions=True,
        )

        for outcome in (dbs_task, files_task):
            if isinstance(outcome, PendingAuthError):
                return _auth_required(outcome.device_code_info)

        if isinstance(dbs_task, Exception):
            return json.dumps({"error": str(dbs_task)})

        databases = dbs_task["rows"]

        # Enrich with file sizes when available (on-prem)
        if not isinstance(files_task, Exception) and files_task["rows"]:
            file_map = {r["database_id"]: r for r in files_task["rows"]}
            for db in databases:
                finfo = file_map.get(db["database_id"], {})
                db["total_size_mb"] = finfo.get("total_size_mb")
                db["data_mb"] = finfo.get("data_mb")
                db["log_mb"] = finfo.get("log_mb")
                db["file_count"] = finfo.get("file_count")

        return to_json({"databases": databases})

    # ─────────────────────────────────────────────────────────────────────

    @mcp.tool()
    async def get_server_info(
        instance_name: Annotated[str, Field(description=(
            "Named SQL Server instance to query. Call list_instances_tool to see available names."
        ))] = "default",
    ) -> str:
        """
        Get SQL Server instance details: version, edition, hardware (CPU, memory), uptime,
        and key configuration settings. Good first call to establish the environment.
        sp_configure settings are on-prem only — skipped gracefully on Azure SQL.
        """
        sql_props = """
          SELECT
            CAST(SERVERPROPERTY('ProductVersion')    AS NVARCHAR(50))  AS product_version,
            CAST(SERVERPROPERTY('ProductLevel')      AS NVARCHAR(50))  AS product_level,
            CAST(SERVERPROPERTY('Edition')           AS NVARCHAR(256)) AS edition,
            CAST(SERVERPROPERTY('EngineEdition')     AS INT)           AS engine_edition,
            CAST(SERVERPROPERTY('ServerName')        AS NVARCHAR(256)) AS server_name,
            CAST(SERVERPROPERTY('Collation')         AS NVARCHAR(256)) AS collation,
            CAST(SERVERPROPERTY('IsHadrEnabled')     AS INT)           AS is_hadr_enabled,
            CAST(SERVERPROPERTY('IsClustered')       AS INT)           AS is_clustered
        """

        sql_sysinfo = """
          SELECT
            cpu_count,
            hyperthread_ratio,
            cpu_count / hyperthread_ratio           AS physical_cpus,
            physical_memory_kb / 1024               AS physical_memory_mb,
            sqlserver_start_time,
            DATEDIFF(HOUR, sqlserver_start_time, GETDATE()) AS uptime_hours,
            committed_kb / 1024                     AS sql_committed_mb,
            committed_target_kb / 1024              AS sql_target_mb
          FROM sys.dm_os_sys_info
        """

        # sys.configurations — on-prem only, not available in Azure SQL
        sql_config = """
          SELECT
            name,
            CAST(value_in_use AS NVARCHAR(256)) AS current_value,
            description
          FROM sys.configurations
          WHERE name IN (
            'max server memory (MB)', 'min server memory (MB)',
            'max degree of parallelism', 'cost threshold for parallelism',
            'optimize for ad hoc workloads', 'max worker threads',
            'remote admin connections'
          )
          ORDER BY name
        """

        props_task, sysinfo_task, config_task = await asyncio.gather(
            query_instance(instance_name, sql_props, max_rows=1),
            query_instance(instance_name, sql_sysinfo, max_rows=1),
            query_instance(instance_name, sql_config, max_rows=20),
            return_exceptions=True,
        )

        for outcome in (props_task, sysinfo_task, config_task):
            if isinstance(outcome, PendingAuthError):
                return _auth_required(outcome.device_code_info)

        output: dict = {}
        output["server_properties"] = (
            props_task["rows"] if not isinstance(props_task, Exception) else {"error": str(props_task)}
        )
        output["system_info"] = (
            sysinfo_task["rows"] if not isinstance(sysinfo_task, Exception) else {"error": str(sysinfo_task)}
        )
        if not isinstance(config_task, Exception):
            output["key_configurations"] = config_task["rows"]
        else:
            output["key_configurations"] = "Not available on Azure SQL — managed at platform level."

        return to_json(output)

    # ─────────────────────────────────────────────────────────────────────

    @mcp.tool()
    async def get_system_triage(
        instance_name: Annotated[str, Field(description=(
            "Named SQL Server instance to query. Call list_instances_tool to see available names."
        ))] = "default",
    ) -> str:
        """
        Phase 1 system pulse — run this first during any incident to identify the root bottleneck.

        Returns three parallel snapshots:
          • resource_stats: CPU / IO / log / worker / session % for the last 15 minutes
            (Azure SQL only — skipped on on-prem where sys.dm_db_resource_stats is unavailable)
          • session_routing: active request counts grouped by wait_type, with a routing
            signal — GOTO PHASE 2 (CPU/parallelism), GOTO PHASE 3 (lock contention),
            GOTO PHASE 4 (IO/memory), or MONITOR
          • lock_contention: summary of waiting lock requests grouped by resource type,
            mode, and database — non-zero rows confirm Phase 3 is needed

        The top-level recommended_action field summarises the dominant routing signal
        so you can immediately know where to look next.
        """
        # Azure SQL only — on-prem lacks sys.dm_db_resource_stats
        sql_resource = """
          SELECT TOP 15
            end_time,
            avg_cpu_percent,
            avg_data_io_percent,
            avg_log_write_percent,
            max_worker_percent,
            max_session_percent
          FROM sys.dm_db_resource_stats
          WHERE end_time >= DATEADD(MINUTE, -15, GETDATE())
          ORDER BY end_time DESC
        """

        sql_routing = """
          SELECT
            COUNT(*)                              AS active_sessions,
            r.status,
            COALESCE(r.wait_type, 'None')         AS wait_type,
            CASE
              WHEN r.wait_type IN (
                'CXCONSUMER','CXSYNC_PORT','SOS_SCHEDULER_YIELD'
              ) THEN 'GOTO PHASE 2'
              WHEN r.wait_type IN (
                'RESOURCE_SEMAPHORE','WRITELOG',
                'PAGEIOLATCH_SH','PAGEIOLATCH_EX',
                'RESERVED_MEMORY_ALLOCATION_EXT'
              ) THEN 'GOTO PHASE 4'
              WHEN r.wait_type IN (
                'LCK_M_S_XACT_MODIFY','LCK_M_IX',
                'LCK_M_S','LCK_M_X',
                'LCK_M_U','LCK_M_SCH_S','LCK_M_SCH_M',
                'PAGELATCH_SH','PAGELATCH_EX'
              ) THEN 'GOTO PHASE 3'
              ELSE 'MONITOR'
            END                                   AS routing
          FROM sys.dm_exec_requests r
          JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
          WHERE r.status NOT IN ('background', 'sleeping')
          GROUP BY r.status, r.wait_type
          ORDER BY active_sessions DESC
        """

        sql_locks = """
          SELECT
            DB_NAME(resource_database_id)         AS database_name,
            resource_type,
            resource_subtype,
            request_mode,
            request_type,
            request_status,
            COUNT(*)                              AS lock_count,
            SUM(CASE WHEN request_status = 'WAIT' THEN 1 ELSE 0 END) AS waiting_requests
          FROM sys.dm_tran_locks
          WHERE resource_database_id > 4
          GROUP BY resource_database_id, resource_type, resource_subtype,
                   request_mode, request_type, request_status
          HAVING SUM(CASE WHEN request_status = 'WAIT' THEN 1 ELSE 0 END) > 0
          ORDER BY waiting_requests DESC
        """

        resource_task, routing_task, locks_task = await asyncio.gather(
            query_instance(instance_name, sql_resource, max_rows=15),
            query_instance(instance_name, sql_routing, max_rows=100),
            query_instance(instance_name, sql_locks, max_rows=100),
            return_exceptions=True,
        )

        for outcome in (resource_task, routing_task, locks_task):
            if isinstance(outcome, PendingAuthError):
                return _auth_required(outcome.device_code_info)

        output: dict = {}

        # Resource stats — Azure SQL only
        if isinstance(resource_task, Exception):
            output["resource_stats"] = "Not available on on-prem — sys.dm_db_resource_stats is Azure SQL only."
        else:
            output["resource_stats"] = resource_task["rows"]

        # Session routing
        if isinstance(routing_task, Exception):
            output["session_routing"] = {"error": str(routing_task)}
            output["recommended_action"] = "UNKNOWN — routing query failed"
        else:
            rows = routing_task["rows"]
            output["session_routing"] = rows
            # Derive dominant action: highest active_sessions with a non-MONITOR routing
            dominant = next(
                (r["routing"] for r in rows if r.get("routing") != "MONITOR"),
                "MONITOR",
            )
            output["recommended_action"] = dominant

        # Lock contention summary
        if isinstance(locks_task, Exception):
            output["lock_contention"] = {"error": str(locks_task)}
        else:
            output["lock_contention"] = locks_task["rows"] or []

        return to_json(output)

    # ─────────────────────────────────────────────────────────────────────
    # Direct HTTP /run registry — maps tool name → callable
    # ─────────────────────────────────────────────────────────────────────
    _registry.update({
        "list_instances_tool":          list_instances_tool,
        "execute_query":                execute_query,
        "get_active_sessions":          get_active_sessions,
        "get_wait_stats":               get_wait_stats,
        "get_wait_stats_since_restart": get_wait_stats_since_restart,
        "get_top_queries":              get_top_queries,
        "get_long_running_transactions": get_long_running_transactions,
        "get_plan_cache_pollution":     get_plan_cache_pollution,
        "get_missing_indexes":          get_missing_indexes,
        "get_memory_usage":             get_memory_usage,
        "get_database_info":            get_database_info,
        "get_server_info":              get_server_info,
        "get_system_triage":            get_system_triage,
    })
