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

    # ─────────────────────────────────────────────────────────────────────

    @mcp.tool()
    async def get_wait_stats(
        instance_name: Annotated[str, Field(description=(
            "Named SQL Server instance to query. Call list_instances_tool to see available names."
        ))] = "default",
        exclude_benign: Annotated[bool, Field(description=(
            "Exclude known idle/background waits to focus on actionable waits. Default true."
        ))] = True,
    ) -> str:
        """
        Get cumulative wait statistics since last SQL Server restart.
        Key signals: PAGEIOLATCH_* = disk I/O; LCK_* = lock contention;
        CXPACKET/CXCONSUMER = parallelism; SOS_SCHEDULER_YIELD = CPU pressure;
        RESOURCE_SEMAPHORE = memory grants.
        """
        benign = [
            "SLEEP_TASK","SLEEP_SYSTEMTASK","SLEEP_DBSTARTUP","SLEEP_DBTASK",
            "SLEEP_TEMPDBSTARTUP","SLEEP_MASTERDBREADY","SLEEP_MASTERMDREADY",
            "SLEEP_MASTERUPGRADED","SLEEP_MSDBSTARTUP","SLEEP_REPLICATION_MONITOR",
            "BROKER_EVENTHANDLER","BROKER_RECEIVE_WAITFOR","BROKER_TASK_STOP",
            "BROKER_TO_FLUSH","BROKER_TRANSMITTER",
            "CHECKPOINT_QUEUE",
            "CLR_AUTO_EVENT","CLR_MANUAL_EVENT","CLR_SEMAPHORE",
            "DBMIRROR_DBM_EVENT","DBMIRROR_DBM_MUTEX","DBMIRROR_EVENTS_QUEUE",
            "DBMIRROR_WORKER_QUEUE","DBMIRRORING_CMD",
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
            "PREEMPTIVE_SP_SERVER_DIAGNOSTICS",
            "PVS_PREALLOCATE","PWAIT_EXTENSIBILITY_CLEANUP_TASK",
            "QDS_ASYNC_QUEUE",
            "QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP",
            "QDS_PERSIST_TASK_MAIN_LOOP_SLEEP","QDS_SHUTDOWN_QUEUE",
            "REDO_THREAD_PENDING_WORK","REQUEST_FOR_DEADLOCK_SEARCH",
            "RESOURCE_QUEUE","SERVER_IDLE_CHECK","SNI_HTTP_ACCEPT",
            "SOS_WORK_DISPATCHER","SP_SERVER_DIAGNOSTICS_SLEEP",
            "SQLTRACE_BUFFER_FLUSH","SQLTRACE_INCREMENTAL_FLUSH_SLEEP",
            "UCS_SESSION_REGISTRATION",
            "WAIT_XTP_OFFLINE_CKPT_NEW_LOG",
            "WAITFOR",
            "XE_DISPATCHER_WAIT","XE_LIVE_TARGET_TVF","XE_TIMER_EVENT",
        ]
        benign_filter = (
            "AND wait_type NOT IN ({})".format(
                ", ".join(f"'{w}'" for w in benign)
            )
            if exclude_benign else ""
        )

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
