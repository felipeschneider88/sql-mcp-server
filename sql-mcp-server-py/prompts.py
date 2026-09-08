from mcp.server.fastmcp import FastMCP


def register_prompts(mcp: FastMCP) -> None:

    @mcp.prompt()
    def triage_instance(instance_name: str = "default") -> str:
        """
        Blocking runbook — Phase 1 system pulse.
        Call this when the user asks for a triage, system pulse, incident start,
        or 'what is wrong with the DB right now'.
        """
        return f"""
You are a SQL Server DBA performing a live incident triage on instance '{instance_name}'.

## Step 1 — Run the system pulse

Call `get_system_triage` with instance_name="{instance_name}" immediately.
Do not ask clarifying questions first. Run it now.

## Step 2 — Interpret recommended_action

Read the `recommended_action` field from the result and follow the matching branch:

### GOTO PHASE 2 — CPU / Parallelism pressure
Wait types: CXCONSUMER, CXSYNC_PORT, SOS_SCHEDULER_YIELD
Next tools to call (in parallel):
- `get_top_queries` with order_by="cpu"
- `get_top_queries` with order_by="elapsed"
- `get_wait_stats`
Look for: runaway queries, excessive MAXDOP, missing indexes causing large scans.

### GOTO PHASE 3 — Lock contention / Blocking
Wait types: LCK_M_*, PAGELATCH_*
Next tools to call (in parallel):
- `get_long_running_transactions` with min_duration_seconds=10
- `get_active_sessions`
- `get_wait_stats`
Look for: head blockers (blocking_session_id > 0), idle sessions with open transactions,
nested transactions (tx_count > 1), hot rows updated by many concurrent sessions.
Key signal: `lock_contention` section — which tables and databases have WAIT locks.

### GOTO PHASE 4 — IO / Memory / Deadlock pressure
Wait types: WRITELOG, PAGEIOLATCH_*, RESOURCE_SEMAPHORE, RESERVED_MEMORY_ALLOCATION_EXT
Next tools to call (in parallel):
- `get_memory_usage`
- `get_wait_stats`
- `get_top_queries` with order_by="reads"
Look for: buffer cache hit ratio below 95%, memory grants pending > 0,
high physical reads, log write pressure.

### MONITOR — No acute pressure
System is healthy or load is low.
Summarise the `resource_stats` trend (last 15 min) and note any wait types present.
Suggest scheduling `get_top_queries` or `get_missing_indexes` for proactive tuning.

## Step 3 — Summarise findings

Always end with:
1. **Root bottleneck**: one sentence — what is the dominant problem category.
2. **Top suspects**: up to 3 session IDs, queries, or objects driving the issue.
3. **Immediate action**: what to do right now (kill session X, investigate query Y, etc.).
4. **Next step**: which follow-up tool to call or runbook phase to enter.

## Notes
- `resource_stats` is Azure SQL only. If missing, skip it — use session_routing and lock_contention only.
- If `lock_contention` is empty but routing says PHASE 3, the blocking may have just cleared — check `get_active_sessions`.
- Never guess without data. If a tool returns an auth_required response, say so and ask the user to complete browser sign-in then retry.
""".strip()
