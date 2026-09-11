"""Frozen telemetry schema (plan.md §9.1) — the single source of truth.

Hotdata tables have no ALTER path, so this is finalized BEFORE the first run
(gate: 10:45). Anything unforeseen goes in `payload` as a JSON string.

Aggregation convention (§9.1): per-agent totals live on `agent_end` rows, per-run
totals on `run_end` rows. Never SUM over `llm_call` and `agent_end` together, or
you double-count.

`ts` is a NAIVE timestamp holding UTC. Hotdata's CSV loader always infers naive
`timestamp`, and a column cannot change type from `timestamptz` after the table's
schema is pinned — so a tz-aware column would reject every inline write. Every
timestamp in this system is UTC by construction (the generator and the emitter
both use timezone.utc), so nothing is lost; just never write a local time here.
"""

from __future__ import annotations

import pyarrow as pa

TABLE = "events"

EVENT_TYPES = (
    "run_start",
    "run_end",
    "wave_start",
    "wave_end",
    "db_create",
    "db_load",
    "db_destroy",
    "agent_start",
    "agent_end",
    "query",
    "llm_call",
    "error",
    "retry",
)

QUERY_KINDS = ("sql", "fts", "vector")

MODES = ("parallel", "single")

# `pipeline`, the five source agents, `correlator`, `sweeper`.
AGENTS = (
    "pipeline",
    "logs",
    "metrics",
    "changes",
    "infra",
    "history",
    "correlator",
    "sweeper",
)

QUERY_TEXT_MAX = 1000  # query_text is truncated to this before emit

# Column -> (arrow type, populated on). Order here is the table's column order.
COLUMNS: dict[str, tuple[pa.DataType, str]] = {
    "event_id": (pa.string(), "all"),
    "run_id": (pa.string(), "all"),
    "pipeline_version": (pa.string(), "all (v1, v2, single-v1)"),
    "mode": (pa.string(), "all (parallel / single)"),
    "scenario_id": (pa.string(), "all"),
    "fault_type": (pa.string(), "all (truth, added by runner)"),
    "prompt_hash": (pa.string(), "agent_*, llm_call"),
    "agent": (pa.string(), "all"),
    "wave": (pa.int32(), "all"),
    "event_type": (pa.string(), "all"),
    "ts": (pa.timestamp("us"), "all"),
    "duration_ms": (pa.float64(), "*_end, query, llm_call, db_*"),
    "model": (pa.string(), "llm_call, agent_end"),
    "tokens_in": (pa.int64(), "llm_call; summed on agent_end and run_end"),
    "tokens_out": (pa.int64(), "same"),
    "cost_usd": (pa.float64(), "same"),
    "query_count": (pa.int32(), "agent_end, run_end"),
    "db_id": (pa.string(), "db_*, query"),
    "query_kind": (pa.string(), "query (sql / fts / vector)"),
    "query_text": (pa.string(), f"query (truncated to {QUERY_TEXT_MAX} chars)"),
    "rows_returned": (pa.int64(), "query, db_load"),
    "success": (pa.bool_(), "all *_end, query, db_*"),
    "retry_count": (pa.int32(), "agent_end"),
    "error_msg": (pa.string(), "error, retry"),
    "hit_service": (pa.bool_(), "agent_end, run_end"),
    "hit_fault": (pa.bool_(), "agent_end, run_end"),
    "score": (pa.int32(), "run_end"),
    "payload": (pa.string(), "overflow JSON: hypotheses, RCAReport, symptoms"),
}

COLUMN_NAMES = tuple(COLUMNS)

ARROW_SCHEMA = pa.schema([(name, dtype) for name, (dtype, _) in COLUMNS.items()])

# Search indexes on the telemetry DB. VERIFY (§8, item 6): whether an index
# refreshes after new loads into an existing table.
INDEXES = {"tel_error_msg": ("events", "error_msg", "fts")}


def create_table_sql(table: str = TABLE) -> str:
    """DDL for the persistent telemetry DB. Run once, at schema-freeze time."""
    sql_types = {
        pa.string(): "VARCHAR",
        pa.int32(): "INTEGER",
        pa.int64(): "BIGINT",
        pa.float64(): "DOUBLE",
        pa.bool_(): "BOOLEAN",
        pa.timestamp("us"): "TIMESTAMP",
    }
    cols = ",\n  ".join(f"{n} {sql_types[t]}" for n, (t, _) in COLUMNS.items())
    return f"CREATE TABLE IF NOT EXISTS {table} (\n  {cols}\n);"


def blank_row() -> dict[str, None]:
    """An all-null row. Emitter fills run-context columns, caller fills the rest."""
    return dict.fromkeys(COLUMN_NAMES)


if __name__ == "__main__":
    print(create_table_sql())
