"""Buffered telemetry emitter (plan.md §9.2).

`Emitter(run_ctx).event(**cols)` appends to an in-memory buffer and fills the
run-context columns automatically.

Two flush strategies, selected by VERIFY item 3 (row inserts vs load-only):
  - inserts supported : flush every 50 events or every 2s.
  - load-only         : micro-batch — flush parquet at every agent_end and wave_end.

Emitter failures NEVER fail a run. They are logged locally and counted in
`payload` on run_end. A final flush that will not land after 3 retries is written
to data/telemetry_backlog/*.parquet and replayed at batch end.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

import pyarrow as pa
import pyarrow.parquet as pq

from telemetry.schema import ARROW_SCHEMA, QUERY_TEXT_MAX, TABLE, blank_row

log = logging.getLogger(__name__)

BACKLOG_DIR = Path("data/telemetry_backlog")
FLUSH_EVERY_N = 50
FLUSH_EVERY_S = 2.0
FLUSH_RETRIES = 3


@dataclass
class RunContext:
    """Columns stamped onto every row of a run. `fault_type` is truth-derived and
    added by the runner, outside agent context (§3 isolation rule)."""

    run_id: str
    pipeline_version: str
    mode: str
    scenario_id: str
    fault_type: str | None = None

    def as_columns(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "pipeline_version": self.pipeline_version,
            "mode": self.mode,
            "scenario_id": self.scenario_id,
            "fault_type": self.fault_type,
        }


class Sink(Protocol):
    """Where flushed rows go. Swap the implementation once VERIFY item 3 lands."""

    def write(self, batch: list[dict[str, Any]]) -> None: ...


class HotdataSink:
    """Appends micro-batches into the persistent telemetry DB.

    VERIFY item 3 is resolved: row inserts ARE supported — a managed-table load
    with mode="append" and inline `data`. So telemetry appends directly and does
    not need the load-only parquet workaround.

    The telemetry DB is event-scoped: created once at schema-freeze time, never
    torn down, and shared across every run of the event (§9.1).
    """

    def __init__(self, db_id: str | None = None, table: str = TABLE,
                 schema: str = "main", client: Any = None) -> None:
        self.db_id = db_id or os.getenv("TELEMETRY_DB_ID")
        if not self.db_id:
            raise RuntimeError("TELEMETRY_DB_ID is not set — see .env.example")
        self.table = table
        self.schema = schema
        self._client = client

    def _api(self):
        import hotdata

        from agents.hotdata_scope import client as make_client
        return hotdata.DatabasesApi(self._client or make_client())

    def write(self, batch: list[dict[str, Any]]) -> None:
        import hotdata

        ndjson = "\n".join(json.dumps(_jsonable(row), default=str) for row in batch)
        self._api().load_database_table(
            self.db_id, self.schema, self.table,
            hotdata.LoadManagedTableRequest(data=ndjson, format="json", mode="append"),
        )

    def create_table(self) -> None:
        """Run once, at schema-freeze time (§11, 10:00-11:00)."""
        import hotdata

        from agents.hotdata_scope import client as make_client
        from telemetry.schema import create_table_sql

        api = hotdata.QueryApi(self._client or make_client())
        api.query(hotdata.QueryRequest(database_id=self.db_id,
                                       sql=create_table_sql(self.table),
                                       default_schema=self.schema))


def _jsonable(row: dict[str, Any]) -> dict[str, Any]:
    """Timestamps to ISO strings; everything else passes through."""
    out = {}
    for k, v in row.items():
        out[k] = v.isoformat() if isinstance(v, datetime) else v
    return out


class ParquetSink:
    """Local fallback + the spike's sink. Also the backlog format."""

    def __init__(self, out_dir: Path = BACKLOG_DIR) -> None:
        self.out_dir = Path(out_dir)

    def write(self, batch: list[dict[str, Any]]) -> None:
        self.out_dir.mkdir(parents=True, exist_ok=True)
        path = self.out_dir / f"events_{int(time.time() * 1000)}_{uuid.uuid4().hex[:6]}.parquet"
        pq.write_table(_to_table(batch), path)


def _to_table(batch: list[dict[str, Any]]) -> pa.Table:
    cols = {name: [row.get(name) for row in batch] for name in ARROW_SCHEMA.names}
    return pa.table(cols, schema=ARROW_SCHEMA)


class Emitter:
    def __init__(self, ctx: RunContext, sink: Sink | None = None, *, load_only: bool = True) -> None:
        self.ctx = ctx
        self.sink = sink or ParquetSink()
        self.load_only = load_only
        self._buf: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        self._last_flush = time.monotonic()
        self.dropped = 0  # reported in payload on run_end

    def event(self, event_type: str, agent: str = "pipeline", wave: int | None = None, **cols: Any):
        row = blank_row()
        row.update(self.ctx.as_columns())
        row.update(
            event_id=uuid.uuid4().hex,
            event_type=event_type,
            agent=agent,
            wave=wave,
            ts=datetime.now(timezone.utc),
        )
        if (qt := cols.get("query_text")) and len(qt) > QUERY_TEXT_MAX:
            cols["query_text"] = qt[:QUERY_TEXT_MAX]
        if isinstance(cols.get("payload"), (dict, list)):
            cols["payload"] = json.dumps(cols["payload"], default=str)
        row.update({k: v for k, v in cols.items() if k in row})

        with self._lock:
            self._buf.append(row)
            due = self._should_flush(event_type)
        if due:
            self.flush()

    def _should_flush(self, event_type: str) -> bool:
        if self.load_only:
            # Micro-batch: flush at every agent_end and wave_end.
            return event_type in ("agent_end", "wave_end", "run_end")
        return (
            len(self._buf) >= FLUSH_EVERY_N
            or (time.monotonic() - self._last_flush) >= FLUSH_EVERY_S
        )

    def flush(self, *, final: bool = False) -> bool:
        """Returns True if the batch landed. Never raises — a telemetry failure
        must not fail the run."""
        with self._lock:
            batch, self._buf = self._buf, []
        if not batch:
            return True

        for attempt in range(FLUSH_RETRIES):
            try:
                self.sink.write(batch)
                self._last_flush = time.monotonic()
                return True
            except Exception as exc:  # noqa: BLE001 — telemetry must never break a run
                log.warning("telemetry flush failed (attempt %d): %s", attempt + 1, exc)
                time.sleep(0.2 * (attempt + 1))

        self.dropped += len(batch)
        try:
            ParquetSink().write(batch)  # backlog; replayed at batch end
            log.warning("telemetry: %d rows written to backlog", len(batch))
        except Exception as exc:  # noqa: BLE001
            log.error("telemetry: backlog write failed, %d rows lost: %s", len(batch), exc)
        return False

    def __enter__(self) -> Emitter:
        return self

    def __exit__(self, *exc_info: object) -> None:
        # The run can't finish until the final flush is attempted (§9.2).
        self.flush(final=True)


def replay_backlog(sink: Sink, out_dir: Path = BACKLOG_DIR) -> int:
    """Replay backlogged parquet into the telemetry DB. Run at batch end."""
    out_dir = Path(out_dir)
    if not out_dir.exists():
        return 0
    replayed = 0
    for path in sorted(out_dir.glob("*.parquet")):
        rows = pq.read_table(path).to_pylist()
        try:
            sink.write(rows)
            path.unlink()
            replayed += len(rows)
        except Exception as exc:  # noqa: BLE001
            log.warning("backlog replay failed for %s: %s", path.name, exc)
    return replayed
