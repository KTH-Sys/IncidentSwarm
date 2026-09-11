"""Hotdata lifecycle: provision / load / index / scoped query / destroy / sweep
(plan.md §8).

Isolation rule (§3): each wave-1 agent sees ONLY its own db_id plus symptoms[].
Agents never receive truth.json and never see another agent's db_id or output
before wave 2.

VERIFY items resolved against hotdata SDK 0.10.0 — see docs/verify.md:
  3. Telemetry appends use typed parquet uploads with mode="append".
  4. Bulk-create is ASYNC. Poll get_database_batch until created_count == count,
     then list_databases(batch=...) for the ids.
  5. Delete each database individually, then cancel its creation batch.
  6. FTS index_type is "bm25"; vector needs an embedding_provider_id.
"""

from __future__ import annotations

import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

import hotdata

from telemetry.emit import Emitter

DB_PREFIX = os.getenv("DB_PREFIX", "isw")
DEFAULT_SCHEMA = "main"

# Orphan backstop (§13): every run-scoped DB self-expires even if teardown and
# the sweeper both fail. Comfortably longer than a run, far shorter than the event.
DB_TTL = "30m"
ORPHAN_MAX_AGE_S = 15 * 60

# Bulk-create polling (VERIFY item 4).
PROVISION_POLL_S = 0.25
PROVISION_TIMEOUT_S = 60.0
INLINE_LOAD_MAX_BYTES = 2 * 1024 * 1024  # above this, stage an upload first

# Per-agent slice: agent -> table, query kinds, budget (§7.2).
SLICES: dict[str, dict[str, Any]] = {
    "logs":    {"table": "logs",        "kinds": ("sql", "fts"),  "budget": 40},
    "metrics": {"table": "metrics",     "kinds": ("sql",),        "budget": 30},
    "changes": {"table": "changes",     "kinds": ("sql",),        "budget": 15},
    "infra":   {"table": "k8s_events",  "kinds": ("sql", "fts"),  "budget": 15},
    "history": {"table": "postmortems", "kinds": ("vector",),     "budget": 10},
}
FANOUT = tuple(SLICES)  # wave-1 order; index i maps to db_ids[i]

# The single-agent baseline gets one DB with all five tables and a budget equal
# to the sum of the parallel budgets (§3).
SINGLE_BUDGET = sum(s["budget"] for s in SLICES.values())  # 110

# (table, column, index_type). "bm25" is Hotdata's full-text type.
INDEX_SPECS = (
    ("logs", "msg", "bm25"),
    ("k8s_events", "message", "bm25"),
    ("postmortems", "body", "vector"),
)


class BudgetExceeded(RuntimeError):
    """An agent exhausted its query budget."""


@dataclass
class Provisioned:
    """Wave-0 output: retain both the batch handle and individual database IDs."""

    batch_id: str
    db_ids: list[str]


@dataclass
class ScopedDB:
    """What a wave-1 agent is handed: one db_id and nothing else."""

    agent: str
    db_id: str
    budget: int
    used: int = 0
    client: Any = field(default=None, repr=False)
    em: Emitter | None = field(default=None, repr=False)

    def sql(self, statement: str) -> list[dict]:
        return query(self.db_id, statement, agent=self.agent, kind="sql",
                     em=self.em, scope=self, client_=self.client)

    @property
    def remaining(self) -> int:
        return self.budget - self.used


def client(api_key: str | None = None, workspace: str | None = None) -> hotdata.ApiClient:
    """Build an API client from the environment. Raises early with a clear
    message rather than failing deep inside a wave."""
    api_key = api_key or os.getenv("HOTDATA_API_KEY")
    workspace = workspace or os.getenv("HOTDATA_WORKSPACE")
    if not api_key:
        raise RuntimeError("HOTDATA_API_KEY is not set — see .env.example")
    if not workspace:
        raise RuntimeError("HOTDATA_WORKSPACE is not set — see .env.example")
    cfg = hotdata.Configuration(api_key=api_key, workspace_id=workspace)
    if host := os.getenv("HOTDATA_HOST"):
        cfg.host = host
    return hotdata.ApiClient(cfg)


def provision(run_id: str, n: int, em: Emitter, *, client_: Any = None,
              ttl: str = DB_TTL) -> Provisioned:
    """Bulk-create n instant DBs from one template. Emits db_create x n.

    Bulk-create is async (VERIFY item 4): the call returns a batch_id, and
    created_count advances as the batch fills. We poll, then resolve ids with
    list_databases(batch=...). DBs are addressed by ID — names are NOT unique.

    Every DB carries expires_at=ttl so a crashed run cannot leave orphans
    burning credits (§13).
    """
    api = hotdata.DatabasesApi(client_ or client())
    t0 = time.monotonic()

    result = api.bulk_create_databases(
        hotdata.BulkCreateDatabasesRequest(
            count=n,
            name_template=f"{DB_PREFIX}-{run_id}-{{index}}",
            expires_at=ttl,
            # Unique per call: the key guards against a retried HTTP request,
            # but reusing it across calls resurrects a spent (already-deleted)
            # batch, which then resolves to zero database ids.
            idempotency_key=f"{DB_PREFIX}-{run_id}-{uuid.uuid4().hex[:8]}",
            default_schema=DEFAULT_SCHEMA,
        )
    )
    batch_id = result.batch_id

    deadline = time.monotonic() + PROVISION_TIMEOUT_S
    while True:
        batch = api.get_database_batch(batch_id)
        if batch.created_count >= n:
            break
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"bulk-create stalled: {batch.created_count}/{n} after "
                f"{PROVISION_TIMEOUT_S}s (batch {batch_id})"
            )
        time.sleep(PROVISION_POLL_S)

    page = api.list_databases(batch=batch_id, limit=max(n, 1))
    db_ids = [d.id for d in page.databases][:n]
    if len(db_ids) != n:
        raise RuntimeError(f"batch {batch_id} yielded {len(db_ids)} ids, wanted {n}")

    # One db_create row per DB, so Q7 can separate provisioning from agent time.
    each_ms = (time.monotonic() - t0) * 1000 / n
    for db_id in db_ids:
        em.event("db_create", agent="pipeline", wave=0, db_id=db_id,
                 duration_ms=each_ms, success=True,
                 payload={"batch_id": batch_id, "expires_at": ttl})

    return Provisioned(batch_id=batch_id, db_ids=db_ids)


@dataclass
class Loaded:
    """A load's result.

    `connection_id` is needed to index the table: the indexes API is addressed by
    CONNECTION id, not database id, and the two are different strings (`conn...`
    vs `dbid...`). Passing the database id fails with a misleading
    "Table 'main.logs' not found in connection 'dbid...'" 404.
    """

    rows: int
    connection_id: str | None


def load(db_id: str, table: str, parquet: Path, em: Emitter, *, client_: Any = None,
         mode: str = "replace") -> Loaded:
    """Upload + load a parquet slice into `table`. Emits db_load.

    Files above 2 MB are staged through the uploads API — inline `data` is the
    quick path for small payloads only, and logs.parquet is ~200k rows.
    """
    c = client_ or client()
    api = hotdata.DatabasesApi(c)
    parquet = Path(parquet)
    t0 = time.monotonic()

    # Declare the table; columns are inferred from the parquet on load.
    try:
        api.add_database_table(db_id, DEFAULT_SCHEMA,
                               hotdata.AddManagedTableRequest(name=table))
    except hotdata.ApiException as exc:
        if exc.status not in (400, 409):  # already declared is fine
            raise

    upload_id = hotdata.UploadsApi(c).upload_file(
        parquet, filename=parquet.name, content_type="application/vnd.apache.parquet"
    ).upload_id

    resp = api.load_database_table(
        db_id, DEFAULT_SCHEMA, table,
        hotdata.LoadManagedTableRequest(upload_id=upload_id, format="parquet", mode=mode),
    )
    rows = resp.row_count or 0
    em.event("db_load", agent="pipeline", wave=0, db_id=db_id,
             duration_ms=(time.monotonic() - t0) * 1000, rows_returned=rows,
             success=True, payload={"table": table, "mode": mode,
                                    "connection_id": resp.connection_id})
    return Loaded(rows=rows, connection_id=resp.connection_id)


def index(connection_id: str | None, specs: tuple = INDEX_SPECS, *, em: Emitter,
          client_: Any = None, embedding_provider_id: str | None = None,
          only_table: str | None = None, db_id: str | None = None) -> None:
    """Create the search indexes a table needs. Emits db_load with kind=index.

    Addressed by CONNECTION id (from a Loaded result), not database id.

    Only builds specs whose table is present, so a sliced DB builds just its own
    (the logs DB gets bm25 on logs.msg and nothing else).

    A vector index needs an embedding provider (VERIFY item 6). Without one the
    vector spec is skipped and HistoryAgent degrades to no index — which is §12
    cut-list item 1 anyway, so a missing provider must not break the run.
    """
    if not connection_id:
        em.event("error", agent="pipeline", wave=0, db_id=db_id, success=False,
                 error_msg="no connection_id; indexes skipped")
        return

    api = hotdata.IndexesApi(client_ or client())
    provider = embedding_provider_id or os.getenv("HOTDATA_EMBEDDING_PROVIDER_ID")

    for table, column, kind in specs:
        if only_table and table != only_table:
            continue
        if kind == "vector" and not provider:
            em.event("error", agent="pipeline", wave=0, db_id=db_id, success=False,
                     error_msg="no embedding provider; vector index skipped",
                     payload={"table": table, "column": column})
            continue
        t0 = time.monotonic()
        req = hotdata.CreateIndexRequest(
            index_name=f"{table}_{column}_{kind}",
            columns=[column],
            index_type=kind,
            **({"embedding_provider_id": provider} if kind == "vector" else {}),
        )
        api.create_index(connection_id, DEFAULT_SCHEMA, table, req)
        em.event("db_load", agent="pipeline", wave=0, db_id=db_id,
                 duration_ms=(time.monotonic() - t0) * 1000, success=True,
                 payload={"kind": "index", "table": table, "column": column, "type": kind})


def query(db_id: str, sql: str, *, agent: str, kind: str = "sql", em: Emitter,
          scope: ScopedDB | None = None, client_: Any = None) -> list[dict]:
    """Run one scoped query. Enforces the budget, times it, emits a `query` row.

    This is the only path an agent has to data — it cannot reach another db_id.
    """
    if scope is not None:
        if scope.used >= scope.budget:
            raise BudgetExceeded(f"{agent} exhausted its budget of {scope.budget}")
        scope.used += 1

    api = hotdata.QueryApi(client_ or client())
    t0 = time.monotonic()
    try:
        resp = api.query(hotdata.QueryRequest(
            database_id=db_id, sql=sql, default_schema=DEFAULT_SCHEMA))
    except Exception as exc:  # noqa: BLE001 — a bad query is an agent's problem, not a crash
        em.event("query", agent=agent, wave=1, db_id=db_id, query_kind=kind,
                 query_text=sql, duration_ms=(time.monotonic() - t0) * 1000,
                 success=False, error_msg=str(exc)[:500])
        raise

    rows = [dict(zip(resp.columns or [], r)) for r in (resp.rows or [])]
    em.event("query", agent=agent, wave=1, db_id=db_id, query_kind=kind,
             query_text=sql, rows_returned=resp.row_count,
             duration_ms=resp.execution_time_ms or (time.monotonic() - t0) * 1000,
             success=True)
    return rows


def destroy(batch_id: str, em: Emitter, *, db_ids: list[str] | None = None,
            client_: Any = None) -> int:
    """Delete the run's databases. ALWAYS reached through try/finally.

    Each database is deleted individually. `delete_database_batch` does NOT do
    this — it cancels the creation batch ("databases already created are kept;
    only further creation is stopped"), so calling it alone silently leaks every
    DB the run made. Verified against the live API; see docs/verify.md item 5.

    Deletes run concurrently because wave 3 is on the critical path and each call
    is an independent round trip.

    Never raises: a teardown failure must not mask a run's result. Failures are
    emitted with success=False, and expires_at plus sweep() are the backstops.
    """
    api = hotdata.DatabasesApi(client_ or client())
    ids = list(db_ids or [])
    deleted = 0

    def _one(db_id: str) -> tuple[str, bool, str | None, float]:
        t0 = time.monotonic()
        try:
            api.delete_database(db_id)
            return db_id, True, None, (time.monotonic() - t0) * 1000
        except Exception as exc:  # noqa: BLE001
            return db_id, False, str(exc)[:500], (time.monotonic() - t0) * 1000

    if ids:
        with ThreadPoolExecutor(max_workers=min(8, len(ids))) as pool:
            for db_id, ok, err, ms in pool.map(_one, ids):
                deleted += ok
                em.event("db_destroy", agent="pipeline", wave=3, db_id=db_id,
                         duration_ms=ms, success=ok, error_msg=err,
                         payload={"batch_id": batch_id})

    # Stop the creation batch too, so a slow batch cannot add DBs after teardown.
    if batch_id:
        try:
            api.delete_database_batch(batch_id)
        except Exception:  # noqa: BLE001 — best effort; the DBs are already gone
            pass
    return deleted


def sweep(prefix: str = DB_PREFIX, max_age_s: int = ORPHAN_MAX_AGE_S, *, em: Emitter,
          client_: Any = None) -> int:
    """Delete orphaned `{prefix}-*` DBs older than max_age_s. Run at batch start
    and end. Emits db_destroy with agent='sweeper'. Returns the count reaped.

    Zero orphan DBs is a submission checklist item (§15).
    """
    from datetime import datetime, timedelta, timezone

    api = hotdata.DatabasesApi(client_ or client())
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=max_age_s)
    telemetry_db = os.getenv("TELEMETRY_DB_ID")
    reaped, cursor = 0, None

    while True:
        page = api.list_databases(search=prefix, limit=100, cursor=cursor)
        for db in page.databases:
            created = getattr(db, "created_at", None)
            if not (db.name or "").startswith(prefix) or (created and created > cutoff):
                continue
            # NEVER sweep the telemetry DB. It shares the isw- prefix, is older
            # than any cutoff, and holds the entire event's results — deleting it
            # is unrecoverable. Three independent guards, because one bad sweep
            # ends the day: its id, its name, and the fact that run-scoped DBs
            # always carry expires_at while the telemetry DB never does.
            if db.id == telemetry_db:
                continue
            if (db.name or "").startswith(f"{prefix}-telemetry"):
                continue
            if getattr(db, "expires_at", None) is None:
                continue
            t0 = time.monotonic()
            try:
                api.delete_database(db.id)
                ok, err = True, None
                reaped += 1
            except Exception as exc:  # noqa: BLE001
                ok, err = False, str(exc)[:500]
            em.event("db_destroy", agent="sweeper", wave=3, db_id=db.id,
                     duration_ms=(time.monotonic() - t0) * 1000, success=ok,
                     error_msg=err, payload={"reason": "orphan sweep"})
        if not page.has_more:
            break
        cursor = page.next_cursor
    return reaped


@contextmanager
def run_scope(run_id: str, n: int, em: Emitter, *, client_: Any = None) -> Iterator[Provisioned]:
    """Provision n DBs and guarantee teardown.

        with run_scope(run_id, 5, em) as prov:
            ...  # waves 1-2 over prov.db_ids
        # wave 3 teardown happens here, even on exception
    """
    prov: Provisioned | None = None
    try:
        prov = provision(run_id, n, em, client_=client_)
        yield prov
    finally:
        if prov is not None:
            destroy(prov.batch_id, em, db_ids=prov.db_ids, client_=client_)
