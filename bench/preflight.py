"""Preflight — prove the stack works before betting the afternoon on it.

    python -m bench.preflight                    # check everything
    python -m bench.preflight --create-telemetry-db

Runs the GATE 1 spike from plan.md §11 for real: create -> load a tiny parquet ->
query -> destroy, with telemetry rows written and read back. If this passes, the
pipeline's data layer works; if it fails, it says exactly which step broke.

Every check is independent, so one failure does not hide the others.
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa

import config  # noqa: F401  — loads .env
import pyarrow.parquet as pq

OK, BAD, SKIP = "  ok  ", " FAIL ", " skip "
_results: list[tuple[str, str, str]] = []


def record(name: str, status: str, detail: str = "") -> None:
    _results.append((name, status, detail))
    print(f"[{status}] {name}" + (f" — {detail}" if detail else ""))


def check_env() -> bool:
    required = ("HOTDATA_API_KEY", "HOTDATA_WORKSPACE", "OPENAI_API_KEY")
    optional = ("TELEMETRY_DB_ID", "MODEL_PRICING", "HOTDATA_EMBEDDING_PROVIDER_ID")
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        record("env: credentials", BAD, f"missing {', '.join(missing)}")
        return False
    record("env: credentials", OK, "hotdata + openai")
    for k in optional:
        record(f"env: {k}", OK if os.getenv(k) else SKIP,
               "" if os.getenv(k) else "not set")
    return True


def check_hotdata_reachable() -> bool:
    try:
        from agents.hotdata_scope import client
        import hotdata

        page = hotdata.DatabasesApi(client()).list_databases(limit=1)
        record("hotdata: reachable", OK, f"{page.count} databases visible")
        return True
    except Exception as exc:  # noqa: BLE001
        record("hotdata: reachable", BAD, str(exc)[:200])
        return False


def check_models() -> bool:
    """Call both tiers for real. A model that only fails under tool use at 13:30
    is a model that cost you the batch."""
    from agents.llm import FAST_MODEL, STRONG_MODEL, Usage, call, price_of

    from telemetry.emit import Emitter, ParquetSink, RunContext

    em = Emitter(RunContext("preflight", "preflight", "parallel", "probe"),
                 ParquetSink(Path(tempfile.mkdtemp())), load_only=True)
    ok_all = True
    for tier, model in (("fast", FAST_MODEL), ("strong", STRONG_MODEL)):
        try:
            u = Usage()
            r = call(model=model, system="Reply with exactly: ok",
                     messages=[{"role": "user", "content": "ok?"}], em=em,
                     agent="pipeline", wave=0, usage=u, max_tokens=16)
            record(f"model: {tier}", OK,
                   f"{model}, {u.tokens_in}+{u.tokens_out} tokens, ${u.cost_usd:.6f}")
        except Exception as exc:  # noqa: BLE001
            record(f"model: {tier}", BAD, f"{model}: {str(exc)[:180]}")
            ok_all = False
        pin, pout = price_of(model)
        if (pin, pout) == (0.0, 0.0):
            record(f"pricing: {model}", BAD,
                   "not in MODEL_PRICING — cost_usd would be 0 and target T3 meaningless")
            ok_all = False
        else:
            record(f"pricing: {model}", OK, f"${pin}/${pout} per 1M in/out")
    em.flush(final=True)
    return ok_all


def list_models() -> None:
    """Show what this key can actually use, so the tiers are picked, not guessed."""
    from agents.llm import client

    names = sorted(m.id for m in client().models.list().data)
    print(f"{len(names)} models available to this key:\n")
    for n in names:
        print(f"  {n}")
    print("\nSet FAST_MODEL and STRONG_MODEL in .env from this list.")


def check_data() -> bool:
    scenarios = sorted(p for p in Path("data").glob("s*") if p.is_dir())
    if not scenarios:
        record("data: scenarios", BAD, "none — run: python -m gen.scenarios --seeds 1-12 99")
        return False
    missing = [s.name for s in scenarios if not (s / "truth.json").exists()]
    record("data: scenarios", OK if not missing else BAD,
           f"{len(scenarios)} scenarios" + (f", incomplete: {missing}" if missing else ""))
    return not missing


def gate1_cycle() -> bool:
    """create -> load -> query -> destroy, with telemetry. The §11 Gate 1 spike."""
    from agents.hotdata_scope import client, destroy, load, provision, query
    from telemetry.emit import Emitter, ParquetSink, RunContext

    tmp = Path(tempfile.mkdtemp())
    pq.write_table(pa.table({"service": ["payments"] * 10,
                            "n": list(range(10))}), tmp / "probe.parquet")
    em = Emitter(RunContext(f"preflight-{uuid.uuid4().hex[:6]}", "preflight", "parallel",
                            "probe"), ParquetSink(tmp), load_only=False)
    c = client()
    prov = None
    try:
        t0 = time.monotonic()
        prov = provision("preflight", 2, em, client_=c, ttl="10m")
        record("gate1: bulk-create 2 DBs", OK,
               f"{(time.monotonic()-t0)*1000:.0f}ms, batch {prov.batch_id[:12]}")

        rows = load(prov.db_ids[0], "probe", tmp / "probe.parquet", em, client_=c)
        record("gate1: load parquet", OK, f"{rows} rows")

        out = query(prov.db_ids[0], "SELECT service, COUNT(*) AS n FROM probe GROUP BY service",
                    agent="pipeline", kind="sql", em=em, client_=c)
        record("gate1: query", OK, str(out)[:80])
        return True
    except Exception as exc:  # noqa: BLE001
        record("gate1: create->load->query", BAD, str(exc)[:300])
        return False
    finally:
        if prov:
            n = destroy(prov.batch_id, em, db_ids=prov.db_ids, client_=c)
            record("gate1: destroy batch", OK if n else BAD, f"deleted {n}")
        em.flush(final=True)


def check_telemetry_db() -> bool:
    if not os.getenv("TELEMETRY_DB_ID"):
        record("telemetry: db", SKIP, "TELEMETRY_DB_ID not set")
        return False
    try:
        from telemetry.emit import HotdataSink
        from telemetry.schema import COLUMN_NAMES
        import hotdata
        from agents.hotdata_scope import client

        resp = hotdata.QueryApi(client()).query(hotdata.QueryRequest(
            database_id=os.getenv("TELEMETRY_DB_ID"),
            sql="SELECT COUNT(*) AS n FROM events", default_schema="main"))
        n = (resp.rows or [[0]])[0][0]
        record("telemetry: events table", OK, f"{n} rows, {len(COLUMN_NAMES)} columns")
        return True
    except Exception as exc:  # noqa: BLE001
        record("telemetry: events table", BAD,
               f"{str(exc)[:150]} — run with --create-telemetry-db")
        return False


def seed_schema(db_id: str, client_=None) -> None:
    """Pin the events table's column types with one typed parquet load.

    A declared managed table has NO schema until its first load — querying it
    first returns "declared but has no data". Since Hotdata has no ALTER path
    (§9.1), that first load decides the types permanently. Loading JSON would let
    the types be inferred from whichever columns happened to be non-null, so we
    seed from telemetry.schema.ARROW_SCHEMA instead, which is the frozen spec.

    The seed row is tagged event_type='schema_seed'. Every query in queries.sql
    filters on a real event_type, so it never shows up in results.
    """
    import hotdata

    from agents.hotdata_scope import DEFAULT_SCHEMA, client as make_client
    from telemetry.schema import ARROW_SCHEMA, TABLE, blank_row

    c = client_ or make_client()
    row = blank_row()
    row.update(event_id="schema_seed", run_id="schema_seed", event_type="schema_seed",
               agent="pipeline", pipeline_version="seed", mode="parallel",
               scenario_id="seed", wave=0, ts=datetime.now(timezone.utc),
               success=True, duration_ms=0.0, tokens_in=0, tokens_out=0,
               cost_usd=0.0, query_count=0, rows_returned=0, retry_count=0,
               score=0, hit_service=False, hit_fault=False)
    tmp = Path(tempfile.mkdtemp()) / "seed.parquet"
    pq.write_table(pa.table({n: [row[n]] for n in ARROW_SCHEMA.names},
                            schema=ARROW_SCHEMA), tmp)

    upload_id = hotdata.UploadsApi(c).upload_file(
        tmp, filename="seed.parquet",
        content_type="application/vnd.apache.parquet").upload_id
    hotdata.DatabasesApi(c).load_database_table(
        db_id, DEFAULT_SCHEMA, TABLE,
        hotdata.LoadManagedTableRequest(upload_id=upload_id, format="parquet",
                                        mode="replace"))


def create_telemetry_db(existing: str | None = None) -> None:
    """Create (or repair) the event-scoped telemetry DB and its frozen table."""
    import hotdata

    from agents.hotdata_scope import DEFAULT_SCHEMA, client
    from telemetry.schema import TABLE

    c = client()
    api = hotdata.DatabasesApi(c)

    if existing:
        db_id = existing
        print(f"repairing existing telemetry DB {db_id}")
    else:
        db = api.create_database(hotdata.CreateDatabaseRequest(
            name=f"isw-telemetry-{time.strftime('%Y%m%d')}",
            default_schema=DEFAULT_SCHEMA))
        db_id = getattr(db, "id", None) or getattr(getattr(db, "database", None), "id", None)

    try:
        api.add_database_table(db_id, DEFAULT_SCHEMA,
                               hotdata.AddManagedTableRequest(name=TABLE))
    except hotdata.ApiException as exc:
        if exc.status not in (400, 409):
            raise

    seed_schema(db_id, c)

    resp = hotdata.QueryApi(c).query(hotdata.QueryRequest(
        database_id=db_id, sql=f"SELECT COUNT(*) AS n FROM {TABLE}",
        default_schema=DEFAULT_SCHEMA))
    print(f"\nevents table ready ({(resp.rows or [[0]])[0][0]} seed row).")
    print(f"Add this to .env:\n\n    TELEMETRY_DB_ID={db_id}\n")
    print("This DB is event-scoped: never tear it down. Every run today writes here.")


def main() -> int:
    ap = argparse.ArgumentParser(description="Verify the stack end to end.")
    ap.add_argument("--create-telemetry-db", action="store_true")
    ap.add_argument("--repair-telemetry-db", metavar="DB_ID",
                    help="seed the frozen schema on an existing telemetry DB")
    ap.add_argument("--skip-gate1", action="store_true", help="skip the live DB cycle")
    ap.add_argument("--list-models", action="store_true",
                    help="list the models this OpenAI key can use, then exit")
    args = ap.parse_args()

    if args.list_models:
        list_models()
        return 0

    print("IncidentSwarm preflight\n")
    have_env = check_env()
    print()

    if args.create_telemetry_db or args.repair_telemetry_db:
        if not (os.getenv("HOTDATA_API_KEY") and os.getenv("HOTDATA_WORKSPACE")):
            print("\nCannot touch the telemetry DB without hotdata credentials.")
            return 1
        create_telemetry_db(args.repair_telemetry_db)
        return 0

    check_data()
    print()
    if have_env and check_hotdata_reachable() and not args.skip_gate1:
        print()
        gate1_cycle()
        print()
        check_telemetry_db()
    print()
    if os.getenv("OPENAI_API_KEY"):
        check_models()
    else:
        record("model: fast/strong", BAD, "OPENAI_API_KEY is not set")

    failures = [r for r in _results if r[1] == BAD]
    print(f"\n{len(_results) - len(failures)}/{len(_results)} checks passed")
    if failures:
        print("\nBlocking:")
        for name, _, detail in failures:
            print(f"  - {name}: {detail}")
        return 1
    print("\nStack is ready. Next: python -m bench.run_batch --mode both --version v1 --seeds 1-2")
    return 0


if __name__ == "__main__":
    sys.exit(main())
