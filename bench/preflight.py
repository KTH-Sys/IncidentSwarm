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
    required = ("HOTDATA_API_KEY", "HOTDATA_WORKSPACE")
    optional = ("TELEMETRY_DB_ID", "ANTHROPIC_API_KEY", "HOTDATA_EMBEDDING_PROVIDER_ID")
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        record("env: hotdata credentials", BAD, f"missing {', '.join(missing)}")
        return False
    record("env: hotdata credentials", OK)
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


def check_anthropic() -> bool:
    try:
        from agents.llm import FAST_MODEL, client

        resp = client().messages.create(
            model=FAST_MODEL, max_tokens=8,
            messages=[{"role": "user", "content": "Reply with the single word: ok"}],
            temperature=0)
        record("anthropic: fast model", OK,
               f"{FAST_MODEL}, {resp.usage.input_tokens}+{resp.usage.output_tokens} tokens")
        return True
    except Exception as exc:  # noqa: BLE001
        record("anthropic: fast model", BAD, str(exc)[:200])
        return False


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


def create_telemetry_db() -> None:
    """Create the event-scoped telemetry DB and its frozen events table."""
    import hotdata

    from agents.hotdata_scope import DEFAULT_SCHEMA, client
    from telemetry.schema import TABLE, create_table_sql

    c = client()
    api = hotdata.DatabasesApi(c)
    db = api.create_database(hotdata.CreateDatabaseRequest(
        name=f"isw-telemetry-{time.strftime('%Y%m%d')}", default_schema=DEFAULT_SCHEMA))
    db_id = getattr(db, "id", None) or getattr(getattr(db, "database", None), "id", None)
    api.add_database_table(db_id, DEFAULT_SCHEMA,
                           hotdata.AddManagedTableRequest(name=TABLE))
    hotdata.QueryApi(c).query(hotdata.QueryRequest(
        database_id=db_id, sql=create_table_sql(), default_schema=DEFAULT_SCHEMA))
    print(f"\nTelemetry DB created. Add this to .env:\n\n    TELEMETRY_DB_ID={db_id}\n")
    print("This DB is event-scoped: never tear it down. Every run of the day writes here.")


def main() -> int:
    ap = argparse.ArgumentParser(description="Verify the stack end to end.")
    ap.add_argument("--create-telemetry-db", action="store_true")
    ap.add_argument("--skip-gate1", action="store_true", help="skip the live DB cycle")
    args = ap.parse_args()

    print("IncidentSwarm preflight\n")
    have_env = check_env()
    print()

    if args.create_telemetry_db:
        if not have_env:
            print("\nCannot create the telemetry DB without hotdata credentials.")
            return 1
        create_telemetry_db()
        return 0

    check_data()
    print()
    if have_env and check_hotdata_reachable() and not args.skip_gate1:
        print()
        gate1_cycle()
        print()
        check_telemetry_db()
    print()
    check_anthropic()

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
