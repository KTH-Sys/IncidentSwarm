"""Offline regressions for cleanup, benchmark integrity, and dashboard states."""

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

import config  # noqa: F401
import duckdb

from agents import llm, pipeline
from agents.hotdata_scope import BudgetExceeded, Provisioned, ScopedDB
from agents.schemas import RCAReport, RootCause, Truth
from gen.scenarios import parse_seeds
from telemetry.emit import _to_table
from telemetry.queries import load_queries

ROOT = Path(__file__).resolve().parents[1]


class MemorySink:
    def __init__(self):
        self.rows = []

    def write(self, rows):
        self.rows.extend(rows)


def truth():
    return Truth(scenario_id="s01", service="payments", fault_type="bad_deploy",
                 trigger_event_id="chg_1", start_ts=datetime(2026, 9, 11, tzinfo=timezone.utc))


def fixture_rows():
    rows = []
    for version, mode, duration, score in [
        ("v1", "parallel", 100, 4), ("single-v1", "single", 200, 3),
        ("v2", "parallel", 50, 4), ("single-v2", "single", 150, 4),
    ]:
        report = {"root_cause": {"service": "payments", "fault_type": "bad_deploy"},
                  "summary": "A deployment preceded the error increase.", "timeline": []}
        rows.append(dict(run_id=version, event_type="run_end", scenario_id="s01",
                         pipeline_version=version, mode=mode, success=True, score=score,
                         duration_ms=duration, cost_usd=0.01, tokens_in=100, tokens_out=20,
                         ts=datetime(2026, 9, 11), fault_type="bad_deploy",
                         payload=json.dumps({"rca": report, "components": {"service": True}})))
    rows.append(dict(rows[0], run_id="failed", success=False, score=None,
                     duration_ms=5000, cost_usd=None, payload=None, error_msg="load failed"))
    for agent in ("logs", "metrics"):
        rows.append(dict(run_id="v1", event_type="agent_end", pipeline_version="v1",
                         mode="parallel", wave=1, agent=agent, fault_type="bad_deploy",
                         duration_ms=40, hit_service=True, hit_fault=True, query_count=2,
                         cost_usd=0.001, success=True))
        rows.append(dict(run_id="v1", event_type="query", pipeline_version="v1", agent=agent,
                         db_id=agent, query_kind="sql", success=False, duration_ms=1,
                         error_msg="query failed"))
    return rows


def database(rows):
    con = duckdb.connect()
    con.register("seed_events", _to_table(rows))
    con.execute("CREATE TABLE events AS SELECT * FROM seed_events")
    return con


class PipelineTests(unittest.TestCase):
    def test_failed_run_cleans_up_and_flushes_terminal_event(self):
        sink = MemorySink()

        def fail(*args, state, **kwargs):
            state["prov"] = Provisioned("batch", ["db1", "db2"])
            raise RuntimeError("load failed")

        with patch.object(pipeline, "load_truth", return_value=truth()), \
             patch.object(pipeline, "run_parallel", side_effect=fail), \
             patch.object(pipeline, "destroy", return_value=2) as destroy:
            with self.assertRaisesRegex(RuntimeError, "load failed"):
                pipeline.run_once("s01", "parallel", "v1", sink=sink, client_=object())
        self.assertEqual(destroy.call_args.kwargs["db_ids"], ["db1", "db2"])
        ended = [r for r in sink.rows if r["event_type"] == "run_end"]
        self.assertEqual(len(ended), 1)
        self.assertFalse(ended[0]["success"])
        self.assertIsNone(ended[0]["score"])
        self.assertTrue(any(r["event_type"] == "wave_end" and r["wave"] == 3 for r in sink.rows))

    def test_failure_before_provision_is_still_recorded(self):
        sink = MemorySink()
        with patch.object(pipeline, "load_truth", return_value=truth()), \
             patch.object(pipeline, "run_parallel", side_effect=RuntimeError("provision failed")):
            with self.assertRaises(RuntimeError):
                pipeline.run_once("s01", "parallel", "v1", sink=sink, client_=object())
        self.assertEqual(sink.rows[-1]["event_type"], "run_end")
        self.assertFalse(sink.rows[-1]["success"])

    def test_completed_run_retains_full_report(self):
        sink = MemorySink()
        rc = truth().model_dump(exclude={"scenario_id", "red_herrings"})
        report = RCAReport(root_cause=RootCause(**rc), summary="Deployment caused errors.")
        with patch.object(pipeline, "load_truth", return_value=truth()), \
             patch.object(pipeline, "run_parallel", return_value=(report, {})):
            result = pipeline.run_once("s01", "parallel", "v1", sink=sink, client_=object())
        self.assertEqual(result.score, 4)
        row = next(r for r in sink.rows if r["event_type"] == "run_end")
        self.assertEqual(json.loads(row["payload"])["rca"]["summary"], report.summary)

    def test_invalid_mode_fails_before_any_io(self):
        with self.assertRaises(ValueError):
            pipeline.run_once("s01", "typo", "v1")

    def test_scoped_sql_preserves_client_and_budget(self):
        from agents.hotdata_scope import hotdata
        from telemetry.emit import Emitter, RunContext
        client = object()
        em = Emitter(RunContext("run", "v1", "parallel", "s01"), MemorySink())
        scope = ScopedDB("metrics", "private-db", 1, client=client, em=em)
        response = SimpleNamespace(columns=["n"], rows=[[1]], row_count=1, execution_time_ms=1)
        with patch.object(hotdata, "QueryApi") as api:
            api.return_value.query.return_value = response
            self.assertEqual(scope.sql("SELECT 1 AS n"), [{"n": 1}])
            api.assert_called_with(client)
            self.assertEqual(api.return_value.query.call_args.args[0].database_id, "private-db")
            with self.assertRaises(BudgetExceeded):
                scope.sql("SELECT 2 AS n")
            self.assertEqual(api.return_value.query.call_count, 1)

    def test_prompt_hash_includes_shared_instructions(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(llm, "PROMPT_DIR", Path(tmp)):
            (Path(tmp) / "logs.md").write_text("prefix {{ _shared.md }}")
            (Path(tmp) / "_shared.md").write_text("shared")
            llm.load_prompt.cache_clear()
            llm.prompt_hash.cache_clear()
            try:
                self.assertEqual(llm.prompt_hash("logs"), hashlib.sha1(b"prefix shared").hexdigest()[:8])
            finally:
                llm.load_prompt.cache_clear()
                llm.prompt_hash.cache_clear()

    def test_seed_validation(self):
        self.assertEqual(parse_seeds(["1-3", "2", "99"]), [1, 2, 3, 99])
        for tokens in ([], ["3-1"], ["-1"], ["bad"], ["1-"]):
            with self.subTest(tokens=tokens), self.assertRaises(ValueError):
                parse_seeds(tokens)

    def test_batch_reports_partial_failure_and_keeps_local_backlog(self):
        from bench import run_batch
        result = SimpleNamespace(scenario_id="s01", score=4, wall_ms=100, cost_usd=.01,
                                 queries=1, mode="parallel")
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "s01").mkdir()
            (Path(tmp) / "s01" / "truth.json").write_text("{}")
            argv = ["run_batch", "--mode", "both", "--version", "v1", "--seeds", "1",
                    "--concurrency", "1", "--local-telemetry", "--no-sweep", "--data", tmp]
            with patch("sys.argv", argv), patch.object(run_batch, "client"), \
                 patch.object(run_batch, "make_sink", return_value=MemorySink()), \
                 patch.object(run_batch, "run_once", side_effect=[result, RuntimeError("failed")]), \
                 patch.object(run_batch, "replay_backlog") as replay, patch("builtins.print"):
                self.assertEqual(run_batch.main(), 1)
                replay.assert_not_called()


class QueryTests(unittest.TestCase):
    def setUp(self):
        self.con = database(fixture_rows())
        self.queries = load_queries()

    def tearDown(self):
        self.con.close()

    def test_all_queries_execute_and_pairs_match_versions(self):
        for name, sql in self.queries.items():
            with self.subTest(query=name):
                self.con.execute(sql).fetchall()
        pairs = self.con.execute(self.queries["Q6"]).df()
        self.assertEqual(len(pairs), 2)
        self.assertEqual(dict(zip(pairs.pipeline_version, pairs.speedup)), {"v1": 2, "v2": 3})
        self.assertEqual(set(pairs.baseline_version), {"single-v1", "single-v2"})

    def test_failures_are_counted_but_excluded_from_performance(self):
        rows = self.con.execute(self.queries["Q4"]).df()
        v1 = rows[rows.pipeline_version == "v1"].iloc[0]
        self.assertEqual(v1.runs, 2)
        self.assertEqual(v1.failed_runs, 1)
        self.assertEqual(v1.completed_runs, 1)
        self.assertEqual(v1.avg_score, 4)
        self.assertEqual(v1.avg_wall_ms, 100)
        errors = self.con.execute(self.queries["Q5"]).df()
        self.assertEqual(errors.n.sum(), 2)

    def test_empty_database(self):
        self.con.execute("DELETE FROM events")
        for name, sql in self.queries.items():
            with self.subTest(query=name):
                self.assertEqual(self.con.execute(sql).fetchall(), [])


class DashboardTests(unittest.TestCase):
    def test_unconfigured_dashboard_has_setup_guidance(self):
        from streamlit.testing.v1 import AppTest
        with patch.dict(os.environ, HOTDATA_API_KEY="", HOTDATA_WORKSPACE="", TELEMETRY_DB_ID=""):
            app = AppTest.from_file(str(ROOT / "dashboard/app.py")).run(timeout=15)
        self.assertFalse(app.exception)
        self.assertIn("Connect your telemetry", app.info[0].value)

    def test_populated_dashboard_and_version_filter(self):
        from streamlit.testing.v1 import AppTest
        import hotdata
        con = database(fixture_rows())

        def query(request):
            result = con.execute(request.sql)
            return SimpleNamespace(columns=[d[0] for d in result.description], rows=result.fetchall())

        try:
            with patch.dict(os.environ, HOTDATA_API_KEY="test", HOTDATA_WORKSPACE="test", TELEMETRY_DB_ID="test"), \
                 patch("agents.hotdata_scope.client", return_value=MagicMock()), \
                 patch.object(hotdata, "QueryApi") as api:
                api.return_value.query.side_effect = query
                app = AppTest.from_file(str(ROOT / "dashboard/app.py")).run(timeout=20)
                self.assertFalse(app.exception)
                self.assertEqual(app.metric[0].value, "4")
                self.assertEqual(app.metric[3].value, "1")
                app.sidebar.multiselect[0].set_value(["v2"]).run(timeout=20)
                self.assertFalse(app.exception)
                self.assertEqual(app.metric[0].value, "1")
                self.assertEqual(app.metric[3].value, "0")
                self.assertIn("payments", [m.value for m in app.metric])
        finally:
            con.close()

    def test_query_failure_does_not_crash_dashboard(self):
        from streamlit.testing.v1 import AppTest
        import hotdata
        with patch.dict(os.environ, HOTDATA_API_KEY="test", HOTDATA_WORKSPACE="test", TELEMETRY_DB_ID="test"), \
             patch("agents.hotdata_scope.client", return_value=MagicMock()), \
             patch.object(hotdata, "QueryApi") as api:
            api.return_value.query.side_effect = RuntimeError("Unavailable")
            app = AppTest.from_file(str(ROOT / "dashboard/app.py")).run(timeout=15)
        self.assertFalse(app.exception)
        self.assertTrue(app.warning)


if __name__ == "__main__":
    unittest.main()
