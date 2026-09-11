"""Live Hotdata telemetry and RCA reports: streamlit run dashboard/app.py."""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config  # noqa: E402,F401 — loads .env
from telemetry.queries import load_queries  # noqa: E402

st.set_page_config(page_title="IncidentSwarm · Mission control", page_icon="◈", layout="wide")


def run_query(sql: str) -> pd.DataFrame:
    """Always query Hotdata; no cached results or local substitutes."""
    import hotdata
    from agents.hotdata_scope import client

    db_id = os.getenv("TELEMETRY_DB_ID")
    if not db_id:
        raise RuntimeError("TELEMETRY_DB_ID is not set — see .env.example")
    with client() as api_client:
        resp = hotdata.QueryApi(api_client).query(
            hotdata.QueryRequest(database_id=db_id, sql=sql, default_schema="main"))
    return pd.DataFrame(resp.rows or [], columns=list(resp.columns or []))


def table(df: pd.DataFrame, empty: str = "No matching telemetry yet.") -> None:
    if df.empty:
        st.info(empty)
    else:
        display = df.copy()
        numeric = display.select_dtypes(include="number").columns
        display[numeric] = display[numeric].round(3)
        st.dataframe(display, hide_index=True)


def report_view(df: pd.DataFrame) -> None:
    st.subheader("Investigation reports")
    st.caption("The latest 50 run outcomes, with the report and score saved by the runner.")
    if df.empty:
        st.info("Complete an investigation to see its root cause, evidence, and next steps here.")
        return
    selected = st.selectbox("Investigation", df["run_id"].tolist())
    row = df.loc[df.run_id == selected].iloc[0]
    st.caption(f"{row.scenario_id} · {row.pipeline_version} · {row['mode']} · {row.ts} UTC")
    if row.success == False:  # noqa: E712 — pandas may return numpy.bool_
        st.error("This run failed before a scored report was produced.")
        if row.error_msg:
            st.code(str(row.error_msg), language=None)
        return
    try:
        payload = json.loads(row.payload or "{}")
    except (TypeError, ValueError):
        st.warning("This run has an unreadable report payload.")
        return
    report = payload.get("rca", {})
    root = report.get("root_cause", report)  # older runs stored only the root cause
    cols = st.columns(3)
    cols[0].metric("Root service", root.get("service") or "Unknown")
    cols[1].metric("Fault", (root.get("fault_type") or "unknown").replace("_", " "))
    cols[2].metric("RCA score", f"{int(row.score)}/4" if pd.notna(row.score) else "—")
    if report.get("summary"):
        st.write(report["summary"])
    if payload.get("components"):
        st.caption("Score components: " + " · ".join(
            f"{name.replace('_', ' ')} {'✓' if hit else '✗'}"
            for name, hit in payload["components"].items()))
    with st.expander("Evidence and full report", expanded=True):
        st.json(report)
    st.download_button("Download report JSON", json.dumps(payload, indent=2),
                       file_name=f"{selected}.json", mime="application/json")


def main() -> None:
    st.caption("INCIDENT RESPONSE / EXPERIMENT TELEMETRY")
    st.title("One incident. Multiple perspectives.")
    st.write("IncidentSwarm investigates logs, metrics, changes, and infrastructure in parallel, "
             "with an optional history specialist. Compare its root-cause analysis with a "
             "single-agent baseline.")

    with st.sidebar:
        st.title("◈ IncidentSwarm")
        st.caption("Live mission control")
        st.button("Refresh telemetry", use_container_width=True)
        st.divider()
        st.markdown("**Investigation flow**\n\n"
                    "1. Provision isolated databases\n"
                    "2. Investigate each source\n"
                    "3. Correlate the evidence\n"
                    "4. Score and clean up")
        st.divider()
        st.caption("Source: persistent Hotdata telemetry. Queries run again on every refresh.")

    required = ("HOTDATA_API_KEY", "HOTDATA_WORKSPACE", "TELEMETRY_DB_ID")
    if any(not os.getenv(name) for name in required):
        st.info("Connect your telemetry database to start exploring investigations.")
        st.markdown("Set `HOTDATA_API_KEY`, `HOTDATA_WORKSPACE`, and `TELEMETRY_DB_ID` "
                    "in `.env`. Create the telemetry database once, then save its ID:")
        st.code("uv run python -m bench.preflight --create-telemetry-db", language="bash")
        st.caption("The dashboard only reads telemetry; it does not launch agents or create databases.")
        return

    queries = load_queries()
    frames: dict[str, pd.DataFrame] = {}
    errors: dict[str, str] = {}
    with st.spinner("Querying live investigation telemetry…"):
        for name, sql in queries.items():
            try:
                frames[name] = run_query(sql)
            except Exception as exc:  # noqa: BLE001 — retain the other panels
                frames[name] = pd.DataFrame()
                errors[name] = str(exc)
    refreshed = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    st.caption(f"Queried at {refreshed} · {len(queries) - len(errors)}/{len(queries)} queries succeeded")
    if errors:
        st.warning("Some live queries are unavailable. Visible panels use only successful responses.")
        with st.expander("Connection and query details"):
            for name, error in errors.items():
                st.text(f"{name}: {error}")
    if len(errors) == len(queries):
        return

    versions = sorted({str(v) for df in frames.values() if "pipeline_version" in df
                       for v in df.pipeline_version.dropna()})
    chosen = st.sidebar.multiselect("Pipeline versions", versions, default=versions)
    frames = {name: df[df.pipeline_version.isin(chosen)] if "pipeline_version" in df else df
              for name, df in frames.items()}
    summary = frames["Q4"]
    if summary.empty:
        st.info("No completed or failed runs for this selection. Run a small paired benchmark:")
        st.code("uv run python -m bench.run_batch --mode both --version v1 --seeds 1 --concurrency 1",
                language="bash")
    else:
        completed = int(summary.completed_runs.sum())
        failed = int(summary.failed_runs.sum())
        weighted_score = (summary.avg_score * summary.completed_runs).sum()
        weighted_cost = (summary.avg_cost_usd * summary.completed_runs).sum()
        cols = st.columns(4)
        cols[0].metric("Completed investigations", str(completed))
        cols[1].metric("Mean RCA score", f"{weighted_score / completed:.2f}/4" if completed else "—")
        cols[2].metric("Estimated model cost · completed", f"${weighted_cost:.4f}" if completed else "—")
        cols[3].metric("Failed investigations", str(failed))
        st.caption("Scores and costs cover completed runs. Model cost uses configured token rates; "
                   "it excludes infrastructure, warm-up probes, and failed runs.")

    overview, agents, data, failures, reports = st.tabs(
        ["Benchmark", "Agent contributions", "Data & concurrency", "Failures", "RCA reports"])
    with overview:
        st.subheader("Does parallel investigation pay off?")
        st.caption("Compare score, wall time, and model cost across versions and modes.")
        table(summary)
        st.subheader("Paired scenario comparison")
        st.caption("Speedup > 1 means parallel was faster. Positive score delta means it scored higher. "
                   "Pairs match v1 with single-v1, v2 with single-v2, and so on.")
        table(frames["Q6"], "No matched parallel and single runs yet. Run both modes under the same version.")
        st.caption("The arms share model tiers and scoring, but differ in prompts, context layout, "
                   "and reasoning turns. With history disabled, their available data and budgets also differ.")
    with agents:
        st.subheader("Which sources find the root cause?")
        st.caption("Hit rate requires both the root service and fault type to match seeded ground truth.")
        hits = frames["Q1"]
        if not hits.empty:
            chart = hits.assign(label=hits.pipeline_version + " / " + hits.agent)
            st.bar_chart(chart, x="label", y="hit_rate", color="fault_type", stack=False)
        table(hits)
        st.subheader("Recurring bottlenecks")
        table(frames["Q2"])
    with data:
        st.subheader("Query behavior")
        st.caption("Query counts include failed attempts. Latencies are milliseconds.")
        table(frames["Q3"])
        st.subheader("How much work overlaps?")
        st.caption("Effective parallelism = total agent duration / longest agent duration. "
                   "This estimates overlap from agent timings; it is not end-to-end speedup.")
        table(frames["Q7"])
    with failures:
        st.subheader("Repeated errors and retries")
        table(frames["Q5"], "No error or retry events for the selected versions.")
    with reports:
        report_view(frames["Q8"])
    with st.expander("SQL behind this view"):
        name = st.selectbox("Query", list(queries))
        st.code(queries[name], language="sql")


if __name__ == "__main__":
    main()
