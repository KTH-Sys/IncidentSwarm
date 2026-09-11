"""Live telemetry dashboard (plan.md §11, 14:30-15:00).

    streamlit run dashboard/app.py

Layout: Q4 and Q6 headline cards at top, Q1 heatmap, then Q2/Q3/Q5/Q7 tables.
EVERY query runs on page load — the judges are told these are live, so nothing
here may read a cached CSV.

Rounding happens here, not in SQL (§9.3, dialect safety).
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import pandas as pd
import streamlit as st

QUERIES_PATH = Path(__file__).resolve().parent.parent / "telemetry" / "queries.sql"

st.set_page_config(page_title="IncidentSwarm telemetry", layout="wide")


def load_queries(path: Path = QUERIES_PATH) -> dict[str, str]:
    """Split queries.sql on `-- name: Qn` markers."""
    text = path.read_text()
    parts = re.split(r"^--\s*name:\s*(\S+)\s*$", text, flags=re.MULTILINE)
    return {parts[i].strip(): parts[i + 1].strip() for i in range(1, len(parts) - 1, 2)}


def run_query(sql: str) -> pd.DataFrame:
    """Execute against the persistent telemetry DB.

    TODO: wire to the Hotdata SDK using TELEMETRY_DB_ID. No caching — every
    query must run on page load (§2, "queries run live in the demo").
    """
    db_id = os.getenv("TELEMETRY_DB_ID")
    if not db_id:
        raise RuntimeError("TELEMETRY_DB_ID is not set — see .env.example")
    raise NotImplementedError("wire to the Hotdata SDK")


def show(name: str, caption: str, q: dict[str, str], *, kind: str = "table") -> None:
    st.subheader(f"{name} — {caption}")
    try:
        df = run_query(q[name])
    except Exception as exc:  # noqa: BLE001 — a dead panel must not blank the demo
        st.warning(f"{name} unavailable: {exc}")
        return
    if df.empty:
        st.info("no rows yet")
        return
    if kind == "heatmap":
        st.dataframe(
            df.pivot(index="agent", columns="fault_type", values="hit_rate")
              .style.background_gradient(cmap="Greens", vmin=0, vmax=1)
              .format("{:.0%}"),
            use_container_width=True,
        )
    else:
        st.dataframe(df.round(3), use_container_width=True)


def main() -> None:
    q = load_queries()

    st.title("IncidentSwarm")
    st.caption("Five isolated agents investigate one incident in parallel. "
               "Every number below is queried live from the telemetry DB.")

    # --- Headlines: Q4 (v1 vs v2 vs baseline) and Q6 (parallel vs single) ---
    st.header("Headline")
    c1, c2 = st.columns(2)
    with c1:
        show("Q4", "did v2 beat v1?", q)
    with c2:
        show("Q6", "parallel vs single, per scenario", q)

    # --- The finding that drove the v1 -> v2 change ---
    st.header("Which agent actually finds the root cause")
    show("Q1", "hit rate by agent and fault type", q, kind="heatmap")

    # --- Supporting detail ---
    st.header("Under the hood")
    show("Q7", "fan-out efficiency", q)
    show("Q2", "wave-1 bottleneck", q)
    show("Q3", "query volume, shape, latency", q)
    show("Q5", "retries and errors", q)


if __name__ == "__main__":
    main()
