"""Scoring (plan.md §6).

0-4, one point per component:
  service          exact match
  fault_type       exact match (enum; `unknown` never matches)
  trigger_event_id exact match; null matches null
  start_ts         |predicted - truth| <= 120s

Per-agent hits for telemetry use the agent's TOP-confidence hypothesis.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from agents.schemas import FaultType, HypothesisSet, RCAReport, Truth

START_TS_TOLERANCE_S = 120
COMPONENTS = ("service", "fault_type", "trigger_event_id", "start_ts")


def _match_service(pred: str | None, truth: str) -> bool:
    return bool(pred) and pred == truth


def _match_fault(pred: FaultType | str | None, truth: FaultType | str) -> bool:
    if pred is None:
        return False
    pred, truth = FaultType(pred), FaultType(truth)
    # `unknown` never matches — not even unknown against unknown.
    return pred is not FaultType.UNKNOWN and pred == truth


def _match_trigger(pred: str | None, truth: str | None) -> bool:
    return pred == truth  # null matches null


def _match_start(pred: datetime | None, truth: datetime | None) -> bool:
    if pred is None or truth is None:
        return False
    return abs((pred - truth).total_seconds()) <= START_TS_TOLERANCE_S


def score_report(report: RCAReport, truth: Truth) -> dict[str, Any]:
    """Returns {score, components{...}, predicted, truth}."""
    rc = report.root_cause
    components = {
        "service": _match_service(rc.service, truth.service),
        "fault_type": _match_fault(rc.fault_type, truth.fault_type),
        "trigger_event_id": _match_trigger(rc.trigger_event_id, truth.trigger_event_id),
        "start_ts": _match_start(rc.start_ts, truth.start_ts),
    }
    return {
        "score": sum(components.values()),
        "components": components,
        "predicted": rc.model_dump(mode="json"),
        "truth": truth.model_dump(mode="json"),
    }


def score_agent(hset: HypothesisSet, truth: Truth) -> dict[str, bool]:
    """hit_service / hit_fault for an agent_end row, from its top hypothesis."""
    top = hset.top
    if top is None:
        return {"hit_service": False, "hit_fault": False}
    return {
        "hit_service": _match_service(top.service, truth.service),
        "hit_fault": _match_fault(top.fault_type, truth.fault_type),
    }


def load_truth(scenario_id: str, data_dir: Path = Path("data")) -> Truth:
    path = Path(data_dir) / scenario_id / "truth.json"
    return Truth.model_validate(json.loads(path.read_text()))


# --- Gate check (§11, 11:45-12:30): perfect scores 4, empty scores 0. ---

def _selfcheck() -> None:
    truth = Truth(
        scenario_id="s07",
        service="payments",
        fault_type=FaultType.POOL_EXHAUSTION,
        trigger_event_id="chg_0031",
        start_ts=datetime.fromisoformat("2026-09-11T14:03:00+00:00"),
        red_herrings=["chg_0012"],
    )
    perfect = RCAReport.model_validate(
        {
            "root_cause": {
                "service": "payments",
                "fault_type": "pool_exhaustion",
                "trigger_event_id": "chg_0031",
                "start_ts": "2026-09-11T14:04:30+00:00",  # 90s off, inside tolerance
                "confidence": 0.9,
            },
            "summary": "db pool capped at 10",
        }
    )
    empty = RCAReport.model_validate(
        {"root_cause": {"service": "", "fault_type": "unknown", "confidence": 0.0}}
    )
    assert score_report(perfect, truth)["score"] == 4, "perfect report must score 4"
    assert score_report(empty, truth)["score"] == 0, "empty report must score 0"

    far = perfect.model_copy(deep=True)
    far.root_cause.start_ts = datetime.fromisoformat("2026-09-11T14:06:00+00:00")  # 180s
    assert score_report(far, truth)["components"]["start_ts"] is False
    print("score.py selfcheck OK: perfect=4, empty=0, tolerance enforced")


def test_perfect_and_empty() -> None:  # pytest entry point
    _selfcheck()


if __name__ == "__main__":
    _selfcheck()
