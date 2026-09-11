"""Wave 2 — fan-in (plan.md §7.2), and the single-agent baseline's synthesis.

The correlator has no database. It merges five HypothesisSets plus symptoms[]
into one RCAReport using the heuristics in agents/prompts/correlator.md.

The baseline uses the SAME prompt for its final synthesis, so the only variable
between the two arms is parallel vs sequential (§3).
"""

from __future__ import annotations

import json
import time
from typing import Any

from agents.llm import STRONG_MODEL, Usage, load_prompt, prompt_hash, validated_call
from agents.schemas import HypothesisSet, RCAReport, RootCause, FaultType
from telemetry.emit import Emitter


def _onset_table(symptoms: list[dict]) -> str:
    """Onset ordering, computed rather than inferred.

    The correlator's first heuristic is "earliest onset beats largest magnitude",
    and wave-0 triage already knows the answer deterministically. Handing it over
    as a table costs nothing and removes an arithmetic step from the model.
    """
    if not symptoms:
        return "(no symptomatic services)"
    lines = ["rank | service | onset | peak_err_rate | peak_p99_ms"]
    for i, s in enumerate(symptoms, 1):
        lines.append(f"{i} | {s.get('service')} | {s.get('onset_ts')} | "
                     f"{s.get('peak_err_rate')} | {s.get('peak_p99_ms')}")
    return "\n".join(lines)


def run_correlator(hsets: list[HypothesisSet], symptoms: list[dict], em: Emitter, *,
                   model: str = STRONG_MODEL, wave: int = 2) -> tuple[RCAReport, Usage]:
    """Merge wave-1 output into one RCAReport. Never raises."""
    usage = Usage()
    t0 = time.monotonic()
    em.event("agent_start", agent="correlator", wave=wave, model=model,
             prompt_hash=prompt_hash("correlator"))

    payload = {
        "symptoms": symptoms,
        "onset_order": _onset_table(symptoms),
        "agent_findings": [h.model_dump(mode="json") for h in hsets],
    }
    user = (
        "Five source agents investigated this incident in parallel, each with access "
        "to only one data source. Their findings and the wave-0 triage follow.\n\n"
        f"Onset ordering (computed, authoritative):\n{payload['onset_order']}\n\n"
        f"{json.dumps({'symptoms': symptoms, 'agent_findings': payload['agent_findings']}, indent=2, default=str)}\n\n"
        "Return ONLY the RCAReport JSON."
    )

    report = validated_call(model=model, system=load_prompt("correlator"),
                            messages=[{"role": "user", "content": user}],
                            schema=RCAReport, em=em, agent="correlator", wave=wave,
                            usage=usage, prompt_name="correlator")
    if report is None:
        report = _fallback_report(hsets, symptoms)

    em.event("agent_end", agent="correlator", wave=wave, model=model,
             duration_ms=(time.monotonic() - t0) * 1000,
             tokens_in=usage.tokens_in, tokens_out=usage.tokens_out,
             cost_usd=usage.cost_usd, retry_count=usage.retries,
             prompt_hash=prompt_hash("correlator"), success=report is not None,
             payload={"rca": report.model_dump(mode="json")})
    return report, usage


def _fallback_report(hsets: list[HypothesisSet], symptoms: list[dict]) -> RCAReport:
    """Deterministic fallback when the correlator itself fails to produce valid
    JSON twice. Takes the highest-confidence hypothesis across all agents, so a
    run still scores instead of being lost."""
    best = None
    for h in hsets:
        if h.top and (best is None or h.top.confidence > best.confidence):
            best = h.top
    if best is None:
        service = symptoms[0]["service"] if symptoms else ""
        return RCAReport(
            root_cause=RootCause(service=service, fault_type=FaultType.UNKNOWN),
            summary="Correlator failed to produce a valid report; no agent hypotheses.")
    return RCAReport(
        root_cause=RootCause(service=best.service, fault_type=best.fault_type,
                             trigger_event_id=best.trigger_event_id,
                             start_ts=best.start_ts, confidence=best.confidence),
        summary="Correlator output invalid; fell back to the highest-confidence agent hypothesis.")
