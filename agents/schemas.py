"""Shared agent contracts (plan.md §7.1).

Every wave-1 agent returns a HypothesisSet. The correlator — and the single-agent
baseline's synthesis call — returns an RCAReport. Both are validated with pydantic;
on failure the runner re-prompts once (emit `retry`), then gives up with an empty
set (emit `error`). One agent failing must not kill the run.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field, field_validator


class FaultType(str, Enum):
    """The §5.3 catalog plus `unknown`. `unknown` never scores a match (§6)."""

    BAD_DEPLOY = "bad_deploy"
    FLAG_FLIP = "flag_flip"
    POOL_EXHAUSTION = "pool_exhaustion"
    CPU_THROTTLE = "cpu_throttle"
    THIRD_PARTY_LATENCY = "third_party_latency"
    MEMORY_LEAK = "memory_leak"
    UNKNOWN = "unknown"


class AgentName(str, Enum):
    LOGS = "logs"
    METRICS = "metrics"
    CHANGES = "changes"
    INFRA = "infra"
    HISTORY = "history"


class Source(str, Enum):
    CHANGES = "changes"
    METRICS = "metrics"
    LOGS = "logs"
    K8S = "k8s"
    HISTORY = "history"


class Hypothesis(BaseModel):
    """Agents may set only the fields their own source can support: ChangeAgent can
    name a trigger_event_id, MetricsAgent cannot."""

    service: str
    fault_type: FaultType
    trigger_event_id: str | None = None
    start_ts: datetime | None = None
    confidence: float = Field(ge=0.0, le=1.0)
    evidence: list[str] = Field(default_factory=list)


class HypothesisSet(BaseModel):
    agent: AgentName
    hypotheses: list[Hypothesis] = Field(default_factory=list, max_length=3)
    queries_used: int = 0
    notes: str = ""

    @field_validator("hypotheses")
    @classmethod
    def _sorted_by_confidence(cls, v: list[Hypothesis]) -> list[Hypothesis]:
        return sorted(v, key=lambda h: h.confidence, reverse=True)

    @property
    def top(self) -> Hypothesis | None:
        """Top-confidence hypothesis — what `hit_service` / `hit_fault` score against (§6)."""
        return self.hypotheses[0] if self.hypotheses else None

    @classmethod
    def empty(cls, agent: AgentName | str) -> HypothesisSet:
        """Returned after a second validation failure, so one bad agent can't kill a run."""
        return cls(agent=AgentName(agent), hypotheses=[], notes="validation failed")


class RootCause(BaseModel):
    service: str
    fault_type: FaultType
    trigger_event_id: str | None = None
    start_ts: datetime | None = None
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)


class TimelineEntry(BaseModel):
    ts: datetime
    source: Source
    event: str


class RuledOut(BaseModel):
    candidate: str
    reason: str


class RCAReport(BaseModel):
    """Correlator output, and the single-agent baseline's synthesis output."""

    root_cause: RootCause
    timeline: list[TimelineEntry] = Field(default_factory=list)
    ruled_out: list[RuledOut] = Field(default_factory=list)
    summary: str = ""


class Truth(BaseModel):
    """Ground truth, per scenario. NEVER loaded into any DB and never shown to an agent.

    The runner joins truth-derived columns (fault_type, hit_*) onto telemetry rows
    outside agent context (plan.md §3, isolation rule).
    """

    scenario_id: str
    service: str
    fault_type: FaultType
    trigger_event_id: str | None = None
    start_ts: datetime
    red_herrings: list[str] = Field(default_factory=list)
