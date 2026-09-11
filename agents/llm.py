"""Model calls with telemetry, cost accounting, and validated output (plan.md §7).

Two tiers (§7.2):
  fast   = Haiku-class, the five wave-1 source agents
  strong = Sonnet-class, the correlator and the baseline's synthesis call

Every call emits an `llm_call` row carrying tokens, cost, and model. Per-agent
totals are summed onto `agent_end`, per-run totals onto `run_end` — never SUM
across both (§9.1).

On a pydantic validation failure we re-prompt once with the error (emit `retry`);
a second failure emits `error` and returns an empty result. One agent failing
must not kill the run.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, TypeVar

import anthropic
from pydantic import BaseModel, ValidationError

from telemetry.emit import Emitter

T = TypeVar("T", bound=BaseModel)

PROMPT_DIR = Path(__file__).parent / "prompts"

FAST_MODEL = os.getenv("FAST_MODEL", "claude-haiku-4-5")
STRONG_MODEL = os.getenv("STRONG_MODEL", "claude-sonnet-5")

# USD per 1M tokens, from the Anthropic pricing table.
PRICING: dict[str, tuple[float, float]] = {
    "claude-haiku-4-5": (1.00, 5.00),
    "claude-sonnet-5": (2.00, 10.00),
    "claude-opus-5": (5.00, 25.00),
}

# Sampling params were REMOVED on Sonnet 5 and the 4.6+ family — sending
# temperature returns a 400. Haiku 4.5 still accepts it, so the fast tier runs
# at temperature 0 and the strong tier relies on the model's own determinism.
# Both arms of the experiment use the same strong model and prompt, so the
# parallel-vs-single control holds either way (see docs/verify.md).
SUPPORTS_TEMPERATURE = {"claude-haiku-4-5"}

MAX_TOKENS_FAST = 4096
MAX_TOKENS_STRONG = 8192


@dataclass
class Usage:
    """Accumulates across a turn so agent_end can carry per-agent totals."""

    tokens_in: int = 0
    tokens_out: int = 0
    cost_usd: float = 0.0
    calls: int = 0
    retries: int = 0

    def add(self, tin: int, tout: int, model: str) -> None:
        pin, pout = PRICING.get(model, (0.0, 0.0))
        self.tokens_in += tin
        self.tokens_out += tout
        self.cost_usd += (tin * pin + tout * pout) / 1_000_000
        self.calls += 1


@lru_cache(maxsize=32)
def load_prompt(name: str) -> str:
    """Load a prompt, expanding a single {{ _shared.md }} include."""
    text = (PROMPT_DIR / f"{name}.md").read_text()
    if "{{ _shared.md }}" in text:
        text = text.replace("{{ _shared.md }}", (PROMPT_DIR / "_shared.md").read_text())
    return text


@lru_cache(maxsize=32)
def prompt_hash(name: str) -> str:
    """sha1(prompt_file)[:8], stored in telemetry so a prompt change is visible
    in the data rather than only in git (§7.2)."""
    return hashlib.sha1((PROMPT_DIR / f"{name}.md").read_bytes()).hexdigest()[:8]


@lru_cache(maxsize=1)
def client() -> anthropic.Anthropic:
    if not os.getenv("ANTHROPIC_API_KEY"):
        raise RuntimeError("ANTHROPIC_API_KEY is not set — see .env.example")
    return anthropic.Anthropic()


def call(*, model: str, system: str, messages: list[dict], tools: list[dict] | None = None,
         em: Emitter, agent: str, wave: int, usage: Usage, prompt_name: str | None = None,
         max_tokens: int | None = None) -> Any:
    """One model call. Emits `llm_call` with tokens, cost, and duration."""
    kwargs: dict[str, Any] = {
        "model": model,
        "max_tokens": max_tokens or (MAX_TOKENS_FAST if model == FAST_MODEL else MAX_TOKENS_STRONG),
        "system": system,
        "messages": messages,
    }
    if tools:
        kwargs["tools"] = tools
    if model in SUPPORTS_TEMPERATURE:
        kwargs["temperature"] = 0

    t0 = time.monotonic()
    resp = client().messages.create(**kwargs)
    dur = (time.monotonic() - t0) * 1000

    tin = resp.usage.input_tokens
    tout = resp.usage.output_tokens
    usage.add(tin, tout, model)
    pin, pout = PRICING.get(model, (0.0, 0.0))
    em.event("llm_call", agent=agent, wave=wave, model=model, duration_ms=dur,
             tokens_in=tin, tokens_out=tout,
             cost_usd=(tin * pin + tout * pout) / 1_000_000,
             prompt_hash=prompt_hash(prompt_name) if prompt_name else None,
             success=True)
    return resp


def text_of(resp: Any) -> str:
    return "".join(b.text for b in resp.content if b.type == "text")


_FENCE = re.compile(r"```(?:json)?\s*(.*?)\s*```", re.DOTALL)


def extract_json(text: str) -> Any:
    """Agents are told to return bare JSON, but models sometimes fence it or add
    a sentence. Recover rather than burn a retry on formatting."""
    text = text.strip()
    if m := _FENCE.search(text):
        text = m.group(1).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start, end = text.find("{"), text.rfind("}")
        if start == -1 or end <= start:
            raise
        return json.loads(text[start:end + 1])


def parse_validated(text: str, schema: type[T]) -> T:
    return schema.model_validate(extract_json(text))


def validated_call(*, model: str, system: str, messages: list[dict], schema: type[T],
                   em: Emitter, agent: str, wave: int, usage: Usage,
                   prompt_name: str | None = None, max_tokens: int | None = None) -> T | None:
    """Call, validate against `schema`, and re-prompt once on failure.

    Returns None if both attempts fail — the caller substitutes an empty result.
    """
    convo = list(messages)
    for attempt in (0, 1):
        resp = call(model=model, system=system, messages=convo, em=em, agent=agent,
                    wave=wave, usage=usage, prompt_name=prompt_name, max_tokens=max_tokens)
        raw = text_of(resp)
        try:
            return parse_validated(raw, schema)
        except (ValidationError, json.JSONDecodeError) as exc:
            if attempt == 0:
                usage.retries += 1
                em.event("retry", agent=agent, wave=wave, success=False,
                         error_msg=str(exc)[:500])
                convo = convo + [
                    {"role": "assistant", "content": raw or "(empty)"},
                    {"role": "user", "content":
                        f"That did not validate against the required schema:\n{exc}\n\n"
                        "Return ONLY the corrected JSON object. No prose, no code fence."},
                ]
            else:
                em.event("error", agent=agent, wave=wave, success=False,
                         error_msg=f"validation failed twice: {exc}"[:500])
    return None
