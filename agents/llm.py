"""Model calls with telemetry, cost accounting, and validated output (plan.md §7).

Provider: OpenAI.

Two tiers (§7.2):
  fast   = the five wave-1 source agents, and the baseline's investigation
  strong = the correlator, and the baseline's synthesis call

Every call emits an `llm_call` row carrying tokens, cost, and model. Per-agent
totals are summed onto `agent_end`, per-run totals onto `run_end` — never SUM
across both (§9.1).

On a pydantic validation failure we re-prompt once with the error (emit `retry`);
a second failure emits `error` and returns an empty result. One agent failing
must not kill the run.

Responses are normalized to `LLMResponse` so the agent loop never touches a
provider-specific message shape. Conversation history is built here too, because
tool-result turns differ by provider (OpenAI uses one `role="tool"` message per
call).
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

import openai
from pydantic import BaseModel, ValidationError

from telemetry.emit import Emitter

T = TypeVar("T", bound=BaseModel)

PROMPT_DIR = Path(__file__).parent / "prompts"

FAST_MODEL = os.getenv("FAST_MODEL", "gpt-5-mini")
STRONG_MODEL = os.getenv("STRONG_MODEL", "gpt-5")

# USD per 1M tokens: {model: (input, output)}.
#
# Cost feeds telemetry and pre-registered target T3, so these must be right.
# Override per model from .env without touching code, e.g.
#     MODEL_PRICING={"gpt-5-mini": [0.25, 2.00], "gpt-5": [1.25, 10.00]}
# Any model missing here is priced 0 and preflight warns loudly, because a
# silent 0 would quietly invalidate every cost number in the demo.
#
# Resolved lazily, not at import time: reading os.getenv when this module is
# first imported makes correctness depend on whether .env happened to load first,
# and the failure is silent — every cost_usd becomes 0 and target T3 quietly
# reports nothing. Ask at call time instead.


@lru_cache(maxsize=1)
def _pricing() -> dict[str, tuple[float, float]]:
    try:
        raw = json.loads(os.getenv("MODEL_PRICING", "{}"))
    except json.JSONDecodeError:
        return {}
    return {k: (float(v[0]), float(v[1])) for k, v in raw.items() if len(v) == 2}


def price_of(model: str) -> tuple[float, float]:
    """USD per 1M (input, output) tokens. (0, 0) means unpriced — preflight
    fails on that rather than letting it through as a free run."""
    return _pricing().get(model, (0.0, 0.0))


def cost_of(tokens_in: int, tokens_out: int, model: str) -> float:
    pin, pout = price_of(model)
    return (tokens_in * pin + tokens_out * pout) / 1_000_000


MAX_TOKENS_FAST = 4096
MAX_TOKENS_STRONG = 8192
TEMPERATURE = 0  # dropped automatically for models that reject it — see _call_raw

# Capability probing, cached per model. Newer OpenAI models take
# `max_completion_tokens` and reject `max_tokens`; reasoning models reject any
# temperature but the default. Rather than hardcode a model list that goes stale,
# find out once per model and remember.
_CAPS: dict[str, dict[str, bool]] = {}


@dataclass
class ToolCall:
    id: str
    name: str
    args: dict


@dataclass
class LLMResponse:
    """Provider-neutral response."""

    text: str
    tool_calls: list[ToolCall] = field(default_factory=list)
    finish_reason: str = "stop"
    raw: Any = None

    @property
    def wants_tools(self) -> bool:
        return bool(self.tool_calls)


@dataclass
class Usage:
    """Accumulates across a turn so agent_end can carry per-agent totals."""

    tokens_in: int = 0
    tokens_out: int = 0
    cost_usd: float = 0.0
    calls: int = 0
    retries: int = 0

    def add(self, tin: int, tout: int, model: str) -> None:
        self.tokens_in += tin
        self.tokens_out += tout
        self.cost_usd += cost_of(tin, tout, model)
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
def client() -> openai.OpenAI:
    if not os.getenv("OPENAI_API_KEY"):
        raise RuntimeError("OPENAI_API_KEY is not set — see .env.example")
    return openai.OpenAI()


def to_provider_tools(tools: list[dict]) -> list[dict]:
    """Neutral tool spec {name, description, input_schema} -> OpenAI function."""
    return [
        {"type": "function",
         "function": {"name": t["name"], "description": t["description"],
                      "parameters": t["input_schema"]}}
        for t in tools
    ]


def system_turn(system: str) -> dict:
    return {"role": "system", "content": system}


def append_assistant(convo: list[dict], resp: LLMResponse) -> None:
    """Append the assistant turn, preserving tool calls so the follow-up
    `role="tool"` messages have something to attach to."""
    msg: dict[str, Any] = {"role": "assistant", "content": resp.text or None}
    if resp.tool_calls:
        msg["tool_calls"] = [
            {"id": tc.id, "type": "function",
             "function": {"name": tc.name, "arguments": json.dumps(tc.args)}}
            for tc in resp.tool_calls
        ]
    convo.append(msg)


def append_tool_results(convo: list[dict], results: list[tuple[str, str]]) -> None:
    """OpenAI takes ONE message per tool result, each keyed by tool_call_id.

    Every call that was issued must get a result back, or the next request is
    rejected for an unanswered tool call.
    """
    for tool_call_id, content in results:
        convo.append({"role": "tool", "tool_call_id": tool_call_id, "content": content})


def _caps(model: str) -> dict[str, bool]:
    return _CAPS.setdefault(model, {"max_completion_tokens": True, "temperature": True})


def _call_raw(model: str, messages: list[dict], tools: list[dict] | None,
              max_tokens: int) -> Any:
    """One request, adapting to per-model parameter incompatibilities.

    Model families differ on `max_tokens` vs `max_completion_tokens` and on
    whether a non-default temperature is allowed. Probing beats a hardcoded model
    list, which would be wrong the week a new model ships.

    The reaction is driven by the error MESSAGE, never by the current cached
    flag. `_CAPS` is shared across the wave-1 threads, so gating on it created a
    race: the first agent to see the 400 flipped the flag, and every other agent
    already in flight then fell through to `raise` instead of retrying. That cost
    three of five agents on the first live run — they returned nothing, having
    made zero queries.
    """
    caps = _caps(model)
    last: Exception | None = None
    for _ in range(4):
        kwargs: dict[str, Any] = {"model": model, "messages": messages}
        if tools:
            kwargs["tools"] = tools
        kwargs["max_completion_tokens" if caps["max_completion_tokens"]
               else "max_tokens"] = max_tokens
        if caps["temperature"]:
            kwargs["temperature"] = TEMPERATURE
        try:
            return client().chat.completions.create(**kwargs)
        except openai.BadRequestError as exc:
            last = exc
            msg = str(exc).lower()
            if "temperature" in msg:
                caps["temperature"] = False
            elif "max_completion_tokens" in msg:
                caps["max_completion_tokens"] = False
            elif "max_tokens" in msg:
                caps["max_completion_tokens"] = True
            else:
                raise
    raise RuntimeError(f"could not find a working parameter set for {model}: {last}")


def warm(model: str, em: Emitter) -> None:
    """Discover a model's parameter quirks once, before the fan-out.

    Without this the first five concurrent agents each pay a failed request to
    learn the same thing.
    """
    if model in _CAPS:
        return
    try:
        _call_raw(model, [{"role": "user", "content": "ok"}], None, 16)
    except Exception as exc:  # noqa: BLE001 — warming is best effort
        em.event("error", agent="pipeline", wave=0, success=False,
                 error_msg=f"warm {model}: {exc}"[:300])


def call(*, model: str, system: str, messages: list[dict], tools: list[dict] | None = None,
         em: Emitter, agent: str, wave: int, usage: Usage, prompt_name: str | None = None,
         max_tokens: int | None = None) -> LLMResponse:
    """One model call. Emits `llm_call` with tokens, cost, and duration."""
    budget = max_tokens or (MAX_TOKENS_FAST if model == FAST_MODEL else MAX_TOKENS_STRONG)
    t0 = time.monotonic()
    raw = _call_raw(model, [system_turn(system), *messages],
                    to_provider_tools(tools) if tools else None, budget)
    dur = (time.monotonic() - t0) * 1000

    choice = raw.choices[0]
    msg = choice.message
    calls: list[ToolCall] = []
    for tc in (msg.tool_calls or []):
        try:
            args = json.loads(tc.function.arguments or "{}")
        except json.JSONDecodeError:
            args = {}
        calls.append(ToolCall(id=tc.id, name=tc.function.name, args=args))

    tin = raw.usage.prompt_tokens if raw.usage else 0
    tout = raw.usage.completion_tokens if raw.usage else 0
    usage.add(tin, tout, model)
    em.event("llm_call", agent=agent, wave=wave, model=model, duration_ms=dur,
             tokens_in=tin, tokens_out=tout, cost_usd=cost_of(tin, tout, model),
             prompt_hash=prompt_hash(prompt_name) if prompt_name else None,
             success=True)

    return LLMResponse(text=msg.content or "", tool_calls=calls,
                       finish_reason=choice.finish_reason or "stop", raw=raw)


_FENCE = re.compile(r"```(?:json)?\s*(.*?)\s*```", re.DOTALL)


def extract_json(text: str) -> Any:
    """Agents are told to return bare JSON, but models sometimes fence it or add
    a sentence. Recover rather than burn a retry on formatting."""
    text = (text or "").strip()
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
        try:
            return parse_validated(resp.text, schema)
        except (ValidationError, json.JSONDecodeError) as exc:
            if attempt == 0:
                usage.retries += 1
                em.event("retry", agent=agent, wave=wave, success=False,
                         error_msg=str(exc)[:500])
                convo = convo + [
                    {"role": "assistant", "content": resp.text or "(empty)"},
                    {"role": "user", "content":
                        f"That did not validate against the required schema:\n{exc}\n\n"
                        "Return ONLY the corrected JSON object. No prose, no code fence."},
                ]
            else:
                em.event("error", agent=agent, wave=wave, success=False,
                         error_msg=f"validation failed twice: {exc}"[:500])
    return None
