# Correlator (wave 2, strong model)

You receive five `HypothesisSet` objects — one each from the logs, metrics,
changes, infra, and history agents — plus the wave-0 `symptoms[]` list. You have
no database access. Merge them into one `RCAReport`.

## Heuristics (apply in this order)

1. **Earliest onset beats largest magnitude.** The service that broke first is a
   better root-cause candidate than the one that broke worst.
2. **A change within 10 minutes *before* onset**, on the root service or its
   dependency, is the prime trigger candidate.
3. **Upstream errors lagging downstream are cascade, not cause.** If gateway's
   error rise trails payments by a minute or two, payments is the cause and
   gateway is a symptom.
4. **Changes more than 20 minutes before onset with no matching symptoms are red
   herrings.** Put them in `ruled_out` with the reason.
5. **If there is no change and no k8s trigger, and the logs name an external
   host**, use `third_party_latency` with a `null` `trigger_event_id`.

Dependency graph: `gateway -> checkout -> {payments, inventory}`,
`payments -> postgres`, `payments -> stripe-api` (external).

## Output

Return ONE JSON object matching `RCAReport` (agents/schemas.py). No prose outside it.

```json
{
  "root_cause": {"service": "", "fault_type": "", "trigger_event_id": null,
                 "start_ts": "", "confidence": 0.0},
  "timeline": [{"ts": "", "source": "changes|metrics|logs|k8s|history", "event": ""}],
  "ruled_out": [{"candidate": "", "reason": ""}],
  "summary": "<= 3 sentences"
}
```

- `timeline` is ordered earliest first and cites which source each entry came from.
- `ruled_out` must name every red herring an agent proposed, with why you dropped it.
- Prefer `unknown` over a confident wrong answer when the agents genuinely conflict.
