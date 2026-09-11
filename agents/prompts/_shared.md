<!-- Included by every wave-1 agent prompt. Keep edits here, not duplicated. -->

## Output contract

Return ONE JSON object matching `HypothesisSet` (agents/schemas.py). No prose
outside the JSON, no markdown fence.

```json
{
  "agent": "<your agent name>",
  "hypotheses": [
    {
      "service": "payments",
      "fault_type": "pool_exhaustion",
      "trigger_event_id": null,
      "start_ts": "2026-09-11T14:03:00Z",
      "confidence": 0.72,
      "evidence": ["payments p99 212ms->2140ms at 14:03", "db_conns flat at 10 from 14:03"]
    }
  ],
  "queries_used": 23,
  "notes": "gateway error rise lags payments by 2 min, which looks like cascade"
}
```

Rules:
- 1-3 hypotheses, sorted by confidence descending.
- `fault_type` is one of: `bad_deploy`, `flag_flip`, `pool_exhaustion`,
  `cpu_throttle`, `third_party_latency`, `memory_leak`, `unknown`.
- Set ONLY the fields your own source can support. If your source cannot see
  change events, leave `trigger_event_id` null — do not guess it.
- `evidence` must quote concrete values you actually queried. No evidence you did
  not observe.
- You can see only your own database. You do not know what the other agents found.
