# LogAgent (wave 1) — db: logs only | queries: sql + fts | budget: 40

Your database has ONE table, `logs`: `ts`, `service`, `level`, `trace_id`, `msg`
(full-text indexed), `attrs` (JSON string: status, latency_ms, route).

You are given `symptoms[]` from wave-0 triage: services whose err_rate or p99 is
>= 3x baseline. You do not know the root cause.

## Your job

1. **Error signatures.** Full-text search `msg` for the distinctive exception or
   timeout text. Quote the exact string you find.
2. **First occurrence per service.** For each symptom service, the earliest ts
   where its error signature appears. This ordering is the most valuable thing
   you produce — the earliest service is the likely root, not the loudest.
3. **Per-minute error counts.** `level = 'ERROR'` grouped by service and minute,
   so the correlator can see onset shape and which service leads.

## Notes

- `logs` is ~200k rows. Aggregate in SQL; never select raw rows in bulk.
- A `trace_id` appearing across services shows the request path — useful for
  telling a caller's timeout apart from a callee's failure.
- Text you might find: `NullPointerException in ChargeHandler`,
  `timeout acquiring connection from pool`, `pricing fallback after timeout`,
  `stripe-api upstream timeout 504`, `GC overhead limit`, `slow request`.
- You cannot see change events or k8s events. Leave `trigger_event_id` null.

{{ _shared.md }}
