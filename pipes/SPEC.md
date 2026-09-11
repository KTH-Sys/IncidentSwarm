# Pipeline spec — what to build in RocketRide Cloud

**Status: planned integration.** No exported `.pipe` files or RocketRide runtime
calls are present in this repository. The runnable implementation is
`agents/pipeline.py`, which uses Python threads for fan-out. This document is a
blueprint for wiring those functions into RocketRide after verifying the node
and runtime-value contracts with the sponsor.

Export and validate the actual pipelines before including them in a submission.
Calling the entire Python runner from one RocketRide node alone would leave
fan-out in Python; it would not demonstrate RocketRide orchestrating the agents.

## `incident_parallel.pipe`

Webhook trigger. Payload: `{run_id, scenario_id, mode: "parallel", pipeline_version}`.

| Wave | Node | Does | Python entry point |
|---|---|---|---|
| 0 | Provision + triage | Bulk-create 5 DBs, load one slice each, index, triage SQL → `symptoms[]` | `agents.wave0.run_wave0` |
| 1 | Fan-out ×5 (concurrent) | LogAgent, MetricsAgent, ChangeAgent, InfraAgent, HistoryAgent — each gets **only** its own `db_id` + `symptoms[]` | `agents.agent.run_agent` |
| 2 | Correlator | Merge 5 HypothesisSets + symptoms → RCAReport | `agents.correlator.run_correlator` |
| 3 | Teardown + score | Delete each DB, cancel creation batch, score vs truth, flush telemetry | Lifecycle adapter around `agents.hotdata_scope.destroy` and `bench.score.score_report` |

Wave 1 must run its five agents **concurrently** — that is the claim being
judged. Each agent node receives exactly two runtime values:

```json
{"db_id": "<from wave 0 db_ids[i]>", "symptoms": [ ... ]}
```

**This is VERIFY item 2**: if a wave cannot take runtime values from wave 0, fall
back to agents-as-tools under a parent agent node (§11 Gate 1), where the parent
holds `db_ids` and passes each child its own.

Budgets and models per agent are in `agents/hotdata_scope.SLICES` — logs 40,
metrics 30, changes 15, infra 15, history 10; fast model throughout, strong model
for the correlator only.

## `incident_single.pipe`

Same trigger, `mode: "single"`. One wave, one agent, one DB holding all five
tables and all three indexes, query budget 110 (the sum of the five parallel
budgets), finishing with one strong-model synthesis call that uses **the same
correlator prompt**.

| Wave | Node | Python entry point |
|---|---|---|
| 0 | Provision 1 DB, load all 5 tables, index, triage | `agents.pipeline.run_single` |
| 1 | Single agent, budget 110 | `agents.baseline.run_baseline` |
| 2 | Synthesis (strong model, correlator prompt) | inside `run_baseline` |
| 3 | Teardown + score | Lifecycle adapter around `agents.hotdata_scope.destroy` and `bench.score.score_report` |

Both arms use the same model tiers, correlator prompt, scenarios, and scoring.
They differ in specialist prompts, context, and reasoning-turn limits. Without
an embedding provider, the Python parallel arm uses four agents and 100 queries;
the baseline retains five tables and 110 queries. These are benchmark limitations,
not a controlled concurrency-only comparison.

The Python functions above are implementation references, not ready-made node
adapters: `run_once` owns the entire lifecycle, and `run_single` includes loading,
investigation, and synthesis. A RocketRide integration must split these steps
without executing them twice and must preserve cleanup when any wave fails.

## Telemetry contract

Every node emits into the persistent telemetry DB (`telemetry/schema.py`, frozen).
The runner — never an agent — attaches `fault_type`, `hit_service`, `hit_fault`,
and `score`, because those derive from `truth.json` and must stay outside agent
context (§3).
