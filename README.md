# IncidentSwarm

**Parallel root-cause analysis.** Five isolated agents investigate logs, metrics,
changes, infra events, and past postmortems at the same time — each with its own
task-scoped database containing only its slice. A correlator merges their findings
into one RCA report, which is scored against seeded ground truth and benchmarked
against a single-agent baseline.

> Incident signals live in separate sources, and each source needs a different
> query mode. Fanning out one agent per source finds the root cause faster than
> one agent working through everything in a single context — and we measure
> whether that's true instead of asserting it.

Built at the Data & AI Hackathon, AWS Builder Loft SF — brief: **Parallel Agents**
(RocketRide + Hotdata).

## Architecture

```
bench/run_batch.py ──webhook {run_id, scenario_id, mode, pipeline_version}──▶ RocketRide

WAVE 0  PROVISION + TRIAGE (deterministic, no LLM)
        POST bulk-create databases (count=5)            → db_ids[5]   emit db_create ×5
        load slice parquet into each DB                                emit db_load ×N
        create indexes: fts(logs.msg), fts(k8s.message), vector(postmortems.body)
        triage SQL on metrics DB → symptoms[]   (services whose err_rate or p99 is ≥3× baseline)

WAVE 1  FAN-OUT (parallel)  — each agent sees ONLY its own db_id plus symptoms[]
        ├─ LogAgent      db_logs      fts + sql
        ├─ MetricsAgent  db_metrics   sql
        ├─ ChangeAgent   db_changes   sql
        ├─ InfraAgent    db_k8s       sql + fts
        └─ HistoryAgent  db_history   vector
        each → HypothesisSet JSON                                      emit agent_start/query/llm_call/agent_end

WAVE 2  FAN-IN
        Correlator (strong model): merge 5 HypothesisSets + symptoms → RCAReport

WAVE 3  TEARDOWN + SCORE
        delete 5 DBs (in a finally path)                              emit db_destroy ×5
        score.py(RCAReport, truth.json)                               emit run_end {score, wall, $, tokens}
        flush telemetry buffer

TELEMETRY DB (event-scoped, persistent): table `events`  ◀── every wave, every agent
dashboard/app.py ── live SQL (Q1–Q7) ──▶ telemetry DB
```

**Isolation.** Agents never receive `truth.json`, and never see another agent's
`db_id` or output before wave 2. Truth-derived telemetry columns (`fault_type`,
`hit_*`) are attached by the runner, outside agent context.

**The baseline.** One agent, one DB with all five tables and all three indexes, a
query budget equal to the sum of the five parallel budgets (110), and the *same*
correlator prompt for its final synthesis. The only variable changed is parallel
vs sequential.

## Results

Pre-registered targets, written down before any run:

| ID | Metric | Target | Actual |
|---|---|---|---|
| T1 | Median wall-clock, parallel ÷ single | ≤ 0.5 | _TBD_ |
| T2 | Mean RCA score (0–4), parallel − single | ≥ 0 | _TBD_ |
| T3 | Mean $/run, v2 vs v1 | v2 < v1 at ≥ v1 score | _TBD_ |
| T4 | Telemetry volume by 15:00 | ≥ 36 runs, ≥ 2 versions + baseline | _TBD_ |

<!-- Fill from live Q4 / Q6 output at 14:30-15:00. Report what the numbers
     actually are — a missed target with a clear explanation demos better than
     no baseline at all. -->

| pipeline_version | mode | runs | avg score | avg wall (s) | avg $ |
|---|---|---|---|---|---|
| _TBD_ | | | | | |

### What telemetry changed

_v1 → finding → one change → v2._ Exactly one change, chosen from telemetry, so
the effect can be attributed.

- **Finding (Q1):** _TBD_
- **Change (v2):** _TBD_
- **Effect (Q4):** _TBD_

## Layout

```
gen/          synthetic incident generator — faults, noise, cascade, postmortems
data/         generated scenarios: 5 parquet tables + truth.json each (gitignored)
pipes/        SPEC.md — the wave graph; .pipe files exported from RocketRide Cloud
agents/       schemas, Hotdata lifecycle, the 7 prompts, and waves 0-3
  wave0.py      provision + load + index + deterministic triage SQL
  agent.py      one wave-1 source agent: tool loop, budget enforcement, telemetry
  correlator.py wave 2 fan-in
  baseline.py   the single-agent control arm
  pipeline.py   waves 0-3 end to end, both modes, teardown guaranteed
telemetry/    frozen event schema, buffered emitter, Q1–Q7
bench/        preflight, validity check, scorer, batch runner
dashboard/    Streamlit — runs Q1–Q7 live on page load
```

`plan.md` is the full build plan. `docs/verify.md` records which sponsor-API
questions were resolved, how, and what changed as a result.

## Run it

```bash
uv sync
cp .env.example .env          # HOTDATA_API_KEY, HOTDATA_WORKSPACE, ANTHROPIC_API_KEY

# one-time: create the event-scoped telemetry DB, then put the id in .env
uv run python -m bench.preflight --create-telemetry-db

# prove the stack works: create -> load -> query -> destroy, for real
uv run python -m bench.preflight

# generate scenarios (seeds 1-12 batch set, 99 demo hold-out)
uv run python -m gen.scenarios --seeds 1-12 99

# prove every scenario is actually solvable from its own data
uv run python -m bench.validate

# benchmark: interleaves parallel and single so both meet the same rate limits
uv run python -m bench.run_batch --mode both --version v1 --seeds 1-12 --concurrency 3

# live telemetry
uv run streamlit run dashboard/app.py
```

## Scoring

0–4, one point per component: `service` (exact), `fault_type` (exact; `unknown`
never matches), `trigger_event_id` (exact; null matches null), `start_ts` (within
120s). Per-agent `hit_service` / `hit_fault` use that agent's top-confidence
hypothesis.

## Why the benchmark isn't trivial

Each scenario injects a cascade — services upstream of the root cause get ~50% of
its error elevation, lagged 1–2 minutes — plus two red herrings sampled from
unrelated deploys, flag flips, gateway spikes, and routine rollout noise. An agent
that blames the loudest service blames `gateway` and scores 0. Getting it right
means ordering onsets, not ranking magnitudes.

## Is the benchmark fair?

A score only means something if the answer was reachable. `bench/validate.py`
runs the **real** wave-0 triage SQL against the generated parquet with DuckDB and
checks, for all 13 scenarios, that the true root service is the earliest
sustained onset, that the cascade never overtakes the service it follows, that
the trigger event exists, and that the root's log signature is present after t0.

It has already earned its place. The first cascade implementation lit up
`gateway` before `checkout`, even though gateway sits upstream of it; and the
first triage reported a resolved red-herring spike as the earliest onset in 3 of
13 scenarios, which would have fed every agent a misleading `symptoms[]` and made
those runs unwinnable. Both were caught here, not in review.
