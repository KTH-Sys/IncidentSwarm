# IncidentSwarm — plan.md

> Parallel root-cause analysis: five isolated agents investigate logs, metrics, changes, infra events, and past postmortems at the same time. A correlator merges their findings into one RCA. Scored against seeded ground truth and benchmarked against a single-agent baseline.

| | |
|---|---|
| Event | Data & AI Hackathon, AWS Builder Loft SF, Fri Sep 11 2026 |
| Brief | **Parallel Agents** (RocketRide + Hotdata). If the Memory → Muscle Memory brief is the one judged, see Appendix A |
| Hacking window | 11:00–15:30, about 4.5h including lunch |
| Submission | 15:30, #showcase on the RocketRide Discord: GitHub link + all `.pipe` files |
| Demos | Top 5 at 16:00; the telemetry view must be shown live |

Items marked **`VERIFY`** are unconfirmed sponsor API behavior. Resolve them during the 10:00–11:00 workshops (§11).

---

## 1. Thesis and pre-registered targets

**Claim:** incident signals live in separate sources, and each source needs a different query mode. Fanning out one agent per source, each with its own isolated database, finds the root cause faster and at least as accurately as one agent working through everything in a single context.

Targets are written down *before* any run, so the results can't be retrofitted.

| ID | Metric | Target |
|---|---|---|
| T1 | Median wall-clock, parallel ÷ single (same scenarios) | ≤ 0.5 |
| T2 | Mean RCA score (0–4), parallel − single | ≥ 0 |
| T3 | Mean $/run, parallel v2 vs parallel v1 | v2 < v1 at ≥ v1 score |
| T4 | Telemetry volume by 15:00 | ≥ 36 runs across ≥ 2 pipeline versions + 1 baseline |

Report whatever the numbers turn out to be. A missed target plus a clear explanation still demos better than no baseline.

## 2. Judging criteria → how each is met

| Judge checks | How we satisfy it | Artifact |
|---|---|---|
| Multiple agents run in parallel on a real task | Wave 1 runs five source agents concurrently | `pipes/incident_parallel.pipe` |
| Parallel is measurably better than a single agent | Same scenarios, model, and tool budget; compare wall-clock, score, and $ | `pipes/incident_single.pipe`, Q6 |
| RocketRide does the orchestration | Waves 0–3 cover provision, fan-out, fan-in, and teardown | `.pipe` files |
| Each parallel agent gets its own task-scoped Hotdata DB | Bulk-create 5 DBs per run, each loaded with only its slice | `agents/hotdata_scope.py` |
| Create → query → destroy is explicit | `db_create` / `db_load` / `query` / `db_destroy` telemetry events, plus `finally` teardown and an orphan sweeper | telemetry rows |
| Telemetry DB spans the whole event, across sessions | One telemetry DB, created once, never torn down; every run is tagged with `pipeline_version` | `telemetry/` |
| Queries run live in the demo | Streamlit page executes Q1–Q7 on page load | `dashboard/app.py` |
| "What we changed because of it" | v1 → telemetry finding → one change → v2, compared in Q4 | §10 |

## 3. Architecture

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

**Single baseline** (`incident_single.pipe`): one DB containing all 5 tables and all 3 indexes. One fast-model agent works with a query budget equal to the sum of the five parallel agents' budgets (110). It finishes with one strong-model synthesis call that uses the **same correlator prompt**. The only variable changed is parallel vs sequential.

**Isolation rule:** agents never receive `truth.json`, and never see another agent's `db_id` or output before wave 2. The runner adds truth-derived columns (`fault_type`, `hit_*`) to telemetry *outside* agent context.

## 4. Repo layout

```
incidentswarm/
├── README.md                 # pitch, architecture diagram, results table, how to run
├── plan.md
├── .env.example              # HOTDATA_API_KEY, HOTDATA_WORKSPACE, ROCKETRIDE_API_KEY, model keys
├── pyproject.toml            # uv; deps: hotdata, hotdata-langchain, pyarrow, pandas, numpy, pydantic, streamlit
├── .gitignore                # data/, .env
├── gen/
│   ├── faults.py             # fault catalog → signal injectors (§5.3)
│   ├── noise.py              # baseline traffic, red herrings, cascades
│   ├── postmortems.py        # templated corpus of 20 past postmortems
│   └── scenarios.py          # CLI: python -m gen.scenarios --seeds 1-12 99
├── data/<scenario_id>/       # logs, metrics, changes, k8s_events, postmortems .parquet + truth.json
├── pipes/
│   ├── incident_parallel.pipe
│   └── incident_single.pipe
├── agents/
│   ├── schemas.py            # Hypothesis, HypothesisSet, RCAReport (pydantic)
│   ├── hotdata_scope.py      # provision / load / index / scoped tools / destroy / sweep
│   └── prompts/              # logs.md metrics.md changes.md infra.md history.md correlator.md single.md
├── telemetry/
│   ├── schema.py             # frozen column spec (§8.1), single source of truth
│   ├── emit.py               # buffered emitter → micro-batch load
│   └── queries.sql           # Q1–Q7
├── bench/
│   ├── score.py
│   └── run_batch.py          # --mode parallel|single --version v1 --seeds 1-12 --concurrency 3
└── dashboard/
    └── app.py                # Streamlit: runs Q1–Q7 live, one chart or table each
```

## 5. Synthetic incident data

### 5.1 World

- **Services:** `gateway → checkout → {payments, inventory}`, `payments → postgres`, `payments → stripe-api` (external).
- **Window:** 120 min. Metrics are sampled every 1 min. Fault onset `t0` is drawn uniformly from minute 50–80.
- **Baseline traffic:** gateway at about 30 rps (sinusoidal ±20%). Downstream services get proportional rps.
- **Baseline levels:** err_rate 0.1–0.5%, p99 80–250 ms by service, Gaussian jitter.
- **Cascade rule:** services upstream of the root cause get about 50% of its error elevation, lagged 1–2 min. This is what fools naive agents into blaming `gateway`.
- **Seeds:** 1–12 form the batch set, two per fault type (round-robin). Seed 99 is the hold-out for the live demo, with a fault type chosen at demo time.

### 5.2 Tables (per scenario)

**`logs.parquet`** (~200k rows, cap at 250k)

| column | type | notes |
|---|---|---|
| ts | timestamp | |
| service | string | |
| level | string | INFO / WARN / ERROR |
| trace_id | string | shared across services for a request |
| msg | string | FTS-indexed |
| attrs | string | JSON: status, latency_ms, route |

**`metrics.parquet`** (120 × 5 = 600 rows)

| column | type |
|---|---|
| ts | timestamp |
| service | string |
| rps | double |
| p99_ms | double |
| err_rate | double |
| cpu_pct | double |
| mem_mb | double |
| db_conns | int (postgres and payments pool; null otherwise) |

**`changes.parquet`**: `event_id` (`chg_####`), `ts`, `kind` (deploy / flag / config), `service`, `detail`, `author`

**`k8s_events.parquet`**: `event_id` (`k8s_####`), `ts`, `service`, `pod`, `reason` (Scheduled / Killing / Unhealthy / OOMKilled / BackOff), `message` (FTS-indexed)

**`postmortems.parquet`**: `pm_id`, `title`, `service`, `fault_type`, `body` (vector-indexed). This is a shared corpus of 20 docs. Only ~12 match catalog fault types, and 8 are distractors.

**`truth.json`** (never loaded into any DB):
```json
{
  "scenario_id": "s07",
  "service": "payments",
  "fault_type": "pool_exhaustion",
  "trigger_event_id": "chg_0031",
  "start_ts": "2026-09-11T14:03:00Z",
  "red_herrings": ["chg_0012", "gateway_spike@13:31"]
}
```

### 5.3 Fault catalog

| fault_type | Root service | Trigger (`trigger_event_id`) | Metrics signature | Logs signature | k8s signature |
|---|---|---|---|---|---|
| `bad_deploy` | payments | deploy `v2.3.0→v2.3.1` at t0 | err_rate 8–15% from t0+1 | `NullPointerException in ChargeHandler`, HTTP 500 | — |
| `flag_flip` | checkout | flag `checkout.new_pricing=on` at t0 | p99 ×3, err_rate ~2% | `pricing fallback after timeout` | — |
| `pool_exhaustion` | payments | config `db_pool_max 50→10` at t0 | payments db_conns pinned at 10, p99 ×10 | `timeout acquiring connection from pool` | — |
| `cpu_throttle` | inventory | first `Unhealthy` k8s event | cpu_pct ≥ 97%, p99 ×4 | sparse `slow request` WARNs | `Unhealthy: readiness probe failed` |
| `third_party_latency` | payments | `null` (external) | p99 ramps ×6 over 10 min, err_rate ~5% | `stripe-api upstream timeout 504` | — |
| `memory_leak` | checkout | first `OOMKilled` event | mem_mb linear ramp from t0−30, sawtooth after restarts | `GC overhead limit` | `OOMKilled`, `BackOff` |

**Red herrings.** Each scenario gets 2, sampled from:
- deploy to `inventory` 35–45 min before t0
- 5-min gateway err spike 25–35 min before t0
- unrelated flag flip on `gateway`
- routine `Scheduled`/`Killing` rollout noise

## 6. Scoring (`bench/score.py`)

The score is 0–4, one point per component:

| Component | Rule |
|---|---|
| service | exact match |
| fault_type | exact match (enum; `unknown` never matches) |
| trigger_event_id | exact match; `null` matches `null` |
| start_ts | \|predicted − truth\| ≤ 120 s |

It returns `{score, components{...}, predicted, truth}`. Per-agent hits for telemetry use the agent's **top-confidence** hypothesis: `hit_service`, `hit_fault`.

## 7. Agents

### 7.1 Shared contracts (`agents/schemas.py`)

```json
// HypothesisSet: every wave-1 agent returns exactly this shape
{
  "agent": "metrics",
  "hypotheses": [
    {
      "service": "payments",
      "fault_type": "pool_exhaustion",
      "trigger_event_id": null,
      "start_ts": "2026-09-11T14:03:00Z",
      "confidence": 0.72,
      "evidence": ["payments p99 212ms→2140ms at 14:03", "postgres db_conns flat at 10 from 14:03"]
    }
  ],
  "queries_used": 23,
  "notes": "gateway error rise lags payments by 2 min, which looks like cascade"
}
```
Rules:
- `fault_type` is an enum over the §5.3 catalog plus `unknown`.
- 1–3 hypotheses per agent, sorted by confidence.
- Agents may set only the fields their own source can support. ChangeAgent can name `trigger_event_id`; MetricsAgent cannot.

```json
// RCAReport: correlator (and single-agent synthesis) output
{
  "root_cause": {"service": "", "fault_type": "", "trigger_event_id": null, "start_ts": "", "confidence": 0.0},
  "timeline": [{"ts": "", "source": "changes|metrics|logs|k8s|history", "event": ""}],
  "ruled_out": [{"candidate": "", "reason": ""}],
  "summary": "≤3 sentences"
}
```

Every agent output is validated with pydantic. On a validation failure, re-prompt once with the error (emit `retry`). On a second failure, emit an `error` event and return an empty HypothesisSet. One agent failing must not kill the run.

### 7.2 Agent roster

| Agent | DB contents | Query kinds | Budget | Model | Job |
|---|---|---|---|---|---|
| LogAgent | logs | fts + sql | 40 | fast | Error signatures, first occurrence per service, per-minute error counts |
| MetricsAgent | metrics | sql | 30 | fast | Change points vs. first-40-min baseline; earliest anomalous service; cascade direction |
| ChangeAgent | changes | sql | 15 | fast | Rank every change by proximity to symptom onset and blast radius |
| InfraAgent | k8s_events | sql + fts | 15 | fast | Restarts, OOMs, probe failures, and their onset |
| HistoryAgent | postmortems | vector | 10 | fast | Nearest past incidents for the symptom pattern; map them to fault_type |
| Correlator | none (JSON in) | — | 1 call | strong | Merge, align timelines, discount cascades and red herrings |

- **Models:** fast = Haiku-class, strong = Sonnet-class, whatever RocketRide's provider nodes expose (`VERIFY`). Temperature 0 everywhere.
- **Prompt hashing:** store `sha1(prompt_file)[:8]` in telemetry as `prompt_hash`.
- **Correlator heuristics** (in its prompt):
  - Earliest onset beats largest magnitude.
  - A change within 10 min *before* onset on the root or its dependency is the prime trigger candidate.
  - Upstream errors lagging downstream are cascade, not cause.
  - Changes more than 20 min before onset with no matching symptoms are red herrings.
  - If there's no change or k8s trigger and the logs name an external host, use `third_party_latency` with a `null` trigger.

## 8. Hotdata lifecycle (`agents/hotdata_scope.py`)

| Function | Does | Emits |
|---|---|---|
| `provision(run_id, n)` | Bulk-create `n` instant DBs from one template; address by **id** (names aren't unique) | `db_create` ×n with duration |
| `load(db_id, table, parquet)` | Upload + load parquet (not inline CSV, which caps at 2 MB) | `db_load` with rows, duration |
| `index(db_id, spec)` | FTS on `logs.msg`, `k8s_events.message`; vector on `postmortems.body` | `db_load` (kind=index) |
| `tools(db_id, budget, agent)` | Scoped SQL/search tools (via `hotdata-langchain` if RocketRide accepts LangChain tools; otherwise thin wrappers over the Python SDK). Enforces budget; times every call | `query` per call |
| `destroy(db_ids)` | Delete DBs; always reached through `try/finally` in wave 3 | `db_destroy` ×n |
| `sweep(prefix)` | Deletes orphaned `isw-*` DBs older than 15 min; run at batch start and end | `db_destroy` (agent=sweeper) |

**`VERIFY` list, resolve before 11:00:**
1. How a RocketRide agent node calls custom Python / LangChain tools inside a wave. Fallback: agents-as-tools under a parent agent node.
2. Whether waves can be parameterized with runtime values (db_ids from wave 0 → wave 1 agent config).
3. Hotdata: row inserts vs load-only. This decides the telemetry emit strategy (§9.2).
4. Hotdata bulk-create response: sync vs async batch polling, and the time to ready.
5. Hotdata delete-DB call and its rate limits.
6. Whether search indexes refresh after new loads into an existing table (affects FTS on telemetry `error_msg`).
7. SQL dialect details: `PERCENTILE_CONT ... WITHIN GROUP` vs `approx_percentile_cont`, and whether `date_trunc` is available.

## 9. Telemetry

### 9.1 Frozen schema: `events` table in the persistent telemetry DB

> Hotdata tables have no ALTER path. **Finalize this schema before the first run.** Put anything unforeseen in `payload` (JSON string).

| column | type | populated on |
|---|---|---|
| event_id | string | all |
| run_id | string | all |
| pipeline_version | string | all (`v1`, `v2`, `single-v1`) |
| mode | string | all (`parallel` / `single`) |
| scenario_id | string | all |
| fault_type | string | all (truth, added by runner) |
| prompt_hash | string | agent_*, llm_call |
| agent | string | all (`pipeline`, `logs`, `metrics`, …, `correlator`, `sweeper`) |
| wave | int | all |
| event_type | string | enum below |
| ts | timestamp | all |
| duration_ms | double | *_end, query, llm_call, db_* |
| model | string | llm_call, agent_end |
| tokens_in | bigint | llm_call; summed on agent_end and run_end |
| tokens_out | bigint | same |
| cost_usd | double | same |
| query_count | int | agent_end, run_end |
| db_id | string | db_*, query |
| query_kind | string | query (`sql` / `fts` / `vector`) |
| query_text | string | query (truncated to 1000 chars) |
| rows_returned | bigint | query, db_load |
| success | boolean | all *_end, query, db_* |
| retry_count | int | agent_end |
| error_msg | string | error, retry |
| hit_service | boolean | agent_end, run_end |
| hit_fault | boolean | agent_end, run_end |
| score | int | run_end |
| payload | string | overflow JSON: hypotheses, RCAReport, symptoms |

`event_type` enum: `run_start`, `run_end`, `wave_start`, `wave_end`, `db_create`, `db_load`, `db_destroy`, `agent_start`, `agent_end`, `query`, `llm_call`, `error`, `retry`.

**Aggregation convention.** Per-agent totals live on `agent_end` rows, and per-run totals on `run_end` rows. Never `SUM` over `llm_call` and `agent_end` together, or you double-count.

### 9.2 Emission (`telemetry/emit.py`)

- `Emitter(run_ctx).event(**cols)` appends to an in-memory buffer and fills `run_ctx` columns automatically.
- **If row inserts are supported:** flush every 50 events or every 2 s.
- **If load-only:** micro-batch. Flush the buffer as parquet at every `agent_end` and `wave_end`. The run can't finish until the final flush succeeds (retry ×3, then write `data/telemetry_backlog/*.parquet` and replay at batch end).
- Emitter failures never fail a run. Log them locally and count them in `payload` on `run_end`.

### 9.3 Live queries (`telemetry/queries.sql`)

The SQL avoids `FILTER`, `::` casts, and `ROUND(double, n)` for dialect safety; rounding happens in Streamlit.

**Q1: Cross-agent — which source agent actually finds the root cause, by fault type**
```sql
SELECT agent, fault_type,
       COUNT(*) AS runs,
       AVG(CASE WHEN hit_service AND hit_fault THEN 1.0 ELSE 0.0 END) AS hit_rate,
       AVG(cost_usd) AS avg_cost_usd,
       AVG(query_count) AS avg_queries
FROM events
WHERE event_type = 'agent_end' AND mode = 'parallel' AND wave = 1
GROUP BY agent, fault_type
ORDER BY fault_type, hit_rate DESC;
```

**Q2: Concurrency — recurring bottleneck on the wave-1 critical path**
```sql
WITH w AS (
  SELECT run_id, pipeline_version, agent, duration_ms,
         RANK() OVER (PARTITION BY run_id ORDER BY duration_ms DESC) AS rk
  FROM events
  WHERE event_type = 'agent_end' AND mode = 'parallel' AND wave = 1
)
SELECT pipeline_version, agent,
       SUM(CASE WHEN rk = 1 THEN 1 ELSE 0 END) AS times_bottleneck,
       AVG(duration_ms) AS avg_ms
FROM w
GROUP BY pipeline_version, agent
ORDER BY pipeline_version, times_bottleneck DESC;
```

**Q3: Data layer — query volume, shape, and latency per agent**
```sql
SELECT agent, query_kind,
       COUNT(*) AS queries,
       COUNT(DISTINCT db_id) AS dbs,
       COUNT(*) * 1.0 / COUNT(DISTINCT db_id) AS queries_per_db,
       PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY duration_ms) AS p50_ms,
       PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) AS p95_ms,
       AVG(rows_returned) AS avg_rows
FROM events
WHERE event_type = 'query'
GROUP BY agent, query_kind
ORDER BY queries DESC;
-- VERIFY dialect; fallback: approx_percentile_cont(duration_ms, 0.95)
```

**Q4: Cross-session — did v2 beat v1? (the "what we changed" query)**
```sql
SELECT pipeline_version, mode,
       COUNT(*) AS runs,
       AVG(score) AS avg_score,
       AVG(duration_ms) AS avg_wall_ms,
       AVG(cost_usd) AS avg_cost_usd,
       AVG(tokens_in + tokens_out) AS avg_tokens,
       MIN(ts) AS first_run, MAX(ts) AS last_run
FROM events
WHERE event_type = 'run_end'
GROUP BY pipeline_version, mode
ORDER BY first_run;
```

**Q5: Failures — who retries or errors, and does it recur across sessions**
```sql
SELECT agent, event_type, pipeline_version,
       COUNT(*) AS n,
       COUNT(DISTINCT run_id) AS runs_affected,
       MIN(ts) AS first_seen, MAX(ts) AS last_seen
FROM events
WHERE event_type IN ('error', 'retry')
GROUP BY agent, event_type, pipeline_version
ORDER BY n DESC;
-- plus full-text over error text (VERIFY index refresh):
-- hotdata search "timeout" --index tel_error_msg
```

**Q6: Parallel vs single, per scenario**
```sql
WITH r AS (
  SELECT scenario_id, fault_type, pipeline_version, mode,
         AVG(duration_ms) AS wall_ms, AVG(score) AS score, AVG(cost_usd) AS cost
  FROM events
  WHERE event_type = 'run_end'
  GROUP BY scenario_id, fault_type, pipeline_version, mode
)
SELECT p.pipeline_version, p.fault_type, p.scenario_id,
       s.wall_ms / p.wall_ms AS speedup,
       p.score - s.score     AS score_delta,
       p.cost  - s.cost      AS cost_delta_usd
FROM r p
JOIN r s ON s.scenario_id = p.scenario_id AND s.mode = 'single'
WHERE p.mode = 'parallel'
ORDER BY p.pipeline_version, p.fault_type, p.scenario_id;
```

**Q7: Fan-out efficiency — effective parallelism and idle time waiting on the slowest agent**
```sql
WITH a AS (
  SELECT run_id, pipeline_version,
         COUNT(*)         AS n_agents,
         SUM(duration_ms) AS agent_ms_sum,
         MAX(duration_ms) AS wave_ms
  FROM events
  WHERE event_type = 'agent_end' AND mode = 'parallel' AND wave = 1
  GROUP BY run_id, pipeline_version
)
SELECT pipeline_version,
       AVG(agent_ms_sum / wave_ms)                   AS effective_parallelism,
       AVG(n_agents * wave_ms - agent_ms_sum)        AS avg_idle_agent_ms,
       AVG(n_agents)                                 AS avg_agents_spawned
FROM a
GROUP BY pipeline_version;
```

## 10. Benchmark protocol and the v1 → v2 decision

**Batches**

| Batch | Runs | Tag | When |
|---|---|---|---|
| B1 | seeds 1–12 × parallel | `v1` | 13:30 |
| B2 | seeds 1–12 × single | `single-v1` | 13:30, interleaved with B1 |
| B3 | seeds 1–12 × parallel | `v2` | 14:15 |
| Demo | seed 99 × parallel | `v2` | live |

**Controls**
- Same models, temperature 0, same total query budget, same correlator prompt.
- Interleave B1 and B2 (P, S, P, S …) so both modes see the same rate-limit conditions.
- Scenario-level concurrency is 3 for both modes. Wall-clock is measured per run, from `run_start` to `run_end`.
- **Timebox:** if the median run exceeds 3 min, cut all batches to seeds 1–6 and still include each fault type once.

**v2 rule:** make exactly **one** change, chosen from telemetry, so the effect can be attributed. Decide by 14:10.

| If telemetry shows… | Signal source | v2 change |
|---|---|---|
| One agent is almost never the top hit for most fault types but costs as much as the others | Q1 | **Adaptive fan-out:** wave-0 triage skips agents whose source shows no anomaly (e.g. skip InfraAgent when there are 0 non-routine k8s events) |
| One agent is the bottleneck in most runs with a high query count | Q2 + Q3 | Cut that agent's budget and add an early exit when confidence ≥ 0.8 |
| Correlator picks the cascade service (gateway/checkout) on wrong runs | Q6 + `payload` | Add an explicit onset-ordering table to the correlator input (computed, not LLM) |
| Retries cluster on one agent's JSON output | Q5 | Stricter output schema + one-shot example in that prompt |

Demo narrative template: *"Q1 showed [agent] hit on [x]% of runs but used [y]% of cost. v2 [change]. Q4: score [a→b], cost [c→d], wall [e→f]."*

## 11. Timeline (hard gates)

### Pre-event (before 10:00)
- [ ] Accounts: RocketRide Cloud + API key + Discord promo credits; Hotdata account + key + promo `9R8YFUDM`; confirm a test DB can be created and queried end to end
- [ ] `git clone rocketride-org/rocketride-workshops`, skim one `solution/` pipeline
- [ ] Repo initialized with the §4 skeleton, `.env.example`, `uv sync` passes
- [ ] Decide team split if not solo: **A** = RocketRide pipes + agents, **B** = generator + scorer + bench, **C** = telemetry + dashboard

### 10:00–11:00 — Workshops
- [ ] Resolve §8 VERIFY items 1–5 with sponsor mentors (7 can wait)
- [ ] Freeze §9.1 schema. Create the telemetry DB and record its id in `.env` as `TELEMETRY_DB_ID`

### 11:00–11:45 — Spike · **GATE 1**
- [ ] `.pipe` with 2 parallel agents in one wave, each doing create → load 10-row parquet → query → destroy
- [ ] Both agents emit telemetry, and the rows are visible via `SELECT COUNT(*) FROM events`
- **Gate:** parallel wave + scoped Hotdata tools + telemetry rows by 11:45. Otherwise switch to agents-as-tools under a parent node and re-test by 12:00. If still failing at 12:00, pivot to the ETL Strategy Tournament on the same skeleton.

### 11:45–12:30 — Data + scoring
- [ ] `gen/` with `bad_deploy`, `pool_exhaustion`, `memory_leak` first, plus noise, red herrings, and cascade
- [ ] `score.py` + unit check: a perfect RCAReport scores 4 and an empty one scores 0
- [ ] Generate seeds 1–12, 99 (add the remaining 3 fault types if time allows, else rotate the 3)

### 12:30–13:30 — Agents end to end (eat at desk) · **GATE 2**
- [ ] 5 prompts + correlator prompt; pydantic validation + one retry
- [ ] Wave 0 triage SQL; waves 1–3 wired in `incident_parallel.pipe`
- **Gate:** one full parallel run on seed 1 produces an RCAReport, all 5 DBs are destroyed, and a `run_end` row is written by 13:30

### 13:30–14:00 — Baseline + v1 batch
- [ ] `incident_single.pipe`
- [ ] `run_batch.py` interleaving B1/B2 → ≥ 24 `run_end` rows

### 14:00–14:30 — Telemetry → change → v2
- [ ] Run Q1–Q7 and pick one change via the §10 table by 14:10
- [ ] Implement, bump `pipeline_version=v2`, run B3

### 14:30–15:00 — Dashboard
- [ ] `dashboard/app.py`: Q4 and Q6 headline cards at top, Q1 heatmap, Q2/Q3/Q5/Q7 tables; every query runs on page load
- [ ] Fill the README results table from live Q4/Q6 output

### 15:00–15:20 — Package
- [ ] README: pitch, architecture diagram (§3), results, "what telemetry changed", run instructions
- [ ] Export both `.pipe` files from RocketRide Cloud into `pipes/`
- [ ] Record a 2-min backup video of a live run + dashboard
- [ ] `sweep()`, then confirm zero orphan DBs

### 15:20–15:30 — Submit
- [ ] #showcase post: one-line pitch, GitHub link, both `.pipe` files attached, headline numbers from Q4/Q6

## 12. Cut list and stretch

**Cut in this order when behind:**
1. HistoryAgent and vector index (4 agents is still parallel)
2. Fault types down to 3
3. Streamlit → run `queries.sql` in a terminal on screen
4. Batch size down to seeds 1–6

**Never cut:** the single-agent baseline, the v1 → v2 comparison, or teardown events. These are the judging criteria.

**Stretch, in order:**
1. **Verifier wave 2a:** parallel verifier agents each re-query their DB to confirm or refute the top-3 hypotheses before the correlator runs
2. **Merge DB:** load all HypothesisSets into a wave-2 Hotdata DB and let the correlator query them with SQL
3. Cost-per-correct-RCA leaderboard by fault type

## 13. Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| RocketRide wave ↔ custom tool wiring eats the morning | High | GATE 1 at 11:45, agents-as-tools fallback, ETL-tournament pivot at 12:00 |
| Hotdata bulk-create is async/slow, and wave 0 dominates wall-clock | Med | Measure in the spike; if >10 s, pre-warm a DB pool per batch and report provisioning separately in Q7 |
| Telemetry is load-only | Med | Micro-batch parquet per `agent_end` (§9.2) |
| Schema mistake after data lands (no ALTER) | Med | Freeze at 10:45; `payload` JSON overflow; worst case use `events_v2` and `UNION ALL` in queries |
| Rate limits distort parallel timings | Med | Interleaved batches, concurrency 3, record `retry` events and show them in Q5 honestly |
| Parallel doesn't beat single on score | Med | Still report it; speed + cost are the claim, and the Q1 insight becomes the story |
| Orphan DBs burn credits | Low | `finally` teardown + `sweep()` at batch start and end |
| Live demo network failure | Med | Backup video + telemetry DB still holds all accumulated runs |

## 14. Demo script (~4 min)

| t | Beat | On screen |
|---|---|---|
| 0:00 | "3am page. Checkout is erroring. Five dashboards, one tired engineer." | Title |
| 0:20 | Fire seed 99 live | RocketRide canvas: wave 0 creates 5 DBs, wave 1 runs 5 agents concurrently |
| 1:00 | Isolation: each agent has its own instant DB with only its slice, destroyed at the end | Q3 row count + `db_create`/`db_destroy` counts |
| 1:30 | RCAReport vs ground truth | Report + score/4 |
| 2:00 | "Is parallel actually better?" | Q6 live: speedup, score delta, cost delta over the day's runs |
| 2:40 | "What telemetry taught us" | Q1 heatmap → the finding → Q4 v1 vs v2 |
| 3:30 | Close: pattern generalizes to any multi-source investigation (security triage, fraud) | README/GitHub |

## 15. Submission checklist

- [ ] Public GitHub repo, with README results filled from live queries
- [ ] `pipes/incident_parallel.pipe`, `pipes/incident_single.pipe` exported from Cloud
- [ ] Telemetry DB populated with ≥ 2 parallel versions + baseline; dashboard runs live
- [ ] Zero orphan DBs
- [ ] Backup video
- [ ] #showcase post before 15:30

---

## Appendix A — Delta if "Memory → Muscle Memory" is the judged brief

That brief requires Cognee, HydraDB, hotdata.dev, RocketRide, Modiqo Rote, plus a Snyk scan. The core product stays the same; add a compounding loop. Budget about 90 min, taken from §12 stretch items and the Streamlit polish.

| Layer | Addition |
|---|---|
| Cognee | After each scored run, ingest the RCAReport + evidence (`remember`). Extraction builds entities `service`, `symptom`, `fault_type`, `change`, `fix` |
| HydraDB | Durable store for that graph. HistoryAgent swaps vector search for Cypher multi-hop, e.g. services with symptom X that were previously caused by a config change within 10 min |
| hotdata.dev | Unchanged: per-agent instant DBs + telemetry |
| RocketRide | Unchanged orchestration; adds a post-run "remember" wave |
| Rote | Capture the successful investigation query path per fault type on the first correct run, then replay it deterministically on repeat fault types. Proof of compounding = Q4 by run index: tokens and wall-clock drop on repeats |
| Snyk | Scan the repo (Python deps + Streamlit) before submission; fix or pin anything flagged |

Additional telemetry query: score, tokens, and wall time by `run_index_within_fault_type`, which shows the compounding curve.
