# VERIFY list — resolutions

plan.md §8 listed 7 unconfirmed sponsor-API behaviors. Items 3–7 were resolved by
introspecting the installed SDK (`hotdata` 0.10.0, `hotdata-langchain` 0.14.0)
rather than by asking a mentor. Items 1–2 are RocketRide-side and still open.

| # | Question | Status |
|---|---|---|
| 1 | How a RocketRide agent node calls custom Python / LangChain tools inside a wave | **OPEN** — mentor |
| 2 | Whether waves take runtime values (wave-0 `db_ids` → wave-1 agent config) | **OPEN** — mentor, gating |
| 3 | Row inserts vs load-only | **RESOLVED against the live API** |
| 4 | Bulk-create: sync vs async, time to ready | **RESOLVED** — ~0.6-1.1s for 2 DBs |
| 5 | Delete-DB call and rate limits | **RESOLVED — earlier note was WRONG, see below** |
| 6 | Index refresh after loads into an existing table | **RESOLVED (partly)** |
| 7 | SQL dialect details | **RESOLVED — Q1-Q7 all run on Hotdata** |

Items 3, 5, and 7 were first answered by reading the SDK and then **corrected by
running against the real API**. Two of those first answers were wrong in ways
that would have cost the afternoon. The SDK tells you what a call is named; only
the API tells you what it does.

## 3 — Appends work, but only as typed parquet

`LoadManagedTableRequest` takes `mode` (`"replace"`, `"append"`, ...) and accepts
either inline `data` or a staged `upload_id`. Appends exist, so §9.2's load-only
fear was unfounded — but two live failures narrowed the usable path to one:

1. **Inline `data` accepts only CSV.** JSON returns
   `inline "data" supports only "csv"; upload the file and load it by upload_id`.
2. **CSV types are inferred per batch.** The events schema is deliberately sparse
   — each `event_type` fills a different subset of the 28 columns — so a column
   that is entirely null in one micro-batch is inferred as `varchar` and rejected
   against the pinned `int32`. This is not an edge case; it is every batch.

So `HotdataSink` writes **parquet built from `ARROW_SCHEMA`** — the same schema
that pinned the table — and loads it by `upload_id`. Types match by construction.
It costs an upload round trip per flush, which is why the emitter micro-batches
at `agent_end` / `wave_end` (`load_only=True`) rather than flushing per event.

### The events table schema is pinned by its FIRST load, permanently

A declared managed table has no schema until data lands: querying it first
returns `declared but has no data; POST a load before querying`. Whatever lands
first decides the column types, and there is no way back —
`DROP TABLE` returns `COPY TO, DML, and DDL statements are not supported`, and a
type change returns `only widening to a larger compatible type is applied`.
**The only recovery is deleting the whole database and recreating it.**

`bench.preflight --create-telemetry-db` therefore seeds the table with one typed
parquet row from `ARROW_SCHEMA` before anything else writes. The seed row is
tagged `event_type='schema_seed'` and every query in `queries.sql` filters on a
real event_type, so it never appears in results.

Two type traps found this way, both now encoded in `telemetry/schema.py`:

- `ts` must be a **naive** timestamp holding UTC. The CSV loader always infers
  naive `timestamp`, and `timestamptz` cannot be narrowed afterwards.
- `ts` must be **microsecond** precision, matching `pa.timestamp("us")`. A
  millisecond value is inferred as `timestamp_ms` and rejected.

## 4 — Bulk-create is ASYNC

`bulk_create_databases` returns `BulkCreateDatabasesResult(batch_id, created,
requested, cancelled)` — **`created` is a count, not the ids**.
`get_database_batch(batch_id)` returns `created_count`, which "advances as the
batch fills", plus a `job_id` to poll. Retrieve ids with
`list_databases(batch=batch_id)`.

`agents/hotdata_scope.provision()` polls at 250ms up to 60s, then resolves ids.
Earlier live checks recorded roughly 0.6–1.1 seconds for two databases.
Re-measure for the current workspace and batch size before making latency claims.

## 5 — Teardown is NOT one call (correcting an earlier note)

An earlier version of this file claimed `delete_database_batch(batch_id)` tears
down the whole batch. **It does not.** It cancels the creation batch — the SDK's
own wording is "databases already created are kept; only further creation is
stopped". Called alone it returns `deleted_count: 0` and silently leaks every
database the run created.

Caught by `bench.preflight`, which checks that teardown actually deleted what it
made. Wave 3 now deletes each database individually with `delete_database(id)`,
concurrently since each is an independent round trip, and cancels the creation
batch afterwards so a slow batch cannot add databases after teardown.

A second trap in the same area: `idempotency_key` must be unique **per call**.
Deriving it from `run_id` alone means a retried run resurrects its spent batch,
which then resolves to zero database ids.

Better still, `BulkCreateDatabasesRequest` accepts `expires_at` as a relative
duration (`"30m"`). Every run-scoped DB now self-expires, so a crash that skips
both the `finally` path and the sweeper still cannot leave orphans burning
credits. That turns §13's orphan risk from "mitigated" into "structurally
prevented" — the sweeper is now a third line of defence.

## 6 — Index types, and the vector-index dependency

`CreateIndexRequest.index_type` is one of `sorted` (range), **`bm25`** (full
text), `vector` (similarity). So FTS is `bm25`, not `fts`.

**A vector index requires an `embedding_provider_id`.** Providers are created via
`EmbeddingProvidersApi` with `provider_type` `"local"` or `"service"` — `"local"`
should avoid needing a third-party embedding key, but this is untested.

This is a live risk for HistoryAgent. Parallel mode omits HistoryAgent when no provider is configured. The baseline
still loads all five tables, and `index()` skips its vector index and emits an
`error` event. This changes the available data and budget between the two arms;
see the benchmark limitations in the README.

Whether an index refreshes after a later load into the same table is still
untested, and matters for FTS over telemetry `error_msg` in Q5.

## 7 — SQL dialect: all seven queries run

**The original Q1–Q7 were executed against the live telemetry DB and returned
rows.** The current revisions separate failed runs, match baseline versions,
and add Q8 for recent reports. They are checked offline with DuckDB; those
changes still need a live Hotdata recheck. That includes the two that were in doubt: `PERCENTILE_CONT(...) WITHIN
GROUP (ORDER BY ...)` in Q3 and `RANK() OVER (PARTITION BY ...)` in Q2. The
`approx_percentile_cont` fallback is not needed.

Wave-0 triage is also dialect-safe by construction: CTEs, `ROW_NUMBER() OVER`, a
scalar subquery, and `CASE` — no interval arithmetic, no casts, no `FILTER`.

One hard limit worth knowing: the query API is **read-only**. `COPY TO, DML, and
DDL statements are not supported`, so tables are created and shaped exclusively
through the databases/uploads APIs.

## Useful API surface

| Need | Call |
|---|---|
| provision | `DatabasesApi.bulk_create_databases` → poll `get_database_batch` → `list_databases(batch=)` |
| declare table | `add_database_table(db, schema, AddManagedTableRequest(name=...))` — columns inferred on load |
| stage a file | `UploadsApi.upload_file(path)` → `upload_id` (handles multipart) |
| load | `load_database_table(db, schema, table, LoadManagedTableRequest(upload_id=, format="parquet", mode=))` |
| index | `IndexesApi.create_index(connection_id, schema, table, request)` using the connection ID returned by the load |
| query | `QueryApi.query(QueryRequest(database_id=, sql=))` → `rows`, `columns`, `row_count`, `execution_time_ms` |
| teardown | `delete_database(id)` for each database, then `delete_database_batch(batch_id)` to cancel creation |
| sweep | `list_databases(search=prefix)` → `delete_database(id)` |

`QueryResponse.execution_time_ms` is the server-side duration — telemetry uses it
for `query.duration_ms` in preference to the client-side clock.

## Model provider: OpenAI

The agents run on OpenAI (`openai` SDK, chat completions). Tiers are set in
`.env` — `FAST_MODEL` for the five wave-1 agents, `STRONG_MODEL` for the
correlator and the baseline's synthesis call. `python -m bench.preflight
--list-models` lists what the key can actually use, so the tiers are chosen
rather than guessed.

Three things this changed:

- **Cost telemetry needs `MODEL_PRICING` in `.env`** — USD per 1M tokens as
  `{"model": [input, output]}`. There is no built-in price table, because a
  stale hardcoded one is worse than none: an unpriced model yields `cost_usd = 0`
  and silently empties target T3 and every dollar figure in the demo. Preflight
  fails when a configured model has no price. Pricing resolves lazily at call
  time, not at import, so it cannot depend on whether `.env` loaded first.

- **Tool-result turns differ by provider.** OpenAI takes one `role="tool"`
  message per call, keyed by `tool_call_id`, where Anthropic takes a single user
  message holding every result block. `agents/llm.py` owns that shape via
  `append_assistant` / `append_tool_results`, and the agent loop works against a
  normalized `LLMResponse`, so swapping providers again touches one file.

- **Request parameters are probed, not hardcoded.** Newer models take
  `max_completion_tokens` and reject `max_tokens`; reasoning models reject a
  non-default `temperature`. `_call_raw` retries once against each known
  incompatibility and caches the answer per model, rather than carrying a model
  list that goes stale the week a new model ships.

Temperature 0 (plan §7.2) therefore holds only where the model accepts it. Both
arms of the experiment use the same models and the same correlator prompt, so
the parallel-vs-single control is unaffected either way.

## Search query syntax remains unverified

Creating an index does not verify the SQL used to search it. The templates in
`agents/agent.py` currently use `search(...)` and `distance(...)`; verify them
against the live service before claiming full-text or vector search works.
`HOTDATA_FTS_SQL` and `HOTDATA_VECTOR_SQL` can override them. Logs and
infrastructure agents can fall back to SQL; HistoryAgent currently has only
the vector tool. The failure dashboard includes failed query events.
