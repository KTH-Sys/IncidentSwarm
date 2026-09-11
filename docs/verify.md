# VERIFY list — resolutions

plan.md §8 listed 7 unconfirmed sponsor-API behaviors. Items 3–7 were resolved by
introspecting the installed SDK (`hotdata` 0.10.0, `hotdata-langchain` 0.14.0)
rather than by asking a mentor. Items 1–2 are RocketRide-side and still open.

| # | Question | Status |
|---|---|---|
| 1 | How a RocketRide agent node calls custom Python / LangChain tools inside a wave | **OPEN** — mentor |
| 2 | Whether waves take runtime values (wave-0 `db_ids` → wave-1 agent config) | **OPEN** — mentor, gating |
| 3 | Row inserts vs load-only | **RESOLVED** |
| 4 | Bulk-create: sync vs async, time to ready | **RESOLVED** |
| 5 | Delete-DB call and rate limits | **RESOLVED** |
| 6 | Index refresh after loads into an existing table | **RESOLVED (partly)** |
| 7 | SQL dialect details | **RESOLVED (offline)** |

## 3 — Row inserts ARE supported

`LoadManagedTableRequest` takes `mode` (`"replace"`, `"append"`, `"update"`,
`"upsert"`, `"delete"`) and accepts either inline `data` or a staged `upload_id`.

So telemetry does **not** need the load-only micro-batch workaround: append
micro-batches directly with inline `data`. `Emitter(..., load_only=False)` — flush
every 50 events or 2s (§9.2). Inline data is documented as the quick path for
small payloads; parquet slices go through the uploads API instead.

## 4 — Bulk-create is ASYNC

`bulk_create_databases` returns `BulkCreateDatabasesResult(batch_id, created,
requested, cancelled)` — **`created` is a count, not the ids**.
`get_database_batch(batch_id)` returns `created_count`, which "advances as the
batch fills", plus a `job_id` to poll. Retrieve ids with
`list_databases(batch=batch_id)`.

`agents/hotdata_scope.provision()` polls at 250ms up to 60s, then resolves ids.
Time-to-ready is unmeasured until credentials land — if it exceeds 10s, §13's
mitigation applies: pre-warm a DB pool per batch and report provisioning
separately in Q7.

## 5 — Teardown is ONE call

`delete_database_batch(batch_id)` deletes the whole batch and returns
`deleted_count`. Wave 3 does not need five delete calls.

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

This is a live risk for HistoryAgent. `index()` skips the vector spec and emits
an `error` row when no provider is configured, so a missing provider degrades
HistoryAgent instead of breaking the run — and HistoryAgent plus the vector index
is §12 cut-list item 1 anyway.

Whether an index refreshes after a later load into the same table is still
untested, and matters for FTS over telemetry `error_msg` in Q5.

## 7 — SQL dialect

Tested offline with DuckDB against the generated parquet (`python -m bench.validate`),
which is not proof for Hotdata's engine but does prove the queries are sound SQL.
Wave-0 triage uses only CTEs, `ROW_NUMBER() OVER (PARTITION BY ...)`, a scalar
subquery, and `CASE` — no interval arithmetic, no casts, no `FILTER`.

`PERCENTILE_CONT ... WITHIN GROUP` in Q3 is still unverified against Hotdata;
the documented fallback is `approx_percentile_cont(duration_ms, 0.95)`.

## Useful API surface

| Need | Call |
|---|---|
| provision | `DatabasesApi.bulk_create_databases` → poll `get_database_batch` → `list_databases(batch=)` |
| declare table | `add_database_table(db, schema, AddManagedTableRequest(name=...))` — columns inferred on load |
| stage a file | `UploadsApi.upload_file(path)` → `upload_id` (handles multipart) |
| load | `load_database_table(db, schema, table, LoadManagedTableRequest(upload_id=, format="parquet", mode=))` |
| index | `IndexesApi.create_index(db, schema, table, CreateIndexRequest(index_type="bm25"|"vector"|"sorted"))` |
| query | `QueryApi.query(QueryRequest(database_id=, sql=))` → `rows`, `columns`, `row_count`, `execution_time_ms` |
| teardown | `delete_database_batch(batch_id)` |
| sweep | `list_databases(search=prefix)` → `delete_database(id)` |

`QueryResponse.execution_time_ms` is the server-side duration — telemetry uses it
for `query.duration_ms` in preference to the client-side clock.
