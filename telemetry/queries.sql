-- Live telemetry queries (plan.md §9.3). Run on dashboard page load.
-- Dialect-safe by construction: no FILTER, no :: casts, no ROUND(double, n).
-- Rounding happens in Streamlit.
--
-- Queries are split on the `-- name: qN` markers by dashboard/app.py.

-- name: Q1
-- Cross-agent — which source agent actually finds the root cause, by fault type
SELECT agent, fault_type,
       COUNT(*) AS runs,
       AVG(CASE WHEN hit_service AND hit_fault THEN 1.0 ELSE 0.0 END) AS hit_rate,
       AVG(cost_usd) AS avg_cost_usd,
       AVG(query_count) AS avg_queries
FROM events
WHERE event_type = 'agent_end' AND mode = 'parallel' AND wave = 1
GROUP BY agent, fault_type
ORDER BY fault_type, hit_rate DESC;

-- name: Q2
-- Concurrency — recurring bottleneck on the wave-1 critical path
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

-- name: Q3
-- Data layer — query volume, shape, and latency per agent
-- VERIFY dialect (§8 item 7); fallback: approx_percentile_cont(duration_ms, 0.95)
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

-- name: Q4
-- Cross-session — did v2 beat v1? (the "what we changed" query)
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

-- name: Q5
-- Failures — who retries or errors, and does it recur across sessions
-- plus full-text over error text (VERIFY index refresh, §8 item 6):
--   hotdata search "timeout" --index tel_error_msg
SELECT agent, event_type, pipeline_version,
       COUNT(*) AS n,
       COUNT(DISTINCT run_id) AS runs_affected,
       MIN(ts) AS first_seen, MAX(ts) AS last_seen
FROM events
WHERE event_type IN ('error', 'retry')
GROUP BY agent, event_type, pipeline_version
ORDER BY n DESC;

-- name: Q6
-- Parallel vs single, per scenario
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

-- name: Q7
-- Fan-out efficiency — effective parallelism and idle time waiting on the slowest agent
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
       AVG(agent_ms_sum / wave_ms)            AS effective_parallelism,
       AVG(n_agents * wave_ms - agent_ms_sum) AS avg_idle_agent_ms,
       AVG(n_agents)                          AS avg_agents_spawned
FROM a
GROUP BY pipeline_version;
