# RocketRide Cloud exports — partial integration

Exported September 11, 2026 from the account file store at
`https://staging.rocketride.ai/` using the project's configured RocketRide SDK.
Both files were saved to Cloud, downloaded again, and verified byte-for-byte.
The SDK schema validator returned a pipeline and version for both, without an
error. This is structural validation, not an execution test.

## Files

- `incident_parallel.pipe`: five specialist agent nodes (LogAgent, MetricsAgent,
  ChangeAgent, InfraAgent, HistoryAgent), each with its own database-tool and
  memory node. Native Wave coordinator uses the agents-as-tools fallback from
  `SPEC.md`, with instructions to invoke all five in one wave before synthesis.
- `incident_single.pipe`: one baseline investigation agent, a database-tool
  node for all five tables, memory, model, and response.

Both canvases include a webhook and explicitly named Wave 0 and Wave 3 Python
tool placeholders. The placeholders are **unconnected and not implemented**.
Their descriptions record the required Python entry points and lifecycle
contracts. They do not invoke existing project functions.

## Remaining work

1. Install/register deterministic adapters for provisioning, loading, indexing,
   triage, and teardown. The Cloud Python tool exposes sandboxed agent-invoked
   execution, not a form for binding a Python callable. Package availability,
   serialized inputs/outputs, and lifecycle ownership are not established.
2. Bind webhook JSON to runtime values. Each parallel specialist must receive
   only its own provisioned `db_id` and shared `symptoms`. The current Hotdata
   nodes are configuration stubs; dynamic DB binding is not implemented.
3. Verify actual five-way concurrent execution and all-results fan-in. Prompting
   the coordinator to use one wave is not evidence that this happened.
4. Enforce budgets (40/30/15/15/10; baseline 110), configure models and secrets,
   and provide HistoryAgent's embedding provider and vector index.
5. Implement the baseline's strong-model synthesis with the same correlator
   prompt. Its current canvas has only the investigation model. Split the
   provisioning portion out of `agents.pipeline.run_single`; calling that whole
   function in Wave 0 would run investigation and synthesis twice.
6. Guarantee cleanup after success, partial provisioning, and every failure.
   Scoring against withheld truth and telemetry flushing remain runner-owned
   adapter work, outside all agent contexts.

Both entry agents include an `ADAPTERS_NOT_CONFIGURED` instruction so that the
partial design does not present fabricated investigation results. These graphs
have not been deployed or run. Remove that instruction only after integration.
All API-key fields remain environment placeholders; actual credentials are not
embedded in the exports.

## Export method and builder limitation

The staging node editor returned `Validation error: 'pipeline.components' must
be an array` when saving a temporary UI edit. The supported SDK file-store API
was used to save the complete graphs and retrieve the exported `.pipe` files.
The pre-existing locally generated four-specialist and single-agent drafts were
backed up before replacement. `SPEC.md` remains the blueprint, with its status
updated to identify these exports as partial.
