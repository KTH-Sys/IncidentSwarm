# Pipes

RocketRide pipelines, exported from RocketRide Cloud into this directory during
packaging (plan.md §11, 15:00-15:20). Both files are part of the submission and
are attached to the #showcase post.

| file | what it is |
|---|---|
| `incident_parallel.pipe` | Waves 0-3: provision + triage, 5-agent fan-out, correlator fan-in, teardown + score |
| `incident_single.pipe` | The baseline: one DB with all 5 tables, one fast agent at budget 110, same correlator prompt for synthesis |

Export both before submitting — an un-exported pipe is a missing deliverable,
not a missing nicety.
