# Outcome-First Agent Loop — Submission

**Name:** Deeksha · **Date:** 2026-11-08 · **Hours actually spent:** not tracked · **Language / storage:** Python 3.10+ / SQLite

## 0. TL;DR

The controller ingests simulator HTTP event chunks, tests one two-arm allocation at a time, and routes config changes through policy-governed auto-apply or human approval.
It persists chunk cursors, sufficient statistics, proposal state, and an audit log in SQLite.
It includes initial caller-ID and prompt-risk detection with validated prompt proposals.
Fresh public_01/public_02 runs, a public_01 kill/restart proof, and a public_03 load run completed. Public_03 ingested 9,737,481 events in 343.50 seconds (28,348 events/second) with a sampled process-tree peak working set of 1,012.5 MiB on this Windows host. Reference-hardware performance remains unmeasured.
Feature-specific cohorts, schedule optimization, a supervisor UI, production telemetry, and automatic decay rollback are deliberately not complete.

## 1. Assumptions

1. The simulator is the policy authority. A stakeholder memo cannot override an API-enforced workflow policy.
2. The simulator process and its state survive a controller restart; the controller database is on durable local storage.
3. Event `cohort_id` is the canonical cohort identity. The controller does not independently create a competing cohort taxonomy.
4. The comparison unit is the workflow's current config version and its `default` cohort. Campaign-locked versions are not pooled with current-version experiments.
5. A proposal affects at most one daily uploaded batch for its impact/affected-lead estimate; this is a queue-ranking estimate, not a causal projection.
6. A prompt QA flag is a noisy incident signal. It triggers validation and human governance, not an automatic prompt deployment.
7. Decision polling is hourly in simulated time; the simulator advances only when asked.

## 2. Scope decisions

| # | Capability | Built / Partial / Cut | Why | What it would take to finish |
|---|---|---|---|---|
| C1 | Backlog-safe, idempotent event ingestion | Partial | Ordered chunks are transactionally aggregated and deduplicated; fresh public_03 measured 28,348 events/second and 1,012.5 MiB sampled process-tree peak working set on this Windows host. | Reference-hardware benchmark and disk/backpressure monitoring. |
| C2 | Feature joins and one shared cohort object | Partial | Uses the event's canonical cohort ID (`default` in the starter defaults); no feature-defined cohort creation. | Define/test stable feature segments and persist their exact definitions for all surfaces. |
| C3 | Bootstrap from history | Built | Processes history chunks before planning. | Add a measured warm-start report. |
| C4 | Call-time optimization | Cut | Protects business-owned schedule/cadence from experimentation. | Add randomized hour experiments with per-person preference evidence and policy tests. |
| C5 | Caller-ID health | Partial | Compares IDs only within the default cohort and current configuration version; uses the configured primary KPI and complementary guardrail before a conservative reallocation. | DID-level calibration, explicit traffic-load correction, and broader per-cohort controls. |
| C6 | Welcome/voice experimentation | Partial | The production evaluator still uses daily arm aggregates, minimum samples, practical effect threshold, and a guardrail. Fixed-horizon experiment definitions, family alpha, looks, and person-level observations persist in SQLite but are not wired into the production planner/evaluator. | Connect planning/evaluation to registered experiments; implement fixed-horizon inference and promotion with the documented family/look controls. |
| C7 | Promotion, decay, rollback | Partial | Promotion retains a 20% comparison arm so degradation can remain observable. | A tested rolling-window rollback state machine and post-rollback audit evidence. |
| C8 | Hallucination response | Partial | Detects a statistically elevated flag rate, asks for suggestions, validates candidates, and requires approval evidence. | Verify post-apply metrics, add an incident lifecycle, and calibrate QA false-positive rates. |
| C9 | Governance client | Partial | Reconciles pending approvals, applies promptly, detects stale versions, and limits itself to one active proposal per workflow. | More exhaustive expiry/lapse/re-proposal tests and explicit per-tenant fatigue scheduling. |
| C10 | Impact, affected leads, dry run | Partial | Supplies an observed sample count, bounded score, one-batch lead count, and a documented method. | Replay historical cohorts and estimate causal impact with uncertainty intervals. |
| C11 | Consolidated supervisor view | Cut | Durable audit CLI is smaller and safer than an unrequested UI. | Build a read-only API/UI over the audit and experiment state. |
| C12 | Queryable audit store | Built | SQLite records decisions and proposal/application identifiers; `--audit` prints records. | Add retention controls, signed exports, and searchable indexes for production. |
| C13 | Crash recovery demonstration | Built | Public_01 was interrupted at simulated minute 60 and completed after restart with the same SQLite database. | Extend restart testing to each remote side-effect boundary. |
| C14 | Controller observability | Partial | Structured logs and durable audit entries exist. | Export ingest lag, decision rate, credit burn, and actionable alert metrics. |
| C15 | Fair credit/approver allocation | Cut | One active proposal per workflow prevents flooding but is not cross-tenant fair scheduling. | Add tenant-weighted queues and credit reservations. |

The scope favors auditable, low-risk experiments and incident response over broad configuration autonomy. The CLI starts with no per-person scheduling or configuration writes outside governance.

### Quickstart

```bash
python -m pip install -r requirements.txt
python -m sim baseline --scenario scenarios/public_01_dev.yaml --run-dir runs/base_public_01_dev
python -m sim serve --scenario scenarios/public_01_dev.yaml --run-dir runs/mine_public_01_dev
# In a second terminal:
CONTROLLER_DB=.controller/public_01.sqlite3 ./run.sh http://127.0.0.1:8700
python -m sim grade --run runs/mine_public_01_dev --baseline runs/base_public_01_dev --out reports/public_01.md
python -m pytest -q tests
```

Set `CONTROLLER_DB` to a durable, writable path before starting the controller. For the full public-scenario benchmark, crash/restart proof, and reports, run:

```bash
python tools/verify_submission.py
```

## 3. Stakeholders

### 01 — Sales

- **Accepted:** Clear outcome measures and a concise operational status.
- **Declined:** “Auto-apply for everything” and blanket use of a persuasive prompt. Kestrel's policy only permits selected dimensions within a maximum shift; Northwind KYC disables auto-apply. Prompt changes require validation evidence and approval.
- **Incorrect claim:** Kestrel enables unrestricted auto-apply; the workflow policy is dimension- and shift-limited.
- **Reply:** “We’ll show separate pickup and engagement outcomes, but we won’t turn a demo lift into an unguarded rollout. The simulator policy only allows specific auto-applied dimensions and limits allocation movement; Northwind KYC requires approval. Prompt candidates will be validated and submitted with evidence. We’ll move quickly on a statistically supported, policy-permitted change and keep the comparison arm live.”

### 02 — Client Compliance

- **Accepted:** Evidence-backed prompt proposals, approval-hours awareness, and config-version audit.
- **Declined:** None of the governance protections; no feature-specific human approval override is added beyond the simulator contract.
- **Incorrect claim:** No claim contradicts the exposed policy. The API does not expose a named approver identity, so the controller cannot manufacture one.
- **Reply:** “Every prompt proposal includes a validation-run ID and pass rate, and every applied config version is retained with its proposal/approval identifiers. We submit only during the workflow’s published approval window and reconcile a changed base version rather than overwriting a supervisor edit. The simulator does not expose an individual approver’s name; we will report the approval ID and status rather than inventing identity.”

### 03 — Platform & SRE

- **Accepted:** Durable restart state, idempotency, bounded HTTP connections, timeouts, small retry limits, and actionable structured logs.
- **Declined:** Claiming a kill/restart proof or the ten-minute load target without executing it.
- **Incorrect claim:** No policy claim; the requested runtime envelope remains an unmeasured target here.
- **Reply:** “The event cursor, sufficient statistics, proposals, and audit records are persisted in SQLite. Chunk aggregation is atomic, proposal IDs are deterministic, and ambiguous advance/apply results are reconciled before repeating side effects. A public_01 kill/restart proof, public_03 throughput, and sampled process-tree peak working set are measured on this Windows host; the 4-vCPU/8-GB reference-host benchmark remains outstanding.”

### 04 — Kestrel Operations

- **Accepted:** One active request at a time, non-destructive supervisor edits, and no literal-script fallback based only on low hallucination.
- **Declined:** A two-request cap is not stated by the API; the controller remains more conservative with one active proposal per workflow.
- **Incorrect claim:** The memo’s request timing/cap is a client ask, not the enforced policy. The workflow policy publishes its own approval hours and request ceiling.
- **Reply:** “We keep one proposal active for the workflow, include the exact config diff and rationale, and treat a supervisor version change as the new base rather than undoing it. We’ll never pick a near-verbatim prompt solely because its safety score is high: it must pass validation, be approved, and then be monitored. Your workflow’s API policy controls the actual hours and request cap.”

### 05 — Product

- **Accepted:** Keep pickup and engagement distinct; use one event cohort ID; include a diff, affected-lead estimate, impact score, and dry-run details; use only observed exposure as evidence.
- **Declined:** Wishlist breadth beyond a reliable core; no UI or per-person call-time optimizer is claimed.
- **Incorrect claim:** No contract discrepancy is assumed. The cohort object used here is the simulator's event cohort ID, not a newly invented definition.
- **Reply:** “The current version keeps the simulator’s cohort ID as the shared key and reports pickup and engagement separately. Proposal scores and lead counts are bounded estimates, not causal forecasts; the request includes the observed sample and method so the supervisor can interpret them. I’ve cut per-person call-time and a dashboard rather than shipping unvalidated versions of both.”

### 06 — CTO

- **Accepted:** Explainability, low-credit incident response, and an honest production gap list.
- **Declined:** Treating every memo wishlist as P0 or presenting an unmeasured result as an outcome.
- **Incorrect claim:** The assignment contract, not any memo, defines the observable world; controller decisions use only the HTTP contract/events.
- **Reply:** “I chose an auditable two-arm learning loop and policy-aware incident proposals over broad automation. The fresh public_01/public_02 outcome reports and a public_01 restart proof are attached. Public_03 ingested 9.74 million events in 343.50 seconds with a 1,012.5 MiB sampled process-tree peak working set on this host; the required reference hardware remains unmeasured. Given a production handoff, I would close rollback and cohort coverage gaps.”

## 4. Architecture

```text
simulator HTTP API
  ├─ history/events chunk listing and NDJSON fetch
  │     └─ worker-thread parse → SQLite arm_totals, experiment observations
  │                              + chunk cursor (one transaction)
  ├─ workflow/config/policy reads
  │     └─ one shared event cohort ID + per-config-version evidence
  ├─ approvals list/reconcile → submit → poll → apply
  ├─ policy-permitted auto-apply (restricted dimensions/TV shift)
  └─ prompt suggestions → validation evidence → approval
                         │
                         └─ SQLite proposals + audit rows → --audit CLI
```

The simulator remains the source of truth for time, policy, approvals, and config. SQLite stores compact daily/arm sufficient statistics, processed chunk IDs, experiment definitions and person-level observations, durable proposals, and audit records. The registered experiment persistence API is not yet wired to the production planner/evaluator; production decisions still use daily arm totals. A restart reloads the cursor and outstanding proposals, fetches any unprocessed chunks, then reconciles remote approvals and config history. It does not import the simulator or inspect scenario/run files.

## 5. The learning loop

- **Cohorts:** The controller consumes the exact `cohort_id` emitted on each call. It operates on `default` for allocation proposals and does not add feature-specific cohorts, so no alternate definitions fragment the experiment.
- **Experiment design:** One dimension at a time. Engagement-primary workflows test welcome message then voice; pickup-primary workflows test caller ID. The initial comparison is 50/50, and a promoted arm remains at 80/20 for continued exposure. A config-version change isolates evidence from the prior experiment.
- **Minimum evidence:** At least 120 dials per arm for pickup or 40 connected calls per arm for engagement and its guardrail. A pooled two-proportion z-test requires a 99% two-sided threshold and at least a 1.5 percentage-point practical difference. A proposal is blocked when the observed guardrail drop exceeds its configured limit or is statistically adverse.
- **Multiple comparisons / stopping:** The production loop evaluates one two-arm comparison per config version; there is no adaptive multi-arm winner selection. Its single-test nominal false-positive bound is approximately 1% under the test assumptions. The persisted family/look alpha controls apply only to registered experiments and are not yet integrated with production evaluation. Caller-ID selection among several IDs and correlated repeated outcomes are not fully calibrated, so this is not a production false-winner guarantee.
- **Decay and rollback:** The 20% holdout keeps a losing arm measurable. Automatic decay rollback is not implemented; this is a known gap, not an implied capability.
- **Call time and caller ID:** Per-person preferred-hour learning is out of scope. Caller-ID incident evidence is limited to the default cohort and current configuration version, and evaluates the configured primary KPI plus its complementary guardrail before proposing a reduction.
- **Guardrail KPI:** Promotion waits for sufficient sample on both KPIs. It is rejected for an over-limit pickup/engagement decline; neither KPI is blended into a composite.

## 6. Governance

```text
planned → submitted → pending → approved → applied
                         ├────→ rejected
                         └────→ expired
approved → lapsed | stale-base conflict
rejected/expired/conflict → new proposal only with fresh simulated-day ID/config evidence
```

The controller keeps at most one pending/approved/submitted proposal per workflow, checks approvals during hourly reconciliation, and applies promptly. It submits approval requests only inside the workflow’s exposed approver hours. A changed base version is recorded as a conflict and is never silently overwritten. New proposal IDs include workflow, base version, content hash, and simulated day.

Auto-apply is considered only when the policy enables it, every changed dimension is allowed, and the computed total-variation allocation shift is within `max_shift`. Prompt changes always use the approval path and carry a matching validation run. The controller batches changes by choosing only one action per workflow reconciliation.

`affected_leads` is one published daily batch size. `dry_run` includes the observed call count/cohort and method. The impact score is a bounded ranking heuristic based on the observed difference/sample size (or a conservative incident estimate); it is not a causal lift forecast. These estimates need historical replay before production use.

## 7. Incidents

- **Caller ID:** Compare recent two-day outcomes only within the default cohort and current configuration version. Each DID must have at least 100 dials, plus the primary metric's minimum sample (120 dials for pickup or 40 connected calls for engagement). A reduction must be at least eight percentage points and pass the two-proportion threshold on the configured primary KPI; the complementary KPI is checked against its guardrail. This heuristic does not yet correct for time-of-day or residual confounding.
- **Prompt safety:** Compare at least 100 connected calls on the current prompt with at least 100 historical connected calls on other prompts. A ≥2.5 percentage-point QA-flag increase and z-score ≥2.58 triggers a suggestion call. Candidates are validated with `n=400`; only a candidate meeting the workflow threshold is submitted with its validation ID. The noisy detector and lack of post-apply verification are limitations.
- **Credits:** Prompt diagnosis costs 3 credits and each validation costs 4. Checks are limited to once per workflow per simulated day, and 402 responses stop the remediation attempt rather than looping.
- **Timeline:** The fresh public_02 run completed with the simulator-reported `ks_cancel` supervisor edit-to-fix MTTR of 270 simulated minutes. The report records the incident and resulting applied proposal.

## 8. Scale

Public_03 completed in 343.50 controller wall-clock seconds across five simulated days, ingesting 9,737,481 events (28,348 events/second; 68.70 seconds per simulated day). The sampled controller process-tree peak working set was 1,012.5 MiB. This Windows host is not the required 4-vCPU/8-GB reference host. Ingestion parses one remote chunk at a time in a worker thread and aggregates into compact per-arm/per-day SQLite rows, with separate person-level observations only for registered experiments. HTTP connections are capped at eight; the controller advances one simulated hour per call.

At 10×, the likely bottlenecks are SQLite’s single writer, chunk transfer/JSON parsing, and simulator’s serialized advance endpoint. The next scale step is measured batching/queueing, then partition statistics by tenant/workflow and move to managed Postgres only if local SQLite write throughput fails the target. No new stateful service is justified without that measurement.

## 9. Reliability

- **Chunk idempotency:** A chunk primary key and transactional arm-total upserts ensure a replayed chunk does not double-count.
- **Proposal idempotency:** A proposal is persisted before its remote request. Retries reuse its deterministic ID; approvals/config history reconcile a remote success after a crash.
- **Advance idempotency:** `/sim/advance` is not blindly retried. After an ambiguous transport failure, the controller reads `/sim/state` before deciding whether another advance is needed.
- **4xx/5xx:** Validation and other non-retryable 4xx responses surface as explicit errors. Safe GETs and proposal-keyed requests retry transient statuses with bounded jitter. Cost-bearing validation/suggestion requests and time-advancing/apply writes are not blindly retried.
- **Crash proof:** Public_01 was interrupted at simulated minute 60 and completed after restarting with the same SQLite database. See `reports/crash_recovery_public_01.txt`.

## 10. Audit & observability

Run `python agentic_controller.py --db .controller/controller.sqlite3 --audit --workflow nw_renewal` to read the decision audit. Each row has timestamp, workflow, action, and JSON detail including proposal/approval IDs, diff, rationale, and applied result. Approval identity is only available as the simulator’s approval ID.

Structured logs expose controller lifecycle and failures. Production alerts should page on persistent chunk-fetch failure, ingest lag beyond the business SLO, repeated stale-base conflicts, missing approval reconciliation, credit exhaustion during an active safety incident, and database write failure. Individual low-volume KPI fluctuations and a single QA flag should not page.

## 11. Results

The grader reports are generated from the simulator's public baselines and completed controller runs:

- `reports/public_01.md`: overall conclusion rate +7.4% vs baseline, engagement +1.8%, pickup +4.8%; four beneficial and one neutral applied changes.
- `reports/public_02.md`: overall conclusion rate +9.4%, engagement +3.5%, pickup +4.9%; four beneficial, one neutral, and one harmful applied changes. The controller received one rejection and had no apply conflicts or lapsed approvals.
- `reports/crash_recovery_public_01.txt`: the controller restarted at simulated minute 60 and reached the final state at minute 11,520 with event ID 297,035 in 64.46 seconds total.
- Public_02 reached its final state at minute 14,400 with event ID 324,319 in 92.29 seconds. These timings are this Windows host's scenario execution times, not the public_03 scale result.
- `reports/public_03_load.md`: 9,737,481 events in 343.50 seconds (28,348 events/second), with a sampled 1,012.5 MiB process-tree peak working set across five simulated days. This is not the reference host.

## 12. Trade-offs & rejected alternatives

| Option | Why considered | Why rejected / what instead |
|---|---|---|
| LLM chooses the winner | Fast hypothesis generation | LLM suggestions are candidates only; statistical evidence determines promotion. |
| Promote 100% | Maximizes short-term exploitation | Keep 20% exploration to retain a comparison and detect deterioration. |
| Feature-specific micro-cohorts | Could capture heterogeneous effects | Increases fragmentation and false winners; this version uses canonical event cohort IDs only. |
| New Postgres/Redis service | Familiar production components | Not justified for a single-controller assignment; SQLite keeps state local and restartable. |
| Per-person call-time tuning | Summit-requested headline feature | Requires robust preference estimates, more policy surface, and larger evidence; left untouched. |
| Auto-apply prompt fixes | Low-latency incident response | Safety evidence and human governance take precedence. |

## 13. Known weaknesses

Fresh public_01/public_02 runs, public_01 restart behavior, and public_03 throughput and sampled process-tree memory are verified. Reference-host performance remains unmeasured. One harmful applied change appeared in the public_02 report; the thresholds and rollback strategy need further calibration. The implementation does not implement automatic decay rollback, feature-segment creation, per-person scheduling, a supervisor UI, complete observability, or tenant-fair credit scheduling. Impact scores are only queue-ranking heuristics.

## 14. If this went to production at Formi next month

1. Investigate the harmful changes in both public grader reports; add hidden-like synthetic tests before rollout.
2. Extend the restart test to each remote side-effect boundary and publish audit reconciliation evidence.
3. Benchmark public_03 on the required 4-vCPU/8-GB target; capture event lag, wall time, memory, credits, and SQLite size.
4. Add rolling-window decay detection and tested rollback with explicit supervisor notification.
5. Add calibrated cohort definitions, sequential/multiple-comparison controls, and historical replay for impact estimates.
6. Export OpenTelemetry traces/metrics, actionable alerts, and a read-only supervisor view.
7. Only then evaluate Postgres persistence, distributed rate limits, dead-letter handling, and tenant-fair scheduling.
