# Experiment design

This document describes the proposed fixed-horizon experimentation design for
the Controller. It distinguishes assignment/API requirements from current
behavior and engineering decisions. The design below is not a claim that the
experiment engine has already been implemented.

## Requirements, current behavior, and decisions

### Assignment and simulator contract

- Pickup rate is connected calls divided by dials. Engagement rate is engaged
  calls divided by connected calls. A workflow declares a primary KPI and the
  other KPI as its guardrail.
- Call events expose workflow, cohort, configuration version, event ID,
  timestamp, variant assignments, `person_id`, `lead_id`, `attempt_no`, and
  outcomes. The simulator documents globally increasing event IDs and stable
  person IDs across returning campaigns.
- Variant dimensions are sampled independently on each dial from the
  configured cohort distribution. A person's later calls can therefore use a
  different variant.
- The Controller communicates with the simulator through HTTP. Historical
  event chunks may be retrieved through the documented chunk endpoints.
- The public simulator state exposes `end_minute`, `last_event_id`, and
  `finished`; an experiment cannot extend beyond the public scenario horizon.
- Hidden truth, simulator-private run files, and scenario-file internals are
  not experiment inputs.

### Existing Controller behavior

- Event ingestion retains daily aggregate counts keyed by workflow, cohort,
  configuration version, day, dimension, and variant. For explicitly
  registered running experiments it also persists the earliest eligible
  call per person, including event attribution, in a separate observation
  table. The daily aggregates remain all-call operational metrics.
- Experiment evaluation currently uses a two-proportion z test, minimums of
  120 dials per pickup arm and 40 connected calls per engagement arm, a
  `CONFIDENCE_Z` value of 2.58, and a 0.015 absolute primary-effect floor.
- The current evaluator records a workflow/config-version evaluation marker
  once sufficient data is seen. This is not a fixed-horizon experiment
  definition, and the marker does not store a comparison pair, analysis
  boundaries, or planned looks.
- The Store can persist experiment families, frozen definitions, three
  ordered event cutoffs, and look-scoped observation reads. These APIs are
  not called by the production planner/evaluator: `_detect_and_plan()` still
  uses `_experiment_change()` and the daily `arm_totals` path. The current
  production decision loop therefore does not claim the registered,
  person-level fixed-horizon design described below.
- Current guardrail behavior uses a workflow margin when supplied and falls
  back to 0.02. Its current significance checks are not a formal
  non-inferiority confidence-bound test.
- Durable proposal submission, approval, application, recovery, and audit
  behavior already exist and should remain the governance path for promoted
  changes.

### Engineering assumptions for the proposed design

- Target power is 80%. This is an engineering assumption, not a simulator or
  assignment guarantee.
- The practically meaningful primary improvement is an absolute 1.5
  percentage points.
- The family-wise significance budget is 1%, shared across a predeclared
  family of hypotheses and three analysis looks.
- Each experiment uses a 50/50 control/candidate allocation while collecting
  evidence, followed by an 80/20 allocation only after a successful decision
  and through the existing governance path.
- The statistical estimand is the outcome of the first eligible call per
  person in the experiment. This avoids counting repeated calls from one
  person as independent observations. It does not claim to estimate
  all-attempt operational performance.
- If the planned target cannot be reached by the public simulator horizon,
  the experiment is marked underpowered; insufficient evidence results in no
  promotion.

## Metrics and population

Keep operational monitoring and experimental inference distinct:

| Metric | Operational rate | Experimental first-contact rate |
|---|---|---|
| Pickup | Connected / all dials | First eligible connected call / first eligible call per person |
| Engagement | Engaged / connected calls | First eligible engaged call / first eligible connected call per person |

Use the workflow's declared primary KPI for the promotion hypothesis and its
declared guardrail KPI for the safety hypothesis. Never substitute one
denominator for the other. Persist the cohort explicitly; the first
implementation may use the default cohort only if that is the declared
experiment population.

Historical baseline rates used for planning must be computed from legitimate
historical event chunks fetched through the public HTTP API. Apply the same
first-eligible-call-per-person rule, metric definitions, workflow/cohort
selection, and configuration-version isolation. Existing daily aggregates do
not retain person identity and cannot be used to reconstruct unique-person
baselines. Do not infer identity or outcomes from aggregate counts, and do not
use hidden truth.

## Experiment definition and assignment

Persist the complete experiment definition before collecting outcomes:

- Experiment and family IDs.
- Workflow, cohort, configuration version, and changed dimension.
- Frozen control and candidate variants.
- Primary and guardrail metrics and the guardrail deterioration margin.
- Planned allocation, practical effect, target power, target sample sizes, and
  baseline rates used for planning.
- Family significance budget, number of hypotheses, planned looks, and
  per-look alpha.
- Start boundary, look boundaries, final cutoff, and status.

Choose exactly one control/candidate pair before observation begins. Do not
choose the two best observed performers after collecting outcomes. Any
additional variants or comparisons must be registered in the family before
their outcomes are examined.

The simulator assigns variants per dial, not permanently per person. The
Controller therefore does not claim to freeze simulator-side assignment. It
includes only each person's first eligible call in the experiment's analysis
and excludes subsequent calls from that person's experiment observations.
Continue retaining all-call aggregates separately for operational metrics.

All experiment observations must match the registered workflow, cohort,
configuration version, dimension, and frozen pair. A configuration-version
change ends or cancels that experiment; do not combine data across versions.

## Event boundaries and persistence

Use global event IDs rather than daily aggregates for experiment boundaries:

1. Once the planned treatment configuration is confirmed active, read public
   simulator state and persist `start_event_id = last_event_id`. Eligible
   calls have event IDs greater than this boundary.
2. Process each fetched chunk in event-ID order. Chunks may contain events
   from multiple workflows; filter every event by the registered experiment
   identity and configuration.
3. At each planned look, persist the exact inclusive `cutoff_event_id` of the
   event that completes the required information for that look. Do not use
   calls with larger IDs for that look, even if they are in the same chunk.
4. At the final horizon, use the public simulator completion state and its
   last event ID as the upper bound. If a target is reached sooner, stop
   enrollment at its event boundary; later events are outside the experiment.

Persist an event-ID-deduplicated first-call observation with at least:
experiment ID, person key, event ID, timestamp, variant, connected, and
engaged. A uniqueness constraint on experiment/person enforces one included
call per person. Use a pseudonymous key or protected local representation of
`person_id`; do not put raw identity in user-facing audit details.

This person-level data supports direct first-contact analysis. If a future
estimand includes multiple calls per person, it must explicitly address
within-person correlation and variant crossover, for example with a
person-clustered variance estimator. A clustered standard error alone does
not resolve treatment carryover or interference.

## Planning and statistical decisions

### Sample size and feasibility

For each registered primary KPI, calculate a two-arm proportion sample target
using the legitimate historical baseline rate, an absolute effect of 0.015,
80% target power, and the adjusted per-look significance level. Calculate
pickup using calls per arm and engagement using connected calls per arm.
Document the assumptions and retain the formula inputs and outputs with the
experiment.

Before enrollment, compare the target with feasible unique-person volume
estimated from public historical event data and public workflow capacity.
Record the target, estimated feasible sample, expected power, and detectable
effect. Do not relax the effect size, significance budget, or power claim to
make the target appear feasible. The estimate is not a guarantee: actual
eligible unique-person traffic may be lower.

An underpowered experiment can still show a large effect that meets all
predeclared decision rules. Underpowered is a planning/attainment property,
not permission to weaken the test. If evidence does not meet the statistical,
practical-effect, and guardrail criteria, make no promotion and record an
inconclusive outcome.

### Family-wise alpha and planned looks

Close and persist the experiment family before outcomes are evaluated. Let
`M` be the number of predeclared hypotheses in that family, counting each
experiment's primary and guardrail hypotheses. Use three fixed looks at 50%,
75%, and 100% of planned information. Allocate the 1% family-wise budget
conservatively:

`alpha_per_hypothesis_per_look = 0.01 / (M * 3)`

Use this alpha for sample-size planning as well as inference. The primary
improvement test is two-sided. The guardrail test is one-sided. The
Bonferroni allocation is conservative and remains valid when hypotheses or
looks are dependent; do not replace it with unadjusted repeated testing.
Changing the family or look schedule requires a new registered plan before
examining results.

### Primary KPI

At each eligible look, compare the frozen candidate with the frozen control
using the registered metric and first-contact denominators. A promotion
requires all of the following at a planned look:

- The adjusted primary test passes.
- Candidate-minus-control improvement is at least 0.015.
- The guardrail non-inferiority criterion passes.

Do not select a winner from unplanned arms or report an observed best arm as
a winner without its registered comparison and adjusted evidence.

### Guardrail non-inferiority

Let `d` be candidate-minus-control guardrail rate, `m` the permitted
deterioration margin, and `SE(d)` the standard error for the first-contact
analysis. Accept non-inferiority only when the adjusted one-sided lower bound
passes:

`d - z_critical * SE(d) > -m`

Read `m` from the workflow's public objective when supplied. Until the
documented/live objective schema is verified for a workflow, a 0.02 fallback
is only an engineering default; persist and report that the fallback was
used. Do not treat a point estimate alone as proof of non-inferiority.

## State machine and governance

Suggested lifecycle:

`planned -> awaiting_configuration -> running -> completed -> evaluated`

Additional terminal outcomes are `inconclusive` and `cancelled`. A version
change, invalidated pair, or unavailable population can cancel an experiment;
reaching the public horizon before adequate evidence marks it underpowered
and, if decision criteria are unmet, inconclusive.

Only the final planned analysis can create a promotion proposal. A passing
primary and guardrail result moves the experiment to evaluated and creates a
proposal through the existing durable proposal system. A failed or
inconclusive decision is audited without proposing a configuration change.
Do not bypass approval, auto-apply policy, version checks, crash recovery, or
concurrency protections.

## Evidence and audit

Persist sufficient evidence to reproduce the decision:

- Experiment/family IDs, population, configuration version, and frozen pair.
- Start, look, and final event-ID boundaries.
- Per-arm eligible-person counts and metric numerators/denominators.
- Historical baseline, target effect, target power, sample target, achieved
  sample, and detectable effect/achieved power where calculated.
- Primary effect direction and magnitude, test statistic, adjusted alpha,
  and decision.
- Guardrail margin, candidate-minus-control difference, standard error,
  adjusted lower confidence bound, and decision.
- Underpowered, inconclusive, cancelled, or promoted status and reason.

Proposal evidence and audit evidence must use the same event cutoff and
reported arm counts. Never synthesize missing observations or evidence.

## Implementation boundaries

The Store currently has additive tables for experiment families, experiment
definitions, planned looks, and person-level observations. Registration
freezes the control/candidate pair and configuration population; family
freezing validates two hypotheses (primary and guardrail) per registered
experiment. Event ingestion deduplicates observations by experiment/person,
keeps the earliest event when chunks are replayed out of order, and performs
aggregate updates, experiment observations, and chunk receipts atomically.
Look cutoffs must be established in order and increase strictly. Queries for
a look use its persisted cutoff and the experiment start boundary. Setting
the final look also records `end_event_id`; the current look-3 query uses the
same persisted look cutoff, which the Store writes atomically with that end
boundary.

This persistence layer is not connected to automatic experiment planning,
statistical evaluation, or promotion. Historical bootstrap must still fetch
documented history chunks over HTTP and apply the same inclusion rule; do not
infer person-level statistics from daily aggregates. Observation rows
currently retain the simulator's `person_id` locally for deduplication; a
production deployment should review retention and consider a stable keyed
pseudonym before processing real identities.

The initial statistical calculations can use the existing Python/NumPy
environment and validated pure functions. The design deliberately uses
conservative Bonferroni correction for its fixed looks rather than requiring
an unvalidated group-sequential boundary implementation.
