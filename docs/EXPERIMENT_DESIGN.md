# Experiment design

This document describes the fixed-horizon experimentation design for the
Controller and distinguishes assignment/API requirements, implemented
behavior, and remaining engineering work. The Store and evaluator support
explicitly registered plans. The normal run loop now checks for applied
exploration proposals before evaluation, but it safely skips new plan
registration because the currently documented inputs are insufficient to
build a defensible plan.

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
- For explicitly registered, frozen experiments, `Controller.run()` invokes
  the planned-look evaluator. It uses persisted person-level observations,
  event cutoffs, a two-proportion primary test, family/look-adjusted alpha,
  the registered practical-effect threshold, and a one-sided guardrail
  non-inferiority bound. Each look requires its registered primary and
  guardrail denominators in both arms. Only final registered targets are
  subject to the 120-dial pickup and 40-connected-call engagement minimums;
  interim looks use the persisted information fractions of those targets.
- An experiment becomes terminal after a beneficial, inconclusive, or
  guardrail-blocked look. If the simulator finishes before a required target
  is reached, it is marked underpowered. Registered comparisons that do not
  pass their decision criteria do not create proposals.
- Before each registered evaluation, the run loop checks durable proposals
  for applied, governance-completed two-arm exploration changes. Existing
  plans linked to those proposals are left unchanged. New plans are not
  registered when their statistical inputs are unavailable; an explicit
  warning explains the missing inputs. Pending, approved-but-not-applied,
  rejected, expired, conflicted, or otherwise ineligible proposals are never
  enrolled.
- The legacy `_detect_and_plan()` path still uses `_experiment_change()` to
  submit two-arm exploration proposals when there is not yet a comparison.
  It does not persist complete experiment definitions. Unregistered
  aggregate comparisons are prevented from promoting. The evaluator is
  connected to the run loop, but normal public runs still do not create new
  registered experiments automatically.
- `CONFIDENCE_Z` and the existing daily aggregates remain in use by the
  separate incident-remediation path; they are not the registered experiment
  evaluator's significance threshold.
- Registered experiments require explicit baseline rates, sample targets,
  and guardrail margins. The Store's daily `arm_totals` retain neither person
  IDs nor event-level first-contact attribution, so they cannot provide a
  compatible baseline for the registered first-contact estimand. The
  documented workflow API identifies primary and guardrail KPIs but does not
  specify a deterioration-margin field. There is also no persisted
  production declaration of experiment-family membership. Registration
  therefore skips rather than inferring these values or reusing observations
  from a different configuration version.
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
- Target power is persisted with the experiment plan, but the evaluator does
  not recalculate sample size or achieved power. The plan's sample targets
  must be prepared and registered before collection.
- A successful registered experiment proposes an 80/20 candidate/control
  allocation through the existing governance path. The evaluator does not
  change simulator configuration directly.
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

The current Controller does not persist historical call events at person
level for a new plan. The `history` chunk ingestion path updates all-call
daily aggregates, but after ingestion discards event-level person history
unless a registered experiment was already running. A new experiment also
starts after its approved configuration is applied, so observations from an
earlier configuration version cannot silently stand in for the required
same-version baseline. The documented workflow response does not define the
guardrail deterioration margin, and the production planner has no explicit
family declaration from which to allocate the family alpha. These missing
inputs make automatic registration unsafe today.

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
proposal requires all of the following at an evaluated look:

- The adjusted primary test passes.
- Candidate-minus-control improvement is at least 0.015.
- The guardrail non-inferiority criterion passes.

Do not select a winner from unplanned arms or report an observed best arm as
a winner without its registered comparison and adjusted evidence.

### Guardrail non-inferiority

Let `m` be the permitted deterioration margin and `alpha` the frozen
family-adjusted one-sided significance level for this look. For candidate rate
`p_c`, control rate `p_0`, and their independent Wilson score limits `L_c` and
`U_0`, calculate the one-sided Newcombe-Wilson hybrid score lower bound:

`(p_c - p_0) - sqrt((p_c - L_c)^2 + (U_0 - p_0)^2)`

The Wilson limits use `z = Phi^-1(1 - alpha)`, so the lower bound is at
confidence level `1 - alpha`. Accept non-inferiority only when this bound is
strictly greater than `-m`. The method assumes two independent binomial
samples with fixed denominators; first-contact observations include at most
one call per person. There is no Wald standard-error fallback, including when
either observed rate is zero or one.

The guardrail deterioration margin `m` must be predeclared and persisted with
the plan. The documented `GET /workflows/{id}` response defines the primary and
guardrail KPIs but does not document a margin field. Until a legitimate
public or Controller-owned approved source is added, a missing margin makes a
plan ineligible for evaluation; do not supply a default or promote from a
point estimate alone.

## State machine and governance

### Production registration gate

Before evaluating persisted experiments, `Controller.run()` calls
`_register_eligible_experiment_plans()`. The method considers only durable
`applied` proposals of the approval or policy-permitted auto-apply kind whose
rationale identifies the initial two-arm exploration. It ignores proposals
that are not applied and does not replace or reset an experiment already
linked to a proposal.

The method deliberately does not infer a baseline from `arm_totals`, assign
sample counts, choose a guardrail margin, or assume a family size. It logs a
warning and creates no plan because the current production Store lacks a
compatible pre-treatment person-level baseline for the experiment's exact
configuration version and cohort; the documented workflow response does not
define a guardrail margin, and the Store has no predeclared family membership
for the proposal. Consequently, persisted plans survive controller restarts,
but new public-run experiments remain disabled until these inputs have a
documented, validated source. Registration alone never promotes or applies a
configuration.

Registered lifecycle:

`planned -> running -> beneficial | inconclusive | guardrail_blocked`

Other terminal outcomes are `cancelled` and `underpowered`. A proposal
conflict can cancel its linked planned experiment. Reaching the public
horizon before adequate evidence marks a running experiment underpowered.

The evaluator may stop at an earlier planned look when the adjusted primary
and guardrail criteria pass; the three-look Bonferroni allocation accounts
for those planned repeated looks. A passing result marks the experiment
beneficial and creates a proposal through the existing durable proposal
system. A failed final look or an underpowered run is audited without
proposing a configuration change.
Do not bypass approval, auto-apply policy, version checks, crash recovery, or
concurrency protections.

### Crash-safe promotion cursor

For an engagement-rate promotion, proposal status finalization, application
audit insertion, cursor toggle, and insertion into
`proposal_cursor_advancements` occur in one `BEGIN IMMEDIATE` SQLite
transaction. The ledger is keyed uniquely by proposal ID; reconciliation
checks it independently of the application audit. A repeated finalization
sees the durable `advanced` record and does not toggle the cursor again. The
existing order remains `welcome_message -> voice -> welcome_message`, and
pickup-rate promotions do not advance this cursor.

The one-time migration records an already-applied promotion without a ledger
row as `legacy_unverified`; a migration marker is committed with those rows
so a restart cannot mistake an interrupted migration for a completed one.
The older database did not record whether its cursor toggle had occurred, so
migration neither infers completion from an audit nor toggles the cursor:
either action could silently skip or repeat a dimension. These legacy records
are logged and require explicit operator reconciliation against the
historical promotion/cursor sequence. New post-migration proposals with a
missing ledger record are recoverable from the applied proposal and workflow
KPI; reconciliation finalizes them even when the application audit already
exists.

### Interim sample thresholds

`MIN_REGISTERED_TARGET_ARM_DIALS` (120) and
`MIN_REGISTERED_TARGET_ARM_CONNECTED` (40) validate the final registered
sample targets only. They are not per-look floors. The three looks require
`ceil(final_target * information_fraction)` in each arm for both metrics.
Therefore a plan whose final target equals the minimum can have an eligible
first look below that minimum; for example, the 50% look for a 40-connection
engagement target requires 20 connected calls per arm. This intentionally
preserves the registered 50%/75%/100% schedule rather than imposing new
minimums at interim analyses.

## Evidence and audit

Persist sufficient evidence to reproduce the decision:

- Experiment/family IDs, population, configuration version, and frozen pair.
- Start, look, and final event-ID boundaries.
- Per-arm eligible-person counts and metric numerators/denominators.
- Historical baseline, target effect, target power, sample target, achieved
  sample, and detectable effect/achieved power where calculated.
- Primary effect direction and magnitude, test statistic, adjusted alpha,
  and decision.
- Guardrail margin, candidate-minus-control difference, Newcombe-Wilson
  method and confidence level, adjusted lower confidence bound, and decision.
- Underpowered, inconclusive, cancelled, or promoted status and reason.

Proposal evidence and audit evidence must use the same event cutoff and
reported arm counts. Never synthesize missing observations or evidence.

## Implementation boundaries

The Store has additive tables for experiment families, experiment
definitions, planned looks, and person-level observations. Registration
freezes the control/candidate pair and configuration population; family
freezing validates two hypotheses (primary and guardrail) per registered
experiment. Event ingestion deduplicates observations by experiment/person,
keeps the earliest event when chunks are replayed out of order, and performs
aggregate updates, experiment observations, and chunk receipts atomically.
Look cutoffs must be established in order and increase strictly. Queries for
a look use its persisted cutoff and the experiment start boundary. Setting
the final look also records `end_event_id`, and the look-3 query uses that
persisted boundary. Enrollment continues after the first and second looks;
terminal experiment state stops further enrollment while ordinary daily
aggregates continue to include all valid calls.

The registered-look evaluator is connected to the Controller run loop, but
the automatic planner does not yet produce complete registered plans.
Historical bootstrap must fetch documented history chunks over HTTP and
apply the same inclusion rule; person-level statistics must not be inferred
from daily aggregates. Observation rows currently retain the simulator's
`person_id` locally for deduplication; a production deployment should review
retention and consider a stable keyed pseudonym before processing real
identities.

The registered evaluator uses the standard two-proportion z test for the
primary KPI and the one-sided Newcombe-Wilson hybrid score lower bound for
guardrail non-inferiority on one first-contact observation per person. It
does not implement a person-cluster robust variance estimator; the
one-observation-per-person estimand is the independence assumption.
Bonferroni correction conservatively allocates alpha across every registered
primary and guardrail hypothesis and all three planned looks.

Other remediation heuristics remain outside this registered design. In
particular, `_incident_change()` chooses the observed best and worst caller
IDs using the same recent event sample used for testing, without selection
adjustment. `_prompt_change()` compares current prompt outcomes to historical
prompt aggregates that may span different configuration versions and does
not bootstrap a registered person-level baseline. These paths are not
claimed to have fixed-horizon or multiplicity-controlled inference.
