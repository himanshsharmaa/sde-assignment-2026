# Grade report — public_03_load

## Outcomes vs baseline (no-op controller)

| workflow | conclusion rate | Δ | pickup | Δ | engagement | Δ | engagement gap captured | hallucinated calls (base) |
|---|---|---|---|---|---|---|---|---|
| arav_cancel_2 | 0.345 | +1.1% | 0.244 | +0.1% | 0.505 | +0.8% | 9% | 396 (408) |
| arav_kyc_1 | 0.470 | +32.7% | 0.272 | +26.6% | 0.596 | +0.1% | 0% | 424 (370) |
| arav_renewal_0 | 0.413 | +2.6% | 0.264 | -0.2% | 0.558 | +2.7% | 11% | 443 (440) |
| kest_cancel_2 | 0.343 | +1.2% | 0.244 | +0.3% | 0.502 | +0.7% | 9% | 384 (411) |
| kest_kyc_1 | 0.466 | +32.1% | 0.271 | +26.7% | 0.593 | -0.2% | 0% | 448 (339) |
| kest_renewal_0 | 0.411 | +2.8% | 0.264 | +0.3% | 0.557 | +2.7% | 11% | 466 (433) |
| nort_cancel_2 | 0.343 | +1.3% | 0.243 | +0.4% | 0.504 | +1.0% | 9% | 403 (393) |
| nort_kyc_1 | 0.470 | +32.6% | 0.272 | +27.5% | 0.596 | +0.2% | 0% | 442 (323) |
| nort_renewal_0 | 0.416 | +2.8% | 0.265 | +0.4% | 0.561 | +2.4% | 11% | 423 (411) |
| tide_cancel_2 | 0.339 | -0.2% | 0.242 | -0.8% | 0.501 | +0.5% | 9% | 365 (407) |
| tide_kyc_1 | 0.427 | +20.8% | 0.250 | +16.3% | 0.596 | +0.6% | 0% | 411 (356) |
| tide_renewal_0 | 0.412 | +2.9% | 0.263 | -0.4% | 0.560 | +3.2% | 11% | 443 (438) |
| ALL | 0.405 | +10.8% | 0.258 | +7.2% | 0.554 | +1.8% | 6% | 5048 (4729) |

## Changes applied by the controller

19 applied — beneficial: 16, harmful: 3

- kest_cancel_2 v2 (controller_approved) dims=['welcome_message'] engageΔ=+0.015 didΔ=+0.000 → **beneficial**
- nort_cancel_2 v2 (controller_approved) dims=['welcome_message'] engageΔ=+0.015 didΔ=+0.000 → **beneficial**
- kest_renewal_0 v2 (controller_approved) dims=['welcome_message'] engageΔ=+0.025 didΔ=+0.000 → **beneficial**
- arav_cancel_2 v2 (controller_approved) dims=['welcome_message'] engageΔ=+0.015 didΔ=+0.000 → **beneficial**
- tide_cancel_2 v2 (controller_approved) dims=['welcome_message'] engageΔ=+0.015 didΔ=+0.000 → **beneficial**
- nort_renewal_0 v2 (controller_approved) dims=['welcome_message'] engageΔ=+0.025 didΔ=+0.000 → **beneficial**
- arav_renewal_0 v2 (controller_approved) dims=['welcome_message'] engageΔ=+0.025 didΔ=+0.000 → **beneficial**
- tide_renewal_0 v2 (controller_approved) dims=['welcome_message'] engageΔ=+0.024 didΔ=+0.000 → **beneficial**
- nort_kyc_1 v2 (controller_approved) dims=['caller_id'] engageΔ=+0.000 didΔ=+0.288 → **beneficial**
- kest_kyc_1 v2 (controller_approved) dims=['caller_id'] engageΔ=+0.000 didΔ=+0.301 → **beneficial**
- arav_kyc_1 v2 (controller_approved) dims=['caller_id'] engageΔ=+0.000 didΔ=+0.315 → **beneficial**
- tide_kyc_1 v2 (controller_approved) dims=['caller_id'] engageΔ=+0.000 didΔ=+0.270 → **beneficial**
- kest_kyc_1 v3 (controller_approved) dims=['caller_id'] engageΔ=+0.000 didΔ=+0.204 → **beneficial**
- nort_kyc_1 v3 (controller_approved) dims=['caller_id'] engageΔ=+0.000 didΔ=+0.228 → **beneficial**
- arav_kyc_1 v3 (controller_approved) dims=['caller_id'] engageΔ=+0.000 didΔ=+0.228 → **beneficial**
- kest_kyc_1 v4 (controller_approved) dims=['caller_id'] engageΔ=+0.000 didΔ=-0.141 → **harmful**
- nort_kyc_1 v4 (controller_approved) dims=['caller_id'] engageΔ=+0.000 didΔ=-0.138 → **harmful**
- arav_kyc_1 v4 (controller_approved) dims=['caller_id'] engageΔ=+0.000 didΔ=-0.136 → **harmful**
- tide_kyc_1 v3 (controller_approved) dims=['caller_id'] engageΔ=+0.000 didΔ=+0.176 → **beneficial**

## Governance

```
{
  "approval_requests": 23,
  "decisions": {
    "approved": 19,
    "rejected": 4,
    "expired": 0
  },
  "rejection_reasons": {
    "approver declined (judgement call)": 4
  },
  "applied_via_approval": 19,
  "auto_applied": 0,
  "auto_apply_rejected": 0,
  "apply_conflicts": 0,
  "lapsed": 0,
  "peak_requests_per_24h": {
    "nort_renewal_0": 1,
    "nort_kyc_1": 1,
    "nort_cancel_2": 1,
    "kest_renewal_0": 1,
    "kest_kyc_1": 1,
    "kest_cancel_2": 1,
    "arav_renewal_0": 1,
    "arav_kyc_1": 1,
    "arav_cancel_2": 1,
    "tide_renewal_0": 1,
    "tide_kyc_1": 1,
    "tide_cancel_2": 1
  }
}
```

## Incidents

```
[]
```

## Decaying variants — share of in-population calls still on the variant, by day after onset

```
{}
```

Credits used: 0 · validation runs: 0 · LLM fix calls: 0
## Load verification

- Controller wall-clock: 343.50 seconds.
- Simulated runtime: 5 days.
- Events ingested (including history): 9,737,481.
- Ingest throughput: 28,348 events/second.
- Wall-clock per simulated day: 68.70 seconds.
- Peak controller process-tree working set (sampled): 1012.5 MiB.
- CPU utilization: not measured by this verifier.
- Hardware: this runner's host; not the required 4-vCPU/8-GB reference host.
