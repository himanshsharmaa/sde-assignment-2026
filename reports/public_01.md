# Grade report — public_01_dev

## Outcomes vs baseline (no-op controller)

| workflow | conclusion rate | Δ | pickup | Δ | engagement | Δ | engagement gap captured | hallucinated calls (base) |
|---|---|---|---|---|---|---|---|---|
| ks_cancel | 0.346 | +4.1% | 0.241 | +1.2% | 0.501 | +2.0% | 8% | 62 (47) |
| nw_kyc | 0.459 | +26.2% | 0.298 | +23.3% | 0.591 | +0.1% | 0% | 33 (21) |
| nw_renewal | 0.423 | +0.8% | 0.263 | -0.3% | 0.554 | +1.5% | 11% | 47 (42) |
| ALL | 0.396 | +7.4% | 0.260 | +4.8% | 0.539 | +1.8% | 7% | 142 (110) |

## Changes applied by the controller

5 applied — beneficial: 4, neutral: 1

- nw_renewal v2 (controller_approved) dims=['welcome_message'] engageΔ=+0.025 didΔ=+0.000 → **beneficial**
- ks_cancel v2 (controller_approved) dims=['welcome_message'] engageΔ=+0.015 didΔ=+0.000 → **beneficial**
- nw_kyc v2 (controller_approved) dims=['caller_id'] engageΔ=+0.000 didΔ=+0.251 → **beneficial**
- nw_kyc v3 (controller_approved) dims=['caller_id'] engageΔ=+0.000 didΔ=+0.133 → **beneficial**
- nw_kyc v4 (controller_approved) dims=['caller_id'] engageΔ=+0.000 didΔ=-0.013 → **neutral**

## Governance

```
{
  "approval_requests": 6,
  "decisions": {
    "approved": 5,
    "rejected": 1,
    "expired": 0
  },
  "rejection_reasons": {
    "approver declined (judgement call)": 1
  },
  "applied_via_approval": 5,
  "auto_applied": 0,
  "auto_apply_rejected": 0,
  "apply_conflicts": 0,
  "lapsed": 0,
  "peak_requests_per_24h": {
    "nw_renewal": 1,
    "nw_kyc": 2,
    "ks_cancel": 1
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