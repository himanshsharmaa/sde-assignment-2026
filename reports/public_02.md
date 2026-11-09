# Grade report — public_02_incidents

## Outcomes vs baseline (no-op controller)

| workflow | conclusion rate | Δ | pickup | Δ | engagement | Δ | engagement gap captured | hallucinated calls (base) |
|---|---|---|---|---|---|---|---|---|
| ks_cancel | 0.262 | +6.0% | 0.180 | -1.9% | 0.512 | +7.6% | 40% | 56 (330) |
| nw_kyc | 0.477 | +27.2% | 0.302 | +24.6% | 0.594 | +0.2% | 0% | 41 (31) |
| nw_renewal | 0.436 | +2.2% | 0.265 | +1.2% | 0.553 | +0.8% | 5% | 65 (55) |
| ALL | 0.366 | +9.4% | 0.233 | +4.9% | 0.548 | +3.5% | 17% | 162 (416) |

## Changes applied by the controller

6 applied — neutral: 1, beneficial: 4, harmful: 1

- nw_renewal v2 (controller_approved) dims=['welcome_message'] engageΔ=+0.000 didΔ=+0.000 → **neutral**
- ks_cancel v2 (controller_approved) dims=['welcome_message'] engageΔ=+0.023 didΔ=+0.000 → **beneficial**
- nw_kyc v2 (controller_approved) dims=['caller_id'] engageΔ=+0.000 didΔ=+0.212 → **beneficial**
- nw_kyc v3 (controller_approved) dims=['caller_id'] engageΔ=+0.000 didΔ=+0.180 → **beneficial**
- nw_kyc v4 (controller_approved) dims=['caller_id'] engageΔ=+0.000 didΔ=-0.032 → **harmful**
- ks_cancel v4 (controller_approved) dims=['prompt_version'] engageΔ=+0.102 didΔ=+0.000 → **beneficial**

## Governance

```
{
  "approval_requests": 7,
  "decisions": {
    "approved": 6,
    "rejected": 1,
    "expired": 0
  },
  "rejection_reasons": {
    "approver declined (judgement call)": 1
  },
  "applied_via_approval": 6,
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
[
  {
    "type": "did_flagged",
    "workflow": "ks_cancel",
    "at": "D2 10:00",
    "flagged_did_share_by_day": {
      "2": 0.491,
      "3": 0.511,
      "4": 0.504,
      "5": 0.498,
      "6": 0.497,
      "7": 0.502,
      "8": 0.507,
      "9": 0.496
    }
  },
  {
    "type": "supervisor_edit",
    "workflow": "ks_cancel",
    "at": "D3 11:30",
    "mttr_minutes": 270,
    "fixed_by": "controller_approved"
  },
  {
    "type": "supervisor_edit",
    "workflow": "nw_renewal",
    "at": "D5 15:00"
  }
]
```

## Decaying variants — share of in-population calls still on the variant, by day after onset

```
{}
```

Credits used: 11 · validation runs: 2 · LLM fix calls: 1