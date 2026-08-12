---
name: all60m-variant-onboarding
description: "PROD onboarding (2026-08-12) of the payment_auth_sr all|60m_5m variant + its alert — IDs, and the runtime follow-up needed (repoint deployed rule-eval bootstrap.jobId) for it to actually fire."
metadata: 
  node_type: memory
  type: project
  originSessionId: d7fd3180-ade6-4ec6-b4eb-b8c5dfd59cf8
---

Onboarded via the ada-onboard flow on **prod** (`ADA_ENV=prod` →
`https://ada-dsl-dev.de.razorpay.com`, backed by prod DB). Metric
`payment_auth_sr` = `mtr_TDtXDzWsNyTC2F` (dataset payments, MID).

New variant **`all|60m_5m_sliding_lookback`** = `mvar_TOuaLJenhxHqLr` (no dimension,
`dimension_fields:[]`, window SLIDING_WITH_LOOKBACK size 3600000/slide 300000/lookback 300000).
Activated → mapped to metrics job `metrics-payments-sliding-60m-5m` (verified in metrics:bootstrap).

Alert side (reused existing rule, no duplicate):
- rule `TEHYiV3k39J1sg` (`payment_auth_sr <= _sr_threshold AND total_payments >= _min_volume`)
- cohort `TOunl6c4ZqOO5c` (`payment_auth_sr_all_60m_5m_sliding_lookback`, EMPTY — attach merchants later via alert-onboard)
- binding `TOunlfYIyrQCX4` (dimension `{}`, P1 `_sr_threshold 0.6 / _min_volume 30`)
- variant→rule-eval mapping `TOunm8qDvvfOyo` → job `rule-eval-payments-sr` (all 5 variants map there)

**RUNTIME FOLLOW-UP (not yet done):** for the 60m alert to actually fire, the deployed
rule-eval Flink job must bootstrap against `rule-eval-payments-sr` (repoint
`bootstrap.bootstrap.jobId` in the deploy YAML from `metrics-payments-sliding-15m-5m`).
Reason: bootstrap jobId `metrics-payments-sliding-15m-5m` resolves 22 bindings (legacy
fallback `job_metric_variant_mappings`); jobId `rule-eval-payments-sr` resolves 37
(explicit `variant_rule_eval_job_mappings`, incl. both 60m variants). Delta validated:
22 ⊂ 37, 0 dropped, +15 (method_type|60m ×14, all|60m ×1). Flag
`ada.variant-rule-eval-mapping.enabled` is ON in prod. See [[rule_eval_job_requirement]].
Both metrics jobs sink to the same topic `ada_window_metrics_payments`, so no source change needed.
