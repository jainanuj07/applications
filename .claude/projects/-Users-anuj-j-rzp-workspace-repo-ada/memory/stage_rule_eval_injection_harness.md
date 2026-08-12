---
name: stage-rule-eval-injection-harness
description: "How to trigger a stage ADA rule-eval detection on demand — produce a metric_aggregate record to stage topic ada-alerts from the ada-payments-producer pod, then read the v4 output on ada-anomalies. Includes the input schema, SSL params, and a real target binding."
metadata: 
  node_type: memory
  type: reference
  originSessionId: d7fd3180-ade6-4ec6-b4eb-b8c5dfd59cf8
---

To reproduce a detection (and inspect the generated payload) in **stage** without
waiting for live traffic: inject a `metric_aggregate` record onto the stage
`ada-alerts` topic (the rule-eval's source), then read the output.

**Topology:** stage metrics job `metrics-payments-sliding-15m` → `ada-alerts` →
stage rule-eval job (Ververica BYOC) → output on **`ada-anomalies`** (schema_version:4,
CANONICAL). NOTE: the deployed stage job writes to `ada-anomalies`, NOT `ada-test`
as the repo file `configs/ververica/rule-evaluator-payments-sliding-15m-stage.yaml`
claims — deployed configs drift from the repo.

**Run from pod** `ada-payments-producer-*` in `--context stage-white-eks -n flink`
(has `confluent_kafka`, python3, SSL certs at `/opt/certs/`). Kafka client config:
```
bootstrap.servers=stage-kafka.razorpay.in:9090, security.protocol=SSL,
ssl.ca.location=/opt/certs/ca.crt, ssl.certificate.location=/opt/certs/user.crt,
ssl.key.location=/opt/certs/user.key, enable.ssl.certificate.verification=False   # <- required
```

**Input record schema** (from `feat/ada-alert-payload-v4:ada-flink-runtime/local-config/metric-aggregate-runtime-e2e-live-inputs-20260701.jsonl`):
top-level `schema_version:1, payload_type:"metric_aggregate", event_id, job_id,
metric_id, variant_id, level, entity_field, entity_value, metric_metadata{metric_key,
variant_key, dimension_fields}, stats{record_count}, window{...}, properties, aggregations[], emitted_at`.
Set window `start/end` to NOW-900000/NOW (recent, else STALE_WINDOW suppression drops it).

**To MATCH + trigger:** the record's `variant_id`/`entity_value` must map to a real
binding+cohort the job bootstrapped (from `ada-dsl.int.stage.razorpay.in`, jobId
`metrics-payments-sliding-15m`). Working stage target (2026-08-12):
metric `TAzis6W3EvheMy`, variant `TAzis6fyDYWx6B` (`method_type|15m_5m_sliding_lookback`),
rule `TB10Yo6JJIB01Q`. Binding for `method_type=UPI Intent` = `TB1B2pm2xiaG73`,
cohort `TB16jiYOESYgDn`, members incl. `MERCH_0020_3V6RF63A` (no override → default P1
`_sr_threshold 0.2 / _min_volume 50`). A row `{"method_type":"UPI Intent","total_payments":100,
"successful_payments":8,"inflight_payments":0,"payment_auth_sr":0.08}` breaches P1.

**Observe:** the rule-eval CG `ada-rule-evaluator-payments-sliding-15m-stage-cg`
consumes ada-alerts (check `committed==end` = consumed); the detection appears on
`ada-anomalies` (filter by your entity_value). Query the stage bootstrap
(rule-bindings/entity-cohorts/metrics :bootstrap) from the same pod via urllib with
`ssl.CERT_NONE`. See [[delivery_payload_shaping]] and [[ada_infra_access_map]].
