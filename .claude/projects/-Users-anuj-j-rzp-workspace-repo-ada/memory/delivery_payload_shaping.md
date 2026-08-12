---
name: delivery-payload-shaping
description: "How ADA rule-eval sinks shape the outgoing payload — DeliveryPayloadBuilder V1/V2, the v4 delivery slim (blob strip), and the recordType=RULE_EVAL precedence that bypasses payloadType. Explains which stream carries _rca_events_compressed."
metadata: 
  node_type: memory
  type: reference
  originSessionId: d7fd3180-ade6-4ec6-b4eb-b8c5dfd59cf8
---

Two independent axes decide what a rule-eval sink emits. Confusing them causes
"why is the payload v2 / why does it still have the compressed blob" questions.

**Axis 1 — `schema_version` (the canonical):** produced by the rule-evaluator.
`payload.v4.threshold: true` in the RULE_EVALUATOR config → `schema_version:4`
(via `AlertPayloadV4Assembler`); without it → `schema_version:3`. Stage and prod
both had v4 ON as of 2026-08-12.

**Axis 2 — delivery shaping in `DeliveryPayloadBuilder`** (`ada-core/.../delivery/DeliveryPayloadBuilder.java`),
selected by the sink's `payloadVersion` config. The enum is only `V1`/`V2`;
`PayloadVersion.fromConfig` maps `"v2"`→V2, and **everything else (null/absent/"v1")→V1**.
- **V1 + v4 canonical** → v4 fast-path (`:94`, `isV4 && payloadVersion != V2`): deep-copy
  the v4 payload, **`remove("_rca_events_compressed")` + `remove("_rca_metadata")`** at
  top-level AND inside `window_payload`, then `slimV4DeliveryRca()` trims
  `rca_analysis.event_level_analysis` to 5 keys. Result = **v4 delivery, blob-stripped**
  (~8.5 KB). This is the current webhook output.
- **V2** → legacy beacon downgrade: fresh map, `by_<dim>`/`by_merchant_id` group, renamed
  measures (`total_payments→total`, `successful_payments→success`, `payment_auth_sr→success_rate`).
  Never copies the blob either.
- **CANONICAL** (payloadType != DELIVERY) → `deepCopyMap(canonical)` → **keeps `_rca_events_compressed`**.

So **only a CANONICAL emission carries the compressed events blob.** Both delivery
versions strip it.

**THE GOTCHA — `recordType: RULE_EVAL` overrides `payloadType: DELIVERY`** on a
KAFKA_SINK. `SinkBuilder.buildKafkaSinkOperator` (`:76`) checks `isRuleEvalRecordType`
FIRST and returns early — it maps via `RuleEvalRecord.fromPayload` (keeps the raw
canonical incl. blob) and `createRuleEvalProducer` (key=`incident_key`, headers=
`[alert_entity,alert_category,rule_id,metric_id,variant_id]`), **never reaching the
`KafkaSinkOperator` delivery transform**. So a sink with BOTH keys emits canonical
(with blob); `payloadType`/`payloadVersion` are dead config. See [[ada_bug_backlog]] BUG-010.

**Per-sink behavior in the payments rule-eval job (2026-08-12):**
- `rca-incidents-sink` → `ada_rule_eval_payments`, `recordType: RULE_EVAL` → canonical + blob (correct, feeds lakehouse).
- `beacon-sink` → `ada_test`, has BOTH `recordType: RULE_EVAL` and `payloadType: DELIVERY` → RULE_EVAL wins → **canonical + blob** (the bug; delivery config ignored).
- `beacon-webhook-sink` (WEBHOOK_SINK) → no RULE_EVAL branch exists for webhooks; `WebhookSinkOperator` ALWAYS runs the delivery transform → **v4 delivery, blob-stripped, HTTP 200** (correct). No `payloadVersion` set → V1 → v4.

Verify the actual deployed sink wiring from the JobManager startup log
(`Building KAFKA_SINK operator: <name>` + `rule-eval mode — key=incident_key…`),
NOT the repo/Downloads YAML (deployed configs drift from the files).
