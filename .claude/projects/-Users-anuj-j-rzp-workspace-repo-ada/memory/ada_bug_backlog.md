---
name: ada_bug_backlog
description: "RUNNING BUG BACKLOG for ADA — append every bug found in any session here (with context/root-cause/fix), to be fixed later. Check here before re-investigating."
metadata: 
  node_type: memory
  type: project
  originSessionId: 81db510a-f685-4076-aeef-5955e6cd95d1
---

# ADA Bug Backlog (append-only)

**Instruction to future sessions:** whenever a bug is found, APPEND a new entry below with:
title, status, symptom, context, root cause, evidence, proposed fix. Do NOT delete entries;
mark them `FIXED (date)` when resolved. Newest at top. Env note: ada-dsl "dev" API/namespace
is backed by the PROD DB — see [[ada_dsl_dev_ns_points_to_prod_db]].

## BUG-010 — `recordType: RULE_EVAL` silently overrides `payloadType: DELIVERY` on a KAFKA_SINK
- **Status:** OPEN (config footgun + optional code guard). Found 2026-08-12.
- **Symptom:** `ada_test` topic (fed by `beacon-sink`) carries `_rca_events_compressed` even though
  `beacon-sink` sets `payloadType: "DELIVERY"` — the compressed blob should never be in a delivery payload.
- **Context:** `beacon-sink` has BOTH `recordType: "RULE_EVAL"` and `payloadType: "DELIVERY"` (+`payloadVersion: v2`).
- **Root cause:** `SinkBuilder.buildKafkaSinkOperator` (`:76`) checks `isRuleEvalRecordType` FIRST and
  returns early (maps via `RuleEvalRecord.fromPayload` = raw canonical + key=incident_key + headers), so
  it NEVER reaches `KafkaSinkOperator` which applies the `DELIVERY` transform (`DeliveryPayloadBuilder`,
  the code that strips the blob). `payloadType`/`payloadVersion` on that sink are dead config.
- **Evidence:** JM startup log shows `beacon-sink` and `rca-incidents-sink` with identical
  `rule-eval mode — key=incident_key, headers=[…]`; captured `ada_test` msg has the blob. See [[delivery_payload_shaping]].
- **Webhook is unaffected:** WEBHOOK_SINK has no RULE_EVAL branch — always runs the delivery transform
  (verified: v4, blob-stripped, HTTP 200).
- **Fix:** remove `recordType: "RULE_EVAL"` from `beacon-sink` (keep `payloadType: DELIVERY`) → falls
  through to the delivery transform. Trade-off: loses incident_key key + rule-eval headers on `ada_test`.
  Optional code guard: warn/error when a sink sets both `recordType: RULE_EVAL` and `payloadType`.

---

## BUG-009 — beacon-webhook-sink filter defaults to logic=AND → drops ALL detections (downtime-manager never gets detections)
- **Status:** OPEN (config). Found 2026-08-12 while checking if the webhook endpoint returns 200 on the
  prod rule-eval job `ada-rule-evaluator-payments-sliding-15m-5m` (job 2b4de524…, ns zmfmgee2e8g6jru8).
- **Symptom:** webhook to `downtime-manager-adatest.dev.razorpay.in/webhooks/ada/anomalies/v3` has made
  ZERO POSTs since restart — no `[TRACE][BeaconSink]` lines anywhere across JM+4 TMs — despite 4 detections
  emitted. So no HTTP status (200/4xx/5xx) is observable because nothing was sent.
- **Root cause:** `beacon-webhook-sink` config omits `logic:`, so it defaults to **AND** (operator logged
  `Applied 2 filter rule(s) (logic=AND)`). Its two filters are `_suppression.suppressed NE true` AND
  `alert_type NE detection`. With AND, any detection fails `alert_type NE detection` → dropped. So the
  webhook forwards ONLY non-suppressed resolutions; downtime-manager never receives a detection (can't open
  a downtime). The sibling `slack-alerts-sink` has the SAME two filters but explicitly sets `logic: "OR"`
  ("suppress only stale detections; resolutions always pass") — the webhook block just forgot it.
- **Evidence:** 5 pods, full history: 0 `[TRACE][BeaconSink]`; alert_type since restart = detection×4, no
  resolution; operator log `logic=AND`; yaml `beacon-webhook-sink` has `filters:` but no `logic:`.
- **Proposed fix:** add `logic: "OR"` to `beacon-webhook-sink.config` (mirror slack) so non-suppressed
  detections + resolutions both POST. Deployed via `/Users/anuj.j/Downloads/rule-evaluator-payments-sliding-15m-5m.yaml`.

## BUG-008 — rule-eval stage job checkpoints to LOCAL /tmp → crash-loop + lost incident state on pod reschedule
- **Status:** OPEN (config). Found 2026-08-12 during live resolution testing.
- **Symptom:** `ada-rule-evaluator-payments-sliding-15m-stage` pod got rescheduled to a new node; new
  deployment crash-looped (CrashLoopBackOff, exit 1) at JobMaster start with
  `java.io.FileNotFoundException: Cannot find checkpoint or savepoint file/directory
  'file:/tmp/flink-checkpoints/ada-rule-evaluator-payments-sliding-15m-stage/<oldJobId>/chk-263' on
  file system 'file'`. Job never started consuming ada-alerts until redeployed clean.
- **Root cause:** stage config checkpoints to a LOCAL path (`state.backend.rocksdb.localdir:
  /tmp/flink/rocksdb`; checkpointDirectory resolves to `file:/tmp/flink-checkpoints/...`). On a pod
  move the new pod can't see the old pod's local `/tmp` checkpoint → recovery fails → crash-loop.
  The incident-lifecycle ActiveIncidentState (open incidents) lives in that keyed state, so a
  reschedule also LOSES all open incidents.
- **Proposed fix:** point `execution.checkpointing.dir` (and savepoint dir) at durable S3
  (`s3i://ververica-byoc-cluster-v2-byoc-.../checkpoints/...`), matching how VVP already stores
  HA/checkpoint metadata for the deployment. Then state survives pod moves and last-state recovery works.
- **Note:** unrelated to the payload-v4 code / 2.1.4 jar — pure infra/config. Metrics job was unaffected.

## RESOLVED-NOTE — v4 resolution + all 6 contract changes validated LIVE on stage 2026-08-12
- The v4 payload-generalization contract-alignment (branch `feat/ada-alert-payload-v4`, commits
  4e306526 + 0d22dfd9, jar `ada-flink-runtime-2.1.4-stage.jar`) is LIVE-validated end-to-end on stage:
  detection + resolution both fire and conform. The 6 changes: (1) `_suppression` default
  `{suppressed:false,types:[],details:[]}` in AlertPayloadV4Assembler; (2) drop top-level
  `business_dba`+`rca_timestamp`/`_ist` (Phase1RcaRuntimeAdapter); (3) slim delivery
  `rca_analysis.event_level_analysis` to 5 keys (DeliveryPayloadBuilder v4 path only, canonical kept
  full); (4) remove `incident_hash` (v4 assembler only); (5) `external_sources` always `[]`
  (RcaResult); (6) drop `rca_analysis.timestamp_ist` (keep numeric `timestamp`).
- **Live-test recipe (zero DB mutation):** cohort `TB16jiYOESYgDn` member `MERCH_0020_3V6RF63A` has NO
  override → uses correct base rule (P1 `_sr_threshold:0.2`/`_min_volume:50`), unlike `BumijLSrta0YAj`
  whose override `TBPNFvrchS8XIJ` still has the buggy `_sr_threshold:80`/`_min_volume:100` (BUG-001).
  With a fractional threshold a clean recovery is reachable (sr=0.9 > 0.2 → matched=false,
  low_volume_breach=false → resolves; buggy 80 makes every non-trigger a low_volume_breach → never
  resolves). Direct-inject two `metric_aggregate` records to `ada-alerts` with explicit increasing
  `window_end` (detection sr=0.1 then recovery sr=0.9) via local script
  `scripts/MetricAggregateResolutionProducer.java` (uncommitted); bypasses flaky metrics
  sliding-window watermark timing (raw future-dated events get dropped as late).

## BUG-007 — payment_auth_sr `successful_payments` measure keys on status=="authorized" ONLY; dedup keeps "captured" first → real-traffic SR undercount
- **Status:** OPEN (config/design). Found 2026-08-11 during stage end-to-end raw→agg→eval test.
- **Symptom:** injected 120 raw UPI-Intent events (12 with status=captured & authorized_at>0, 108 failed);
  window measure returned `successful_payments:0, payment_auth_sr:0.0` while the RCA engine on the SAME
  events returned `total_successful_transactions:12` (sr 0.1). Two engines, two success definitions.
- **Root cause:** deployed metric bootstrap (`metrics-payments-sliding-15m` → metric TAzis6W3EvheMy /
  variant TAzis6fyDYWx6B) defines `successful_payments = COUNT_DISTINCT(id) WHERE status EQ ["authorized"]`
  (single exact value), `failed_payments = ... status EQ ["failed"]`, `payment_auth_sr =
  successful_payments / NULLIF(total_payments,0)`. But the WINDOW_AGGREGATOR dedups per id with
  `statusPriorityOrder=[captured, refunded, failed, authorized, authenticated, created, pending]`
  (STATUS_THEN_TIMESTAMP) → a payment that reaches **captured** (higher priority than authorized) is
  deduped to captured and then **not** matched by `status==authorized` → counted as neither success nor
  failure. In real UPI traffic most successes terminate as `captured`, so this measure would UNDERCOUNT
  successes / depress SR. Meanwhile RCA (rule-eval config) uses `authorized_at > 0`
  (`success_criteria_expression: SUM(CASE WHEN authorized_at>0 THEN 1 ELSE 0 END)`) — a DIFFERENT, more
  correct success definition. The two disagree.
- **Evidence:** clean stage run with status=="authorized" successes → measure correctly gave
  `successful:12, failed:108, sr:0.1`; with status=="captured" successes → `successful:0, sr:0.0`. RCA gave
  12 successes in both. Measure def dumped from `.../metrics-payments-sliding-15m/metrics:bootstrap`.
- **Proposed fix:** change `successful_payments` criteria to `status IN [authorized, captured]` (and/or
  `refunded`), OR switch the measure to `authorized_at > 0` to match RCA. Align metric success semantics
  with RCA success_criteria so aggregate SR and RCA agree. Also stale-memory note: prior
  [[ada-reconcile-skill-design]] claimed successful = authorized_at>0 — that's the RCA criterion, NOT the
  deployed metric measure. Relates to [[ada_threshold_percentage_bug]] (BUG-001: this variant's override
  still stores `_sr_threshold:80`, `_min_volume:100` — % not fraction).

## BUG-006 — v4 feature branch (`feat/ada-alert-payload-v4`) has 9 pre-existing failing tests
- **Status:** OPEN. Found 2026-08-10. NOT caused by the session's changes (verified: identical
  failures on clean HEAD with changes stashed).
- **Symptom:** `mvn test` on `feat/ada-alert-payload-v4` fails 8 in ada-flink-runtime + 1 in ada-flink-operators.
- **Failures by category:**
  - Slack v4 rendering (5): `SlackVisualMessageComposerTest` (2 — e.g. expects "ML Prophet Alert", got
    "ML Alert"), `SlackMetricAggregateVariantsRenderingTest` (2), `PayoutSrRcaSlackRenderingTest`,
    `SlackPresentationProfileTest.visualFormat_usesConfiguredPayoutProfile`.
  - Avro (1): `ActiveIncidentStateAvroSerializationTest.serialize_shouldAllowNullOptionalFields` — NPE
    "null in string in field alertCategory" (schema made alertCategory non-nullable).
  - Incident lifecycle (1): `IncidentLifecycleOperatorTest.matchedFalse_newerWindow_shouldEmitResolutionAndClearState`
    (expected true, got false).
  - Entity cohort (1): `EntityGroupBroadcastPipelineTest.stage1_missingCohortGracefullyHandled`
    (missing cohort → 126 members, expected 0).
- **Root cause:** not individually root-caused; hypothesis = v4 migration WIP gaps (renderers/Avro/lifecycle
  read v3-era fields). Needs per-test triage before a clean full build / deploy.
- **Proposed fix:** triage each category; likely the v4 payload field relocations (dataset_id, top-level
  entity, resolution.*) aren't yet reflected in Slack renderers + ActiveIncidentState Avro schema.

## BUG-005 — v4 `_suppression` contract mismatch: readers expect legacy `status`, v4 writes `suppressed`
- **Status:** PARTIALLY FIXED 2026-08-10 (commit c84e791 on `feat/ada-alert-payload-v4`). Config + one
  reader still OPEN.
- **Symptom:** Under v4, sink `suppressWhenSuppressed` and the Slack `_suppression.status NE suppressed`
  filter never suppress → suppressed alerts still delivered to beacon/kafka/slack.
- **Root cause:** legacy `SuppressionTagger` writes `_suppression.status`=allowed/suppressed; the v4
  `StrategySuppressionTagger` (StrategySuppressionTagger.java:131) writes `_suppression.suppressed`
  (boolean, + types/details) and NO `status`. Readers were status-based:
  `SinkDeliveryFilter.isSuppressed()` (status ==), Slack `AnomalyFilter` config (`_suppression.status NE
  suppressed` — absent field + NE → passes, AnomalyFilter.java:98-104), and
  `IncidentLifecycleOperator` (reads `_suppression.reason == suppression_window_active`, v4 has no reason).
- **Fixed:** `SinkDeliveryFilter.isSuppressed()` now reads `_suppression.suppressed` boolean first, legacy
  `status` fallback (covers beacon/kafka/slack `suppressWhenSuppressed`).
- **(a) FIXED 2026-08-10 (config):** stage slack sink switched from `filters: _suppression.status NE
  suppressed` to `suppressWhenSuppressed: true`, which routes through the v4-aware SinkDeliveryFilter
  (reads `suppressed` boolean, legacy `status` fallback). Prod slack configs on v3 must NOT change until
  they go v4 (v3 uses `status`). SinkDeliveryFilter fix covers beacon/kafka/slack `suppressWhenSuppressed`.
- **(b) WON'T-FIX (not a current bug):** v4 has NO maintenance-suppression-window plumbing —
  `_suppression_window` is stamped only in the legacy v3 path (MetricAggregateAlertPayloadBuilder:93),
  NOT in `buildV4`; StrategySuppressionTagger has only a STALE_WINDOW strategy, no maintenance-window one.
  So `IncidentLifecycleOperator`'s `_suppression.reason == suppression_window_active` guard is dormant
  under v4 (never fires), not misbehaving. Nothing correct to point it at until the v4 suppression-window
  feature is built. Revisit when v4 gains scheduled-window suppression.
- **Note:** none of this bites the stage config `rule-evaluator-payments-sliding-15m-stage` at runtime —
  that pipeline has no suppression tagger, so `_suppression` is unstamped and nothing is suppressed anyway.
  The (a) config change is future-proofing for when a tagger is added.

## BUG-004 — Slack threading breaks for incidents that span a channel switch / last-state restore
- **Status:** OPEN (code fix pending). Found 2026-08-06.
- **Symptom:** In `#ada-v2-alerts` (C0BN2JC0ULB), the no-dimension "PAYMENT_SR alerts - <mid>" (`_global_`)
  alerts post a NEW top-level Slack message every 15-min window instead of threading. Method-dimensioned
  incidents thread fine (e.g. "UPI Intent - initial" = 10 replies).
- **Context:** rule-eval job `ada-rule-evaluator-payments-sliding-15m-5m` (deploy id bd46299c…, ns
  zmfmgee2e8g6jru8), jar `ada-flink-runtime-2.1.3`. Channel C0BN2JC0ULB was created ~2026-08-05 22:08;
  the stuck incident's root is `ada-b62346609efa-20260805051500` (Aug 5, before the channel existed).
- **Root cause:** job restarted with `last-state`, so `SlackAlertSinkFunction.slackTsByAnomalyId` was
  restored from the old job. For incidents rooted before the channel switch, the stored `thread_ts`
  points to a message in the OLD channel. `chat.postMessage(channel=C0BN2JC0ULB, thread_ts=<old-channel ts>)`
  can't find that parent → Slack posts top-level and returns a ts → Flink logs `threaded=true` but it
  never actually threaded. `rememberThreadTs` uses `putIfAbsent` (SlackAlertSinkFunction.java:780) so the
  stale ts is never refreshed → permanent. Not really "no-dimension"-specific; it's "root predates channel
  switch". Incidents rooted after deploy thread fine.
- **Evidence:** Flink `[TRACE][SlackSink]` shows `threaded=true` every window on subtask (3/4) with stable
  root b62…051500; Slack channel shows each window as separate top-level. Channel history starts Aug 6 07:45.
- **Proposed fix:** (A operational) restart sink with clean thread state (change sink operator uid, or
  stateless) so it re-roots in the current channel. (B durable) key thread map by (channel, root_anomaly_id);
  after chat.postMessage, inspect response thread_ts/ts and if it didn't actually thread, OVERWRITE the
  stored ts (replace putIfAbsent with put) so it self-heals on channel change/stale parent.

## BUG-003 — Runtime rule NAME shows per-merchant alertconfig name instead of canonical rule name
- **Status:** FIX EXISTS (#103), NOT DEPLOYED. Found 2026-08-06.
- **Symptom:** Slack alert shows `Rule: PAYMENT_SR alerts - 6q0BL9DgjgHdIv (TEHYiV3k39J1sg)` /
  `Payment Auth Success Rate Drop - UPI Intent - NiYEswJ1TSn8q0` — the per-alertconfig name, not the
  clean rule name.
- **Context:** The rule name is served by ada-dsl's runtime-rules bootstrap projector, not Flink. It's
  data the Flink job renders as-is.
- **Root cause:** commit `#103` (66e1a1d7) changed `RuntimeAlertRuleProjector` from
  `firstNonBlank(alertConfig.getName(), rule.getName())` to `rule.getName()`. But the DEPLOYED ada-dsl
  service is stale (~8 days old, predates #103), so it still emits the alertconfig name. Live bootstrap
  shows a MIX (bindings w/ alertconfig → per-merchant name; bindings w/o → clean name), which is the old
  behavior.
- **Fix:** redeploy ada-dsl (with #103), then restart/re-bootstrap the rule-eval Flink job so it re-reads
  rule names. Flink jar version is irrelevant here.

## BUG-002 — `N/A` in Slack header for no-method-dimension alerts (cosmetic)
- **Status:** OPEN (small Flink template change). Found 2026-08-06.
- **Symptom:** Header renders `N/A Testbook | 6q0BL9DgjgHdIv | N/A` for `_global_`/all-variant alerts.
- **Context:** slack-alerts-sink `subjectTemplate = {window_payload.business_dba} | {window_payload.merchant_id} | {impacted_dimension.method_type}`.
  For the all-variant (TDtXDztbWN25ew, no method dimension) `impacted_dimension.method_type` is absent → `N/A`.
- **Root cause:** template hard-codes `method_type`, which doesn't exist for no-dimension configs. Incident/
  threading logic underneath is fine (see BUG-004 for the real threading issue).
- **Proposed fix:** graceful fallback when no method dimension — show variant label or drop the segment.
  (Also: "job name mismatch" header vs SUMMARY is NOT a bug — header=evaluator job, SUMMARY=source metrics job.)

## BUG-001 — SR threshold stored as percentage (35) not fraction (0.35); no range validation on write
- **Status:** DATA FIXED 2026-08-06; CODE FIX PENDING. See [[ada_threshold_percentage_bug]].
- **Symptom:** `_sr_threshold` stored as 15/25/35/40/… (percentage) in bindings/overrides/alert_configs;
  runtime `SR <= 35` always true → constant/spurious alerts.
- **Context:** 3 tables in prod_ada_dsl: `alert_rule_binding_overrides.thresholds_json`,
  `alert_rule_bindings.thresholds_json`, `alert_configs.thresholds`.
- **Root cause:** SR-range guard `0<sr<1` lives ONLY in `AlertRuleCriteriaValidator` (rule-create path,
  used only by AlertRuleService). Binding/override/`POST /v1/alerts` write paths route through
  `ThresholdConfigNormalizer`/`ThresholdsJson`, which validate severity+numeric but do NO range check and
  NO %→fraction conversion → verbatim store.
- **Done:** DB backfilled (÷100, 2-dp) across all 3 tables (1716+5+1639 rows), verified.
- **Proposed code fix:** add `0<value<1` validation for `*sr*`/`*rate*` placeholders (and integer≥1 for
  volume) in `ThresholdConfigNormalizer.normalizePlaceholders()` so all write paths inherit it.
