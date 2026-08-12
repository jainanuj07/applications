---
name: ada-infra-access-map
description: "Where the ADA jobs and DBs actually run — kube contexts/namespaces/pods for prod & stage rule-eval (Ververica BYOC), stage flink, and the ada-dsl-dev (prod DB) pod. Plus how to run raw SQL from the ada-dsl pod (no mysql client → jshell)."
metadata: 
  node_type: memory
  type: reference
  originSessionId: d7fd3180-ade6-4ec6-b4eb-b8c5dfd59cf8
---

Rediscovering these each session is painful. Kube contexts often need the FULL ARN
(short aliases like `ververica-byoc-cluster-v2` may not resolve; `kubectl config
get-contexts -o name` shows the real names). `kubectl get ns` frequently returns
empty on BYOC clusters (namespace-scoped access) — you must know the ns.

**Prod rule-eval (payments)** — Ververica BYOC:
- context: `arn:aws:eks:ap-south-1:899071933356:cluster/ververica-byoc-cluster-v2`
- namespace: `zmfmgee2e8g6jru8`
- pods: `job-<jobId>-<hash>-<pod>` (JobManager) + `job-<jobId>-taskmanager-1-N`.
  Webhook sink is parallelism 1 → on one TM. `[TRACE][BeaconSink]` logs at WARN;
  enable raw payload via `logRawEvents: true` on the sink.
- JM startup log prints deployed sink wiring (`Building KAFKA_SINK operator: <name>`,
  `rule-eval mode — key=incident_key…`) — authoritative over repo/Downloads YAML.

**Stage flink** — context `stage-white-eks`, namespace `flink`:
- `ada-payments-producer-*` pod = kafka produce/consume tool (confluent_kafka + certs
  at /opt/certs; use `enable.ssl.certificate.verification=False`). Broker
  `stage-kafka.razorpay.in:9090`. See [[stage_rule_eval_injection_harness]].
- stage metrics job `ada-metrics-payments-sliding-15m-stage`; the stage rule-eval
  itself runs on Ververica BYOC (job id given by user, e.g. 4eeef4aa-…).
- stage ada-dsl bootstrap API: `https://ada-dsl.int.stage.razorpay.in` (in-cluster
  only; not reachable from laptop). Reads are tokenless-capable.

**ada-dsl-dev pod (backed by PROD Aurora `prod_ada_dsl`)** — see [[ada_dsl_dev_ns_points_to_prod_db]]:
- context `prod-de-white-eks`, namespace `ada-dsl-dev`, pod `ada-dsl-dev-*`.
- DB creds are pod env: `ADA_DSL_WRITER_DB_URL/PORT/NAME/USERNAME/PASSWORD`
  (host `prod-aurora-mysql-ada-dsl.db.de.razorpay.vpc:3306`, db `prod_ada_dsl`).
  JDBC url = `jdbc:mysql://$URL:$PORT/$NAME?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC`.
- **No `mysql` client in the pod.** To run raw SQL: extract the driver from the app jar
  (`cd /tmp && jar xf /app/app.jar BOOT-INF/lib` → `mysql-connector-j-*.jar`) and use
  `jshell --class-path <driver.jar> script.jsh`, reading creds via `System.getenv(...)`
  (keeps secrets out of the transcript). Prefer the API for mutations when it exists.

**Prod ada-dsl API** (for onboarding writes) = `https://ada-dsl-dev.de.razorpay.com`
(the "dev" host is PROD data); token via env var only (never a pod) — see
[[ada_dsl_dev_ns_points_to_prod_db]].
