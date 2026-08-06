#!/usr/bin/env bash
# CHIEF-ARCHITECT three-mode E2E on kind + MinIO (S3 API) — proves Zelox is ONE engine covering all three
# comparison modes (docs/design/engine-comparison-mode-mapping.md, REFERENCES §0), each on real pods + real
# object store, FREE, before any EKS spend:
#   MODE 1 BATCH               (↔ Spark batch)               : write→read→agg to MinIO, rows==N + sum exact
#   MODE 2 STRUCTURED STREAMING(↔ Spark Structured Streaming): availableNow windowed COUNT→MinIO, sum==N
#   MODE 3 REALTIME            (↔ Flink)                      : .trigger(realTime) pass-through→MinIO, EO dup=0 across a hard pod-kill
# Assumes: kind up with zelox:$TAG loaded, Kafka+MinIO in ns stream. Usage: TAG=p2main N=2000000 scripts/kind_three_mode_e2e.sh
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
CTX="${CTX:-kind-zelox-kind}"; NS=stream; TAG="${TAG:-p2main}"; N="${N:-2000000}"; KEYS="${KEYS:-1000}"
MINIO_EP="http://minio.$NS.svc.cluster.local:9000"
kk() { kubectl --context "$CTX" -n "$NS" "$@"; }
S3ENV="AWS_REGION=us-east-1 AWS_ENDPOINT=$MINIO_EP AWS_ENDPOINT_URL=$MINIO_EP AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_ALLOW_HTTP=true"
scale_kind() { sed -E -e 's/cpu: "1[0-9]"/cpu: "1"/g' -e 's/cpu: "[3-9]"/cpu: "1"/g' -e 's/memory: "2[0-9]Gi"/memory: "2Gi"/g' -e 's/memory: "1[0-9]Gi"/memory: "1500Mi"/g'; }
SR="sc://zelox-stream.$NS.svc.cluster.local:50051"; BOOT="kafka.$NS.svc.cluster.local:9092"
R1=""; R2=""; R3=""

echo "==== [deploy] zelox-stream (single-node, MinIO S3) + client ===="
sed -E -e "s#__ECR__/zelox:[A-Za-z0-9._-]+#zelox:$TAG#g" -e 's/imagePullPolicy: Always/imagePullPolicy: IfNotPresent/g' k8s/stream/zelox-stream.yaml | scale_kind | kk apply -f - >/dev/null
kk patch deploy zelox-stream --type=json -p='[{"op":"add","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"Never"}]' >/dev/null 2>&1
kk patch deploy zelox-stream --type merge -p '{"spec":{"strategy":{"rollingUpdate":{"maxSurge":0,"maxUnavailable":1}}}}' >/dev/null 2>&1
kk set env deploy/zelox-stream $S3ENV >/dev/null
kk wait --for=condition=available --timeout=240s deployment/zelox-stream >/dev/null || { echo "STREAM DEPLOY FAIL"; exit 1; }
scale_kind < k8s/stream/zelox-client.yaml | kk apply -f - >/dev/null; kk wait --for=condition=ready --timeout=200s pod/zelox-client >/dev/null
until kk logs zelox-client 2>/dev/null | grep -q CLIENT_READY; do sleep 3; done
kk cp scripts/batch_s3_bench.py zelox-client:/tmp/batch.py; kk cp scripts/stream_windowed_agg.py zelox-client:/tmp/wagg.py; echo "ready"

echo ""; echo "############ MODE 1 — BATCH (↔ Spark batch) ############"
R1=$(kk exec zelox-client -- sh -c "$S3ENV SPARK_REMOTE=$SR S3_PATH=s3://zelox/m1batch N_ROWS=$N ENGINE=zelox python3 /tmp/batch.py" 2>&1 | grep -aoE 'BATCH_RESULT.*' | tail -1)
echo "  $R1"

echo ""; echo "############ MODE 2 — STRUCTURED STREAMING availableNow (↔ Spark SS) ############"
KPOD=$(kk get pod -l app=kafka -o jsonpath='{.items[0].metadata.name}')
kk exec "$KPOD" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic m2ss >/dev/null 2>&1
for i in $(seq 1 20); do kk exec "$KPOD" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null | grep -qx m2ss || break; sleep 2; done
kk exec "$KPOD" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic m2ss --partitions 4 --replication-factor 1 >/dev/null 2>&1
# monotonic ts = contiguous 10s windows; COMPLETE_ON_END flushes all windows at bounded end -> sum==N deterministic
python3 -c "import sys,json
N=$N;K=$KEYS;base=1700000000000;pw=max(1,N//100)
sys.stdout.write(''.join(json.dumps({'k':i%K,'ts':base+(i//pw)*10000,'v':1})+'\n' for i in range(N)))" | \
  kk exec -i "$KPOD" -- /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic m2ss >/dev/null 2>&1
kk set env deploy/zelox-stream ZELOX_COMPLETE_ON_END=1 >/dev/null; kk rollout status deploy/zelox-stream --timeout=120s >/dev/null 2>&1
until kk logs zelox-client 2>/dev/null | grep -q CLIENT_READY; do sleep 3; done
R2=$(kk exec zelox-client -- sh -c "$S3ENV SPARK_REMOTE=$SR BOOT=$BOOT TOPIC=m2ss N_EVENTS=$N OUT=s3://zelox/m2ss_out CK=s3://zelox/m2ss_ck python3 /tmp/wagg.py" 2>&1 | grep -aoE 'ZELOX_WAGG.*' | tail -1)
echo "  $R2"

echo ""; echo "############ MODE 3 — REALTIME .trigger(realTime) (↔ Flink), EO across pod-kill ############"
M3LOG=/tmp/m3_rteo.log
TAG=$TAG N=100000 bash scripts/kind_rt_eo.sh 2>&1 | tee "$M3LOG" | grep -aoE 'RT_EO_RUN.*|RT_EO_VERIFY.*|KIND_RT_CRASH_EO.*'
R3=$(grep -aoE 'RT_EO_VERIFY.*|KIND_RT_CRASH_EO.*' "$M3LOG" | tail -2 | tr '\n' ' ')

echo ""; echo "######## THREE-MODE E2E MATRIX (kind + MinIO, image $TAG, N=$N) ########"
echo "  MODE 1 BATCH                (↔ Spark batch) : $R1"
echo "  MODE 2 STRUCTURED STREAMING (↔ Spark SS)    : $R2"
echo "  MODE 3 REALTIME             (↔ Flink)       : $R3"
echo ""
echo "PASS criteria: M1 rows==$N & sum_v==$N (v=1) ; M2 sum_count==$N (complete) ; M3 dup=0 & contiguous (EO)"
