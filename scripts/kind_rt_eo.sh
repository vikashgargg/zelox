#!/usr/bin/env bash
# T2 kind — REALTIME (.trigger(realTime)) exactly-once across a HARD POD-KILL, PASS-THROUGH (no window/
# watermark → dup=0 is timing-independent, unlike windowed-append which needs watermark finalization and
# flakes on a cpu:1 kind VM). Produce unique ids 0..N-1, continuous Kafka->parquet on MinIO S3,
# `kubectl delete pod --grace-period=0 --force` the stream server mid-drain (SIGKILL), resume from the S3
# checkpoint, verify the durable output is EXACTLY 0..N-1 (no dup, no loss) = the Flink-parity crash-EO
# guarantee on real distributed pods, FREE (docs/design/three-tier-sdlc.md, T2 before any EKS spend).
# Assumes `TAG=<tag> scripts/kind_up.sh` loaded the image and Kafka+MinIO are deployed in ns `stream`.
# Usage: TAG=p2main N=100000 scripts/kind_rt_eo.sh
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
CTX="${CTX:-kind-zelox-kind}"; NS=stream; TAG="${TAG:-p2main}"; N="${N:-100000}"; TOPIC=rt_eo
KILL_AT="${KILL_AT:-15}"; RUN1="${RUN1:-30}"; RUN2="${RUN2:-45}"
MINIO_EP="http://minio.$NS.svc.cluster.local:9000"
kk() { kubectl --context "$CTX" -n "$NS" "$@"; }
S3ENV="AWS_REGION=us-east-1 AWS_ENDPOINT=$MINIO_EP AWS_ENDPOINT_URL=$MINIO_EP AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_ALLOW_HTTP=true"

echo "==== [0] clear S3 output/checkpoint (reproducibility — no stale-state dup confound) ===="
kk exec zelox-client -- sh -c "$S3ENV python3 -c \"import boto3; c=boto3.client('s3',endpoint_url='$MINIO_EP',aws_access_key_id='minioadmin',aws_secret_access_key='minioadmin'); [c.delete_object(Bucket='zelox',Key=o['Key']) for p in ['rteo/','rteo_ck/'] for o in c.list_objects_v2(Bucket='zelox',Prefix=p).get('Contents',[])]\"" 2>/dev/null && echo "cleared rteo + rteo_ck" || echo "clear best-effort"

echo "==== [1] fresh topic $TOPIC + produce $N unique ids ===="
KPOD=$(kk get pod -l app=kafka -o jsonpath='{.items[0].metadata.name}')
kk exec "$KPOD" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$TOPIC" >/dev/null 2>&1
for i in $(seq 1 30); do kk exec "$KPOD" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null | grep -qx "$TOPIC" || break; sleep 2; done
kk exec "$KPOD" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic "$TOPIC" --partitions 4 --replication-factor 1 >/dev/null 2>&1
python3 -c "import sys
sys.stdout.write(''.join('{\"id\": %d}\n'%i for i in range($N)))" | \
  kk exec -i "$KPOD" -- /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic "$TOPIC" >/dev/null 2>&1
echo "TOPIC $TOPIC=$(kk exec "$KPOD" -- /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$TOPIC" 2>/dev/null | awk -F: '{s+=$3} END{print s}')"

echo "==== [2] client: continuous realTime pass-through -> MinIO S3 ===="
kk cp scripts/rt_eo_query.py zelox-client:/tmp/rteo.py
SR="sc://zelox-stream.$NS.svc.cluster.local:50051"; BOOT="kafka.$NS.svc.cluster.local:9092"
runq() { kk exec zelox-client -- sh -c "$S3ENV SPARK_REMOTE=$SR BOOT=$BOOT TOPIC=$TOPIC OUT=s3://zelox/rteo CK=s3://zelox/rteo_ck RUN_SECS=$1 python3 /tmp/rteo.py $2" 2>&1; }

echo "== run1 ($RUN1 s) with a hard pod-kill at ${KILL_AT}s =="
runq "$RUN1" run >/tmp/rteo_run1.log 2>&1 &
RUNPID=$!
sleep "$KILL_AT"
echo "== CHAOS: kubectl delete pod -l app=zelox-stream --grace-period=0 --force (SIGKILL mid-drain) =="
kk delete pod -l app=zelox-stream --grace-period=0 --force >/dev/null 2>&1
wait $RUNPID 2>/dev/null; echo "(run1 died with the server)"; grep -aoE 'RT_EO_RUN.*' /tmp/rteo_run1.log | tail -1 || true
kk wait --for=condition=available --timeout=300s deployment/zelox-stream >/dev/null
until kk logs zelox-client 2>/dev/null | grep -q CLIENT_READY; do sleep 3; done
echo "== run2 ($RUN2 s): resume from S3 checkpoint =="
runq "$RUN2" run | grep -aoE 'RT_EO_RUN.*' || true

echo "==== [3] verify durable output = exactly 0..$((N-1)) ===="
kk exec zelox-client -- sh -c "$S3ENV SPARK_REMOTE=$SR N=$N OUT=s3://zelox/rteo python3 /tmp/rteo.py verify" 2>&1 | grep -aE "RT_EO_VERIFY|KIND_RT_CRASH_EO"
