#!/usr/bin/env bash
# Zelox-vs-Flink REALTIME head-to-head — the CORRECT 1:1 (docs/design/engine-comparison-mode-mapping.md).
# Flink maps to Zelox `.trigger(realTime)` (Spark 4.2 Real-Time Mode), NOT availableNow. Both engines run
# their ONE long-lived continuous pipeline draining the SAME pre-loaded backlog (+per-partition closers so
# the watermark advances past the last window), sequentially on the same node = true like-for-like.
# Metric = CATCH-UP drain: Flink via consumer-group lag->0; Zelox via `stream_realtime_drain.py` which
# reports consume_s (true consumption throughput, cadence-robust) + output completeness. Replaces the
# availableNow `eks_stream_headtohead.sh` for the FLINK comparison. Usage: eks_realtime_headtohead.sh [N]
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
N="${1:-100000000}"; REGION="${REGION:-ap-south-1}"; NS=stream; TAG="${TAG:-p2main}"
ECR="$(aws ecr describe-repositories --region $REGION --repository-name zelox --query 'repositories[0].repositoryUri' --output text)"; REG="${ECR%/zelox}"
mask(){ sed -E 's/[0-9]{12}/<ACCT>/g'; }
kk() { kubectl -n "$NS" "$@"; }
wait_ready() { kk wait --for=condition=available --timeout=300s deployment/"$1"; }
echo "ECR=$(echo "$ECR"|mask) N=$N mode=REALTIME(Trigger.RealTime vs Flink continuous)"

echo "==== [1] Kafka + produce $N WITH closers (watermark advance) ===="
kk apply -f k8s/stream/kafka.yaml >/dev/null 2>&1 || kubectl apply -f k8s/stream/kafka.yaml; wait_ready kafka
kk delete job producer --ignore-not-found >/dev/null 2>&1
# CLOSER_TS = one high-event-time sentinel per partition so BOTH engines can close the final windows.
sed -e "s|N_EVENTS\", value: \"[0-9]*\"|N_EVENTS\", value: \"$N\"|" k8s/stream/producer-job.yaml | kk apply -f -
kk wait --for=condition=complete --timeout=1800s job/producer; kk logs job/producer | grep -a PRODUCED

echo "==== [2] FLINK realtime (unbounded, dml-sync=false) drain ===="
FLINK_OUT=$(bash scripts/eks_flink_realtime_drain.sh "$N" 2>&1 | mask); echo "$FLINK_OUT" | grep -aE "FLINK_REALTIME_DRAIN|WARN" || true
FLINK_LINE=$(echo "$FLINK_OUT" | grep -aoE 'FLINK_REALTIME_DRAIN.*' | tail -1)

echo "==== [3] ZELOX realtime (.trigger(realTime)) drain ===="
BUCKET="zelox-rtht-$(date +%s)"; aws s3 mb "s3://$BUCKET" --region "$REGION" >/dev/null && echo "s3://$BUCKET"
cleanup(){ aws s3 rb "s3://$BUCKET" --force >/dev/null 2>&1; }
trap cleanup EXIT
sed -E -e "s|__ECR__/zelox:[A-Za-z0-9._-]+|$REG/zelox:$TAG|g" -e "s|__ECR__|$REG|g" k8s/stream/zelox-stream.yaml | kk apply -f -
kk set env deploy/zelox-stream AWS_REGION="$REGION" >/dev/null; wait_ready zelox-stream
kk apply -f k8s/stream/zelox-client.yaml; kk wait --for=condition=ready --timeout=300s pod/zelox-client
until kk logs zelox-client 2>/dev/null | grep -q CLIENT_READY; do sleep 3; done
kk cp scripts/stream_realtime_drain.py zelox-client:/tmp/rtdrain.py
VPOD=$(kk get pod -l app=zelox-stream -o jsonpath='{.items[0].metadata.name}')
ZOUT=$(kk exec zelox-client -- sh -c \
  "SPARK_REMOTE=sc://zelox-stream.$NS.svc.cluster.local:50051 BOOT=kafka.$NS.svc.cluster.local:9092 TOPIC=events N_EVENTS=$N OUT=s3://$BUCKET/rt CK=s3://$BUCKET/rt_ck AWS_REGION=$REGION python3 /tmp/rtdrain.py" 2>&1 | mask)
echo "$ZOUT" | grep -aE "ZELOX_RT|consume_s|drain_s|P1_VERIFY|VERIFY" || echo "$ZOUT" | tail -5
ZELOX_MEM=$(kk exec "$VPOD" -- sh -c 'cat /sys/fs/cgroup/memory.peak 2>/dev/null || cat /sys/fs/cgroup/memory/memory.max_usage_in_bytes' 2>/dev/null)

echo ""; echo "######## REALTIME HEAD-TO-HEAD (Trigger.RealTime vs Flink continuous, N=$N) ########"
echo "FLINK : $FLINK_LINE"
echo "ZELOX : $(echo "$ZOUT" | grep -aoE 'ZELOX_RT.*|consume_s=[0-9.]+.*' | tail -1)  peakRSS=$(awk -v m="${ZELOX_MEM:-0}" 'BEGIN{printf "%.2f GiB", m/1073741824}')"
echo "(both = ONE continuous pipeline draining the same backlog — the valid Flink comparison)"
echo "Teardown: scripts/aws_eks_teardown.sh <cluster> $REGION when done."
