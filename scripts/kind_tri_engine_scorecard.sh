#!/usr/bin/env bash
# CHIEF-ARCHITECT tri-engine scorecard on kind + MinIO (S3 API) — the FREE, prod-grade, reusable local
# head-to-head before EKS. Zelox = ONE engine; each mode compared 1:1 with the engine it replaces
# (REFERENCES §0 / engine-comparison-mode-mapping.md), all on real pods + real object store:
#   MODE batch  : Zelox batch        vs Spark 4.2 batch            (throughput, peak RSS, rows/sum correctness)
#   MODE ss     : Zelox availableNow vs Spark 4.2 Structured Stream (throughput, completeness, RSS)
#   MODE rt     : Zelox .trigger(realTime) vs Flink continuous     (drain throughput, RSS; EO = kind_rt_eo.sh)
# Sequential (never concurrent) on the same cluster = fair. Requires kind up with zelox:$TAG loaded +
# Kafka+MinIO in ns stream (scripts/kind_up.sh). Usage: TAG=p2main N=2000000 MODE=all scripts/kind_tri_engine_scorecard.sh
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
CTX="${CTX:-kind-zelox-kind}"; NS=stream; TAG="${TAG:-p2main}"; N="${N:-2000000}"; MODE="${MODE:-all}"
MINIO_EP="http://minio.$NS.svc.cluster.local:9000"
kk() { kubectl --context "$CTX" -n "$NS" "$@"; }
S3ENV="AWS_REGION=us-east-1 AWS_ENDPOINT=$MINIO_EP AWS_ENDPOINT_URL=$MINIO_EP AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_ALLOW_HTTP=true"
SR="sc://zelox-stream.$NS.svc.cluster.local:50051"; BOOT="kafka.$NS.svc.cluster.local:9092"
scale_kind() { sed -E -e 's/cpu: "1[0-9]"/cpu: "1"/g' -e 's/cpu: "[3-9]"/cpu: "1"/g' -e 's/memory: "2[0-9]Gi"/memory: "2Gi"/g' -e 's/memory: "1[0-9]Gi"/memory: "1500Mi"/g' -e 's/memory: "[0-9]+Gi"/memory: "1500Mi"/g'; }
rss_gib() { kk exec deploy/"$1" -- sh -c 'cat /sys/fs/cgroup/memory.peak 2>/dev/null' 2>/dev/null | awk '{printf "%.2f",$1/1073741824}'; }
clear_s3() { kk exec zelox-client -- sh -c "$S3ENV python3 -c \"import boto3; c=boto3.client('s3',endpoint_url='$MINIO_EP',aws_access_key_id='minioadmin',aws_secret_access_key='minioadmin'); [c.delete_object(Bucket='zelox',Key=o['Key']) for p in ['$1/','${1}_ck/'] for o in c.list_objects_v2(Bucket='zelox',Prefix=p).get('Contents',[])]\"" 2>/dev/null || true; }
R_BATCH=""; R_SS=""; R_RT=""

deploy_zelox() {
  kk get deploy zelox-stream >/dev/null 2>&1 && return
  sed -E -e "s#__ECR__/zelox:[A-Za-z0-9._-]+#zelox:$TAG#g" -e 's/imagePullPolicy: Always/imagePullPolicy: IfNotPresent/g' k8s/stream/zelox-stream.yaml | scale_kind | kk apply -f - >/dev/null
  kk patch deploy zelox-stream --type=json -p='[{"op":"add","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"Never"}]' >/dev/null 2>&1
  kk set env deploy/zelox-stream $S3ENV ZELOX_COMPLETE_ON_END=1 >/dev/null
  kk wait --for=condition=available --timeout=240s deployment/zelox-stream >/dev/null
  kk get pod zelox-client >/dev/null 2>&1 || scale_kind < k8s/stream/zelox-client.yaml | kk apply -f - >/dev/null
  kk wait --for=condition=ready --timeout=200s pod/zelox-client >/dev/null
  until kk logs zelox-client 2>/dev/null | grep -q CLIENT_READY; do sleep 3; done
}
produce() { # $1=topic $2=N  (robust k8s librdkafka producer job)
  local KPOD; KPOD=$(kk get pod -l app=kafka -o jsonpath='{.items[0].metadata.name}')
  kk exec "$KPOD" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$1" >/dev/null 2>&1; sleep 3
  kk delete job producer --ignore-not-found >/dev/null 2>&1
  sed -E -e "s/name: TOPIC, value: \"events\"/name: TOPIC, value: \"$1\"/" -e "s/name: N_EVENTS, value: \"[0-9]*\"/name: N_EVENTS, value: \"$2\"/" -e 's/name: N_PARTS, value: "[0-9]*"/name: N_PARTS, value: "4"/' k8s/stream/producer-job.yaml | scale_kind | kk apply -f - >/dev/null
  kk wait --for=condition=complete --timeout=900s job/producer >/dev/null 2>&1
  echo "  produced $1=$(kk exec "$KPOD" -- /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$1" 2>/dev/null | awk -F: '{s+=$3} END{print s}')"
}

batch_mode() {
  echo "######## MODE batch — Zelox vs Spark 4.2 (-> MinIO) ########"; deploy_zelox
  kk cp scripts/batch_s3_bench.py zelox-client:/tmp/batch.py; clear_s3 zbatch
  local Z; Z=$(kk exec zelox-client -- sh -c "$S3ENV SPARK_REMOTE=$SR S3_PATH=s3://zelox/zbatch N_ROWS=$N ENGINE=zelox python3 /tmp/batch.py" 2>&1 | grep -aoE 'BATCH_RESULT.*' | tail -1)
  local ZM; ZM=$(rss_gib zelox-stream); echo "  ZELOX : $Z peakRSS=${ZM}GiB"
  kk scale deploy/zelox-stream --replicas=0 >/dev/null; sleep 4   # fair: free the node for Spark
  clear_s3 sbatch
  kk create configmap batch-s3-script --from-file=batch_s3_bench.py=scripts/batch_s3_bench.py --dry-run=client -o yaml | kk apply -f - >/dev/null
  kk delete job spark-batch-s3 --ignore-not-found >/dev/null 2>&1
  sed -e "s|__S3_PATH__|s3://zelox/sbatch|" -e "s|__N_ROWS__|$N|" k8s/kind/spark42-batch-minio.yaml | kk apply -f - >/dev/null
  kk wait --for=condition=complete --timeout=1200s job/spark-batch-s3 >/dev/null 2>&1
  local S; S=$(kk logs job/spark-batch-s3 2>/dev/null | grep -aoE 'BATCH_RESULT.*|SPARK_PEAK_RSS_GiB=.*' | tr '\n' ' ')
  kk scale deploy/zelox-stream --replicas=1 >/dev/null 2>&1
  echo "  SPARK : $S"; R_BATCH="Z[$Z rss=${ZM}] | S[$S]"
}

ss_mode() {
  echo "######## MODE ss — Zelox availableNow vs Spark 4.2 Structured Streaming (-> MinIO) ########"; deploy_zelox
  produce m_ss "$N"; kk cp scripts/stream_windowed_agg.py zelox-client:/tmp/wagg.py; clear_s3 zss_out
  local Z; Z=$(kk exec zelox-client -- sh -c "$S3ENV SPARK_REMOTE=$SR BOOT=$BOOT TOPIC=m_ss N_EVENTS=$N OUT=s3://zelox/zss_out CK=s3://zelox/zss_out_ck python3 /tmp/wagg.py" 2>&1 | grep -aoE 'ZELOX_WAGG.*' | tail -1)
  local ZM; ZM=$(rss_gib zelox-stream); echo "  ZELOX : $Z peakRSS=${ZM}GiB"
  kk scale deploy/zelox-stream --replicas=0 >/dev/null; sleep 4; clear_s3 sss_out
  kk create configmap spark-ss-script --from-file=spark_ss_wagg.py=scripts/spark_ss_wagg.py --dry-run=client -o yaml | kk apply -f - >/dev/null
  kk delete job spark-ss --ignore-not-found >/dev/null 2>&1
  sed -e "s|__S3_OUT__|s3://zelox/sss_out|" -e "s|__N_ROWS__|$N|" -e "s|__TOPIC__|m_ss|" k8s/kind/spark42-ss-minio.yaml | kk apply -f - >/dev/null
  kk wait --for=condition=complete --timeout=1200s job/spark-ss >/dev/null 2>&1
  local S; S=$(kk logs job/spark-ss 2>/dev/null | grep -aoE 'SPARK_SS.*|SPARK_SS_PEAK_RSS_GiB=.*' | tr '\n' ' ')
  kk scale deploy/zelox-stream --replicas=1 >/dev/null 2>&1
  echo "  SPARK : $S"; R_SS="Z[$Z rss=${ZM}] | S[$S]"
}

rt_mode() {
  echo "######## MODE rt — Zelox .trigger(realTime) vs Flink continuous (drain) ########"; deploy_zelox
  produce m_rt "$N"; kk cp scripts/stream_realtime_drain.py zelox-client:/tmp/rtdrain.py; clear_s3 zrt_out
  local Z; Z=$(kk exec zelox-client -- sh -c "$S3ENV SPARK_REMOTE=$SR BOOT=$BOOT TOPIC=m_rt N_EVENTS=$N OUT=s3://zelox/zrt_out CK=s3://zelox/zrt_out_ck MAX_SECS=240 python3 /tmp/rtdrain.py" 2>&1 | grep -aoE 'ZELOX_RT.*|consume_s=.*' | tail -1)
  local ZM; ZM=$(rss_gib zelox-stream); echo "  ZELOX : $Z peakRSS=${ZM}GiB"
  # Flink side: reuse the proven realtime drain harness (deploys Flink, polls consumer-group lag -> 0).
  echo "  (Flink side: scripts/eks_flink_realtime_drain.sh — deploy Flink on kind + lag-drain; run separately if Flink image fits)"
  R_RT="Z[$Z rss=${ZM}] | Flink[see eks_flink_realtime_drain.sh]"
}

case "$MODE" in
  batch) batch_mode ;;
  ss) ss_mode ;;
  rt) rt_mode ;;
  all) batch_mode; echo; ss_mode; echo; rt_mode ;;
  *) echo "usage: MODE={batch|ss|rt|all}"; exit 1 ;;
esac
echo ""; echo "######## KIND TRI-ENGINE SCORECARD (image $TAG, N=$N, MinIO S3) ########"
[ -n "$R_BATCH" ] && echo "  BATCH (vs Spark 4.2) : $R_BATCH"
[ -n "$R_SS" ]    && echo "  SS    (vs Spark 4.2) : $R_SS"
[ -n "$R_RT" ]    && echo "  RT    (vs Flink)     : $R_RT"
