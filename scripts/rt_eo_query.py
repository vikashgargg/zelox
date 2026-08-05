#!/usr/bin/env python3
"""Realtime (.trigger(realTime)) PASS-THROUGH exactly-once probe for kind/EKS crash-EO
(scripts/kind_rt_eo.sh). Pass-through (no window/watermark) so dup=0 is timing-independent — the correct
deterministic realtime EO test (windowed-append needs watermark finalization and flakes on a cpu:1 VM).
  run    : continuous Kafka->parquet for RUN_SECS, then stop cleanly.
  verify : read durable S3 output, assert ids == exactly 0..N-1 (no dup, no loss)."""
import os, sys, time
from pyspark.sql import SparkSession, functions as F

PHASE = sys.argv[1] if len(sys.argv) > 1 else "run"
SR = os.environ["SPARK_REMOTE"]; OUT = os.environ.get("OUT"); CK = os.environ.get("CK")
s = SparkSession.builder.remote(SR).getOrCreate()

if PHASE == "run":
    BOOT = os.environ["BOOT"]; TOPIC = os.environ["TOPIC"]; RUN = int(os.environ.get("RUN_SECS", "30"))
    raw = (s.readStream.format("kafka").option("kafka.bootstrap.servers", BOOT)
           .option("subscribe", TOPIC).option("startingOffsets", "earliest").load())
    rows = raw.select(F.get_json_object(F.col("value").cast("string"), "$.id").cast("long").alias("id"))
    q = (rows.writeStream.format("parquet").option("path", OUT).option("checkpointLocation", CK)
         .outputMode("append").trigger(realTime="2 seconds").start())
    t0 = time.time()
    while time.time() - t0 < RUN and q.isActive:
        time.sleep(2)
    if q.isActive:
        q.stop()
    print(f"RT_EO_RUN ran_s={time.time()-t0:.1f} trigger=realTime", flush=True)
elif PHASE == "verify":
    N = int(os.environ["N"])
    d = s.read.parquet(OUT).where(F.col("id").isNotNull())
    total = d.count(); distinct = d.select("id").distinct().count()
    mn = d.agg(F.min("id")).collect()[0][0]; mx = d.agg(F.max("id")).collect()[0][0]
    dup = total - distinct
    contiguous = (distinct == N and mn == 0 and mx == N - 1)
    ok = (dup == 0 and contiguous)
    print(f"RT_EO_VERIFY total={total} distinct={distinct} dup={dup} min={mn} max={mx} expect_N={N}", flush=True)
    print(f"KIND_RT_CRASH_EO {'PASS' if ok else 'FAIL'} (dup={dup}, distinct==N={distinct==N}, contiguous={contiguous})", flush=True)
