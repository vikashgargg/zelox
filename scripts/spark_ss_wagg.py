#!/usr/bin/env python3
"""Spark 4.2 Structured Streaming (availableNow) side of the SS comparison vs Zelox `.trigger(availableNow)`
(REFERENCES §0). IDENTICAL logical query to scripts/stream_windowed_agg.py: read Kafka `events`
(JSON {k,ts,v}), 10s event-time TUMBLE window, GROUP BY window+k, COUNT(*), write Parquet to S3 (MinIO).
availableNow = bounded micro-batch drain -> wall = catch-up throughput. Env: BENCH_REMOTE, BOOT, TOPIC,
N_ROWS, S3_OUT."""
import os, time
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, LongType, IntegerType

REMOTE = os.environ.get("BENCH_REMOTE", "local[2]")
BOOT = os.environ["BOOT"]; TOPIC = os.environ["TOPIC"]
N = int(os.environ.get("N_ROWS", "2000000")); S3_OUT = os.environ["S3_OUT"]
CK = S3_OUT.rstrip("/") + "_ck"

s = SparkSession.builder.master(REMOTE).appName("spark-ss-wagg").getOrCreate()
s.sparkContext.setLogLevel("WARN")
schema = StructType([StructField("k", LongType()), StructField("ts", LongType()), StructField("v", IntegerType())])

raw = (s.readStream.format("kafka")
       .option("kafka.bootstrap.servers", BOOT).option("subscribe", TOPIC)
       .option("startingOffsets", "earliest").load())
parsed = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
          .select(F.col("e.k").alias("k"),
                  (F.col("e.ts") / 1000).cast("timestamp").alias("event_time")))
agg = (parsed.groupBy(F.window("event_time", "10 seconds"), F.col("k")).count())

t0 = time.time()
q = (agg.writeStream.format("parquet").outputMode("append")
     .option("path", S3_OUT).option("checkpointLocation", CK)
     .trigger(availableNow=True).start())
q.awaitTermination()
wall = time.time() - t0
d = s.read.parquet(S3_OUT)
nwin = d.select("window").distinct().count(); tot = d.agg(F.sum("count")).collect()[0][0]
print(f"SPARK_SS events={N} wall_s={wall:.2f} throughput={N/wall/1e6:.3f}M_events/s "
      f"n_windows={nwin} sum_count={tot}", flush=True)
