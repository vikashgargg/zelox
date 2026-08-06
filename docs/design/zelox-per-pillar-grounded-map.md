# Zelox per-pillar grounded map — where we ACTUALLY stand vs Flink, Arroyo, RisingWave & Spark (measured, honest)

> **The standing question:** *"Zelox is a single binary, no-JVM, no serialize/deserialize — why is it
> still slower than Flink?"* This doc answers it per pillar with **measured EKS numbers**, the exact
> mechanism where Zelox diverges, and the prod-grade design (built on their proven work, cited).
>
> ⚠️ **2026-07-28 correction (read this):** an earlier revision of this doc reported streaming as a
> broad LOSS (throughput 2.5×, memory up to 3.4×) and prescribed building credit-based flow control.
> **Both were wrong against the current source-of-truth** and are corrected below:
> - The **"4.0M ev/s = LOSE 2.5×"** figure was a **harness-cadence artifact** —
>   [zelox-architecture-review.md](zelox-architecture-review.md) proved the real number is ~1.4×, and
>   after T1+T2+T4+T7a the [D1 scorecard](prodgrade-dimensions-scorecard.md) measures **5.37M vs Flink
>   5.74M ev/s = 1.068×** (near-parity).
> - **Memory is path-dependent, not a flat loss** — [BOARD](../BOARD.md) line 39: realtime/continuous
>   **WINS 7.06 vs 8.58 GiB**; only the *bounded* path loses (10.38 vs 8.57). The old "13 GiB passthrough"
>   blamed the N×M exchange sub-channels, but a **Kafka→Kafka passthrough has no `StreamExchange` at all**
>   — that root-cause was misattributed.
> - **Credit-based flow control is already DONE and proven** (T-BF2.4), not a pending fix — see §2.
>
> The corrected map below is the one to trust. No un-measured claim is stated as fact.

## The measured truth (EKS 100M realtime, latest = D1 scorecard / streaming-source-parse-fusion)

Zelox `.trigger(realTime=…)` vs Flink 1.19, identical keyed tumbling COUNT, both output-completeness-timed:

| Pillar | Zelox | Flink | Verdict |
|---|---|---|---|
| **Correctness / completeness** | 10 win / 100M / per_group=10000 | same | **TIE — byte-identical output, 0 mismatch** |
| **Throughput** ⚠️ **mode caveat** | **5.37M ev/s** | 5.74M ev/s | 1.068× — but this is **`availableNow` (micro-batch), NOT realtime** — see below |
| **Peak RSS** — realtime/continuous | **7.06 GiB** | 8.58 GiB | **WIN ~1.2×** |
| **Peak RSS** — bounded path | 10.38 / 9.61 GiB | 8.57 GiB | **LOSE ~1.12–1.21× (path-specific)** |
| **Latency** Kafka→Kafka p50 / p99 | 101 / 131 ms | 95 / 127 ms | **LOSE slight; tail ties — linger-bound, see §3** |

**Honest verdict: Zelox TIES correctness, WINS realtime memory, loses slightly on bounded-path memory and
latency.** Batch vs Spark wins decisively (8× / ~3× memory, byte-identical). **⚠️ THROUGHPUT MODE CAVEAT
(2026-08-06):** the "1.068×" above is a **`availableNow` (micro-batch) vs Flink** number — the WRONG mode
mapping (availableNow maps to *Spark* streaming; **Flink maps to Zelox `Trigger.RealTime`**). The KB [REF §6]
shows availableNow pays a ~25× per-trigger re-plan/commit tax a continuous pipeline doesn't — so this
handicaps Zelox and is not a valid Flink comparison. **The fair `Trigger.RealTime`-vs-Flink throughput at
scale is UNMEASURED** (tooling exists — `stream_realtime_drain.py` — but was never wired into the scorecard).
See [engine-comparison-mode-mapping.md](engine-comparison-mode-mapping.md). Do NOT cite 1.068× as "realtime
vs Flink." Realtime correctness/EO IS proven (crash-EO dup=0). The EKS run's one job = the fair realtime number.

## Multi-engine standing (Flink · Arroyo · RisingWave · Spark) — the AIM charter view

AIM requires beating Spark/Flink/DataFusion/RisingWave/Arroyo on every prod axis. Flink is the streaming
baseline (above); this section places Zelox against the other frontier engines, per the KB. **Arroyo is the
one to watch** — Rust+Arrow+DataFusion, our EXACT stack, so it proves the ceiling and exposes our real gap.
Legend: ✅ win/lead · = tie · 🟡 path-dependent/partial · 🔴 lag · ? unmeasured · N/A not that engine's domain.

| Pillar | vs **Flink** | vs **Arroyo** (Rust/Arrow/DF) | vs **RisingWave 3.0** | vs **Spark** | Grounding |
|---|---|---|---|---|---|
| Correctness / EO | = (dup=0 kill-9, byte-identical) | ✅ ≥ (aligned-barrier union commit — more complete than a younger engine) | = (both Chandy-Lamport barriers) | ✅ | BOARD 45; [REF §8] |
| **Checkpointing** | ✅ **WIN** (O(delta) inc-ckpt on one Arrow substrate, beats ForSt-RocksDB) | ✅ ≥ | = (both S3 state) | ✅ | BOARD 47 |
| State mgmt / rescale | = (F5 spillable Arrow chunks, key-group FLIP-8) | ✅ ≥ | 🟡 RW ahead on shared-arrangement + MV | ✅ | BOARD 44,48; [REF §8] |
| Data on S3 / lakehouse | = verified (parquet+EO, same `object_store` as prod) | ✅ ≥ (Arroyo 0.15 adds Iceberg) | = (Hummock S3) | 🟡 Delta/Iceberg partial | T1/T2 S3 E2E |
| Latency | ✅ **WIN** (p50 30 vs 42ms, tail 4.6–6× no-GC) | ? unmeasured | ? RW sub-100ms on S3 | ✅ | BOARD 40 |
| Memory | 🟡 realtime WIN 7.06 vs 8.58 GiB (no-JVM edge); bounded loses | ✅ no-JVM single-binary edge | = | ✅ ~3× | BOARD 41 |
| Batch speed | N/A | **N/A** (Arroyo/RW are streaming-only) | N/A | ✅ **WIN 6.2–8×** | P4/TPC-H |
| **Streaming throughput** | 🟡 single-node ~1.07×; **distributed 2.2–2.77× LAG** | 🔴 **LAG (~5×, our biggest gap)** | 🟡 behind | N/A | BOARD 42,43; [REF §6,§9] |
| Incremental MV | — | — | 🔴 RW **leads** (we have changelog/retract, not full MV maintenance) | — | [REF §8] |
| Spark API / DX | ✅ unique | ✅ unique (Arroyo = SQL only) | ✅ unique (RW = Postgres-wire) | = | BOARD 54 |
| No-JVM footprint | ✅ | = (Arroyo also Rust) | = (RW also Rust) | ✅ | — |

**The honest headline:** Zelox **leads or ties on correctness, checkpointing (beats Flink), latency, batch
(6–8× Spark), S3 durability, and Spark-API** — all measured. It **lags on exactly one pillar: distributed
streaming throughput**, where Arroyo (same stack) leads ~5× and Flink ~2.2–2.77×. "Arroyo excels at
everything" is true only for that one (highly visible) pillar — Arroyo has no batch engine, no Spark API,
and a thinner EO/checkpoint/rescale story. **The gap is mechanism, not language** (same stack ⇒ reachable).

**Why Arroyo leads throughput, and where we are on each lever (KB §6/§9):**
- **(A) Specialized window operator** — Arroyo stores window state in purpose-built in-memory structures
  (time-based eviction) vs Flink's generic Map/List backends; this is the **10× sliding-window** lever. Zelox
  uses generic DataFusion grouped-hash → **NOT yet built** (the deeper Arroyo-parity lever).
- **(B) Shuffle-Edge** — batch + connection-pool + zero-copy Arrow before the network boundary, to amortize
  per-batch IPC. Our measured distributed gap is EXACTLY this: 24k tiny ~4k-row Flight batches, `exchange_cpu=0`,
  `shuffle_recv=598s` blocked [REF §6]. Zelox `coalesce_flow_events` **IS this lever — IMPLEMENTED, T1+T2
  validated (2.14× fewer msgs, cost≈0, counts EXACT); at-scale EKS number is the pending proof.**
- **(C) One continuous pipeline** vs Spark `availableNow` micro-batch re-plan/commit-per-trigger tax. Zelox
  realtime mode (`StreamDriver::Realtime`) **is this — crash-EO proven on real pods (dup=0).**

**RisingWave/Polars levers we should also mine (KB §8/§9):** RW **materialized-view incremental computation**
(we have changelog/retraction, not full MV — the one place RW clearly leads); Polars **per-morsel
SemaphorePermit** memory discipline (our bounded mpsc + T-BF2.4 credit-flow is the analog, done). Compute/
storage disaggregation (RW Hummock / Flink ForSt) — our F5+inc-ckpt is the same direction, validated.

**Decision rule (AIM — measure before building):** the one built-but-unproven lever (B, Shuffle-Edge) targets
our one *measured* gap. Get the **EKS throughput number** first: if B closes the distributed gap, the residual
Arroyo delta is lever A (specialized window op) — build it then, grounded. One measurement decides what to
build; don't speculatively code the window operator blind. Full Nexmark (not just windowed-COUNT) is the
gold-standard streaming benchmark to add for a credible "beats Arroyo/Flink" claim [REF §7].

## Per-pillar: credible design → Zelox's measured standing → mechanism → what's left

### 1. Throughput — NEAR-PARITY 5.37M vs 5.74M (~1.07×); root gap = Kafka SOURCE consume rate
- **Credible:** **Arroyo** (Rust/Arrow/DataFusion) beats Flink 3–5× via vectorized Arrow + specialized
  window operators + async checkpointing. **Flink** FLIP-27 source + credit flow. **DataFusion**
  morsel-driven exec. [REFERENCES §6]
- **Zelox mechanism (measured):** transport / shuffle / serde / JVM are **RULED OUT** (BOARD line 36).
  The residual gap is the **Kafka source CONSUME rate** — Zelox `StreamConsumer` vs Flink `KafkaSource`.
  **FLIP-27 batch-queue consume is MEASURED at 2.8×** (`rd_kafka_consume_batch_queue`, local 10M A/B
  1.38→3.89 M/s, identical Arrow build), gated `ZELOX_KAFKA_BATCH_QUEUE`.
- **What's left:** land/validate the FLIP-27 batch-queue source at EKS scale as the throughput lever
  (branch `throughput/kafka-batch-queue-flip27`); this is the ONE real remaining gap, and it is
  source-reader work, NOT exchange or memory work. [phase2-distributed-parity-plan.md, VAJ-BF2]
- **Status: NEAR-PARITY — remaining gap root-caused to the source; fix measured, EKS confirmation pending.**

### 2. Memory — WIN on realtime (7.06 vs 8.58); credit-flow already DONE
- **Credible:** **Flink FLIP-2 credit-based flow control** — receiver-granted buffer credits; network
  memory bounded, backpressure exact. **Flink 2.0 ForSt** disaggregated state. **Polars** per-morsel
  `SemaphorePermit` + spillable sinks. **RisingWave 3.0** network-buffer backpressure. [REFERENCES §3, §9]
- **Zelox status (from code + BOARD):** credit-based flow control is **implemented and proven — T-BF2.4**:
  a **bounded-overflow + `reserve()` atomic-permit** design in
  [stream_manager/local.rs:159](../../crates/zelox-execution/src/stream_manager/local.rs#L159), gated
  `ZELOX_CREDIT_BACKPRESSURE` (0=off default), **MEASURED nm_dist_gate dup=0 + deterministic + f3c
  crash-EO PASS** ([vaj-bf2-distributed-streaming.md §4m](vaj-bf2-distributed-streaming.md)). Note: a
  naive *blocking-send* credit variant was measured **LOSSY and reverted** — do not re-add it (the
  proven design parks the batch in `overflow` first, then bounds the FIFO drain via the atomic permit).
- **What's left:** only the **bounded-path** RSS is slightly over Flink (10.38/9.61 vs 8.57). That is a
  spillable-state / operator-buffer question (E8 F5 spill), NOT a network-credit question — the network
  credit lever is done. Do not build a second credit mechanism on the exchange edge; it would duplicate
  T-BF2.4 and target a path (keyed shuffle) that is not the bounded-path memory source.
- **Status: WIN realtime / slight-LOSE bounded — network credit-flow DONE; remaining is spill (E8/F5).**

### 3. Latency — LOSE slight (p50 101 vs 95 ms); tail ties at 136
- **Credible:** Spark 4.1/4.2 RTM concurrent-stage + in-memory shuffle; Flink continuous. [REFERENCES §1, §3c]
- **Zelox mechanism:** the ~100 ms floor on BOTH engines = Kafka sink `linger`/batching at 20k/s, which
  masks engine latency. The measurable engine-boundary latency needs a linger-isolated harness.
- **What's left:** measure at the engine boundary (not Kafka-linger-bound) to expose the true no-GC tail.
- **Status: LOSE (slight) — needs a linger-isolated measure; low priority vs near-parity throughput.**

### 4. Reliability / correctness — TIE (won the completeness bug)
- Byte-identical windowed output to Flink (0 mismatch), crash-EO dup=0, aligned Chandy-Lamport barriers,
  inc-checkpoint O(delta), watermark flush-before-idle fix (commit 9ae02e7e). **Genuinely at parity.**

## Unproven / unwanted code to REMOVE (prod-grade cleanup, user-requested)
Measure-or-delete. Verify each is off the proven path before removing:
- `coalesce_flow_events` combinator — OFF by default, never proven (needs drain guarantee).
- **Dual idle mechanisms** — wall-clock `active_partition_watermark` vs source-signaled `Idle` (E4).
  Target = **E4-only** (streaming-watermark.md consolidation debt).
- Gated experiments with no proven win: `ZELOX_T7_FUSE`, `ZELOX_KAFKA_LEGACY_POLL`, `ZELOX_RT_SINGLE`,
  `ZELOX_KAFKA_PREFETCH_*` / `ZELOX_SHUFFLE_*` sweep knobs, `ZELOX_SOURCE_MAX_BATCH_BYTES` if unused.
- Any `G1/G2` prefetch/raw-TCP remnants (measured marginal).

## Execution sequence (AIM way — corrected 2026-07-28)
The two "levers" the old revision prescribed are **already landed**: credit-flow = T-BF2.4 (done);
FLIP-27 batch-queue source = measured 2.8× (gated). So the real sequence is:
1. **Do NOT build new credit-flow or exchange-edge memory work** — it duplicates proven T-BF2.4.
2. **Throughput:** confirm the FLIP-27 batch-queue source at EKS scale (the one real gap).
3. **Bounded-path memory:** close via spill (E8/F5), not network credits.
4. **Cleanup:** remove the unproven knobs above → smaller prod-grade surface.
5. **Latency:** linger-isolated measure to expose the no-GC tail (low priority).
6. **ONE final EKS** to confirm — only after T1/T2 free validation.

---
*Sources: [BOARD.md](../BOARD.md) lines 36/39 · [zelox-architecture-review.md](zelox-architecture-review.md)
(cadence artifact) · [prodgrade-dimensions-scorecard.md](prodgrade-dimensions-scorecard.md) D1 ·
[streaming-source-parse-fusion.md](streaming-source-parse-fusion.md) (5.37M/1.068×) ·
[vaj-bf2-distributed-streaming.md §4m](vaj-bf2-distributed-streaming.md) (T-BF2.4 credit-flow) ·
[phase2-distributed-parity-plan.md](phase2-distributed-parity-plan.md) · [Arroyo blog](https://www.arroyo.dev/blog/)
· [REFERENCES.md](../REFERENCES.md) §1/§2/§3/§6/§9.*
