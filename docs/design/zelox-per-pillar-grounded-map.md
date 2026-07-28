# Zelox per-pillar grounded map — where we ACTUALLY stand vs Flink (measured, honest)

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
| **Throughput** (backlog drain→output-complete) | **5.37M ev/s** | 5.74M ev/s | **NEAR-PARITY — 1.068× slower** |
| **Peak RSS** — realtime/continuous | **7.06 GiB** | 8.58 GiB | **WIN ~1.2×** |
| **Peak RSS** — bounded path | 10.38 / 9.61 GiB | 8.57 GiB | **LOSE ~1.12–1.21× (path-specific)** |
| **Latency** Kafka→Kafka p50 / p99 | 101 / 131 ms | 95 / 127 ms | **LOSE slight; tail ties — linger-bound, see §3** |

**Honest verdict: Zelox TIES correctness, WINS realtime memory, is within ~7% on throughput, loses
slightly on the bounded-path memory and on latency (both linger/path artifacts).** This is a
**near-parity** streaming standing, not the broad loss the old revision claimed — and batch vs Spark
wins decisively (8× / ~3× memory, byte-identical). The remaining true gap is narrow and source-side (§1).

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
