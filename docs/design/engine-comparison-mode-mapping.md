# Engine-comparison mode mapping — the like-for-like contract (READ before any benchmark)

> **AIM rule:** "Prove it like-for-like vs official Spark + Flink on the same hardware/data; report honestly."
> A comparison is only valid if Zelox runs the mode that **maps 1:1 to the engine it's being compared against.**
> Getting this wrong produces a number that flatters or handicaps unfairly — and wastes an EKS run.

## The three mappings (each Zelox mode → the engine it replaces, in its comparable mode)

| Zelox mode | Trigger / entry | Compared against | Why 1:1 | Harness |
|---|---|---|---|---|
| **Batch** | DataFrame batch API | **Spark 3.5 batch** (`local[16]`) | Both bounded batch write→read→agg to S3 | `eks_batch_s3.sh` + `batch_s3_bench.py` (ENGINE=zelox\|spark) ✅ |
| **Structured streaming** | `.trigger(availableNow=True)` | **Spark Structured Streaming** (`availableNow`) | Both micro-batch: re-plan + commit + checkpoint per trigger | `stream_windowed_agg.py` — **compare vs SPARK, NOT Flink** |
| **Realtime** | `.trigger(realTime=<dur>)` (Spark 4.2 Real-Time Mode) | **Flink** (continuous/unbounded streaming) | Both ONE long-lived continuous pipeline, no per-trigger re-plan | `stream_realtime_drain.py` (Zelox) + `eks_flink_realtime_drain.sh` (Flink) |

**Grounding:** Spark 4.2 Real-Time Mode [REF §1/§10] is the *continuous* trigger that "breaks the micro-batch
barrier" — it is the mode that maps to Flink's continuous engine. `availableNow` is bounded micro-batch and
maps to *Spark's own* streaming, NOT Flink. The KB [REF §6] measured that `availableNow` pays a per-trigger
re-plan/commit/checkpoint tax (~25× at 100M) that a continuous pipeline does not — so comparing Zelox-
`availableNow` to Flink-continuous **handicaps Zelox and is not a valid Flink comparison.**

## The defect this doc fixes (found 2026-08-06)
- `tri_engine_scorecard.sh` (main orchestrator) and `eks_stream_headtohead.sh` run Zelox on **`availableNow`
  vs Flink** — the WRONG mapping (should be Spark's counterpart, or use realtime for Flink).
- The D1 scorecard **"5.37M vs 5.74M = 1.068×"** ([prodgrade-dimensions-scorecard.md](prodgrade-dimensions-scorecard.md))
  is therefore an **availableNow-vs-Flink** number, **NOT** the fair `Trigger.RealTime`-vs-Flink 1:1. Every
  doc that cites 1.068× as "the realtime-vs-Flink standing" is citing the wrong-mode number — corrected here.
- The correct realtime tooling (`stream_realtime_drain.py` = Zelox `Trigger.RealTime`, drain-to-completion by
  polling S3 `sum(count)`→N; `eks_flink_realtime_drain.sh` = Flink unbounded, drain by consumer-group lag→0)
  **exists but was never wired into a unified scorecard** → the fair realtime-vs-Flink throughput at scale is
  **UNMEASURED**. That measurement is the one legitimate purpose of the EKS run.

## What "correct" looks like (the fix)
1. **Realtime scorecard** = `stream_realtime_drain.py` vs `eks_flink_realtime_drain.sh`, both draining the SAME
   pre-loaded backlog to output-completeness, same node, sequentially. Metric = catch-up drain ev/s + peak RSS.
   This replaces `eks_stream_headtohead.sh` for the **Flink** comparison.
2. **Structured-streaming scorecard** (optional, secondary) = `availableNow` Zelox vs Spark `availableNow` —
   a Zelox-vs-SPARK streaming number, clearly labelled as such (never presented as "vs Flink").
3. **Batch scorecard** = unchanged (already batch↔Spark, correct).
4. **Validate the realtime harness FREE on kind** (Zelox `Trigger.RealTime` vs Flink) before EKS; EKS only for
   the at-scale number.

## Standing status
- Fair **realtime-vs-Flink throughput at scale = NOT YET MEASURED** (the wired harness used the wrong mode).
- Realtime correctness/EO IS proven (crash-EO dup=0 on real pods, [[zelox-phase2-t2-standing]]).
- **Do not cite "1.068× near-parity vs Flink" as a realtime result** — it is an availableNow number. The honest
  line until the realtime scorecard runs: *the fair realtime-vs-Flink throughput is unmeasured; correctness is
  proven; the mode mapping is now fixed so the EKS run measures the right thing.*

*Sources: AIM.md (like-for-like rule) · REF §1/§6/§10 (Spark 4.2 RTM, availableNow micro-batch tax) · Flink
continuous streaming. Supersedes the mode choice in eks_stream_headtohead.sh / tri_engine_scorecard.sh.*
