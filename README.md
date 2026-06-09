# agave-votor-scope

A two-part latency testbench for Solana's block production engine. Part A hooks into a real Agave `BankingStage` and records end-to-end packet→PoH latency. Part B is a pure Tokio model that replays the same pipeline shape so `tokio-console` can show *why* latency spikes happen without needing to instrument C++/Rust FFI internals.

Both parts measure against Votor's slot budget from the Alpenglow paper: `Δ_block = 400 ms` per slot, with a skip-vote timeout at `Δ_timeout = 1200 ms`.

---

## Repo layout

| Directory | What it does |
|---|---|
| `src/` | Tokio model: producer → scheduler → N workers → collector, plus jitter injectors |
| `banking-jitter/` | Real Agave harness: spins up `BankingStage`, feeds packets, waits on PoH entries |
| `votor-budget/` | Shared constants & timeout math pulled from the paper's Algorithm 2 |
| `docs/` | Notes on where each constant came from in the white paper |

The split exists because `banking-jitter` path-depends on `../../agave` (it literally compiles Agave's `solana-core`). That's heavy. The Tokio model lives in `src/` and builds standalone.

---

## Quick start

```bash
# Tokio lab (no Agave needed)
cargo test
RUSTFLAGS="--cfg tokio_unstable" cargo run -- --jitter cpu --duration-secs 5

# Real BankingStage (needs Agave cloned at ../agave)
cd banking-jitter
cargo test real_banking_stage_baseline_stays_inside_delta_block -- --nocapture
cargo test -- --ignored --nocapture   # stress modes
```

Attach tokio-console while the lab runs:

```bash
cargo install tokio-console
tokio-console
```

---

## Jitter modes

The harness injects three kinds of interference, each with a distinct latency signature:

| Mode | What it simulates | Visible in |
|---|---|---|
| `cpu` | Spawns hog threads that compete for cores | schedule delay spikes, execute stays flat |
| `io` | Calls `std::thread::sleep` on every 8th batch | execute tail stretches (blocked worker) |
| `alloc` | Allocates and touches a large Vec before each batch | execute work increases, occasional end-to-end spikes |

`none` is the clean baseline.

---

## Votor budget at a glance

| Param | Value | Source |
|---|---|---|
| Network round δ | 80 ms | Section 1.5 yardstick |
| Synchrony bound Δ | 400 ms | Conservative timeout sizing |
| Block budget Δ_block | 400 ms | Table 10 |
| Skip timeout Δ_timeout | 3Δ = 1200 ms | Section 2.6 |
| Leader window | 4 slots | Table 10 parameter w |

Slot timeout schedule (Algorithm 2):

```
Timeout(slot+0) = 1600 ms
Timeout(slot+1) = 2000 ms
Timeout(slot+2) = 2400 ms
Timeout(slot+3) = 2800 ms
```

The real target is tighter: block production should complete within `Δ_block = 400 ms`.

---

## Sample output

### Part A — Real Agave baseline

```
Agave BankingStage jitter mode: None
samples: 24
Votor Δ_block: 400 ms
latency p50=0.100ms p90=0.165ms p99=12.199ms max=12.199ms
p99 headroom vs Δ_block: 388 ms
```

Baseline p99 is ~12ms against a 400ms budget — plenty of room.

### Part B — Tokio model (5-second runs)

| Mode | Schedule delay p99 | Execute p50 | Execute p99 |
|---|---|---|---|
| none | 16ms | 0.19ms | 0.33ms |
| cpu | 81ms | 0.16ms | 0.18ms |
| io | 24ms | 0.16ms | 12.7ms |
| alloc | 13ms | 0.74ms | 0.95ms |

CPU pressure shows up as *waiting* (schedule delay). I/O shows up as *execution* (the worker blocks). Allocator churn raises per-batch work cost.

---

## Diagnosing jitter from the numbers

1. schedule delay ↑, execute flat → CPU oversubscription
2. execute tail long → blocking call or oversized batch
3. execute rises with allocation size → memory/allocator pressure
4. p99 headroom → 0 → BankingStage is eating into Votor's timeout slack

---

## Pipeline shape the harness models

```
PacketProducer ──→ BankingScheduler ──→ [Worker 0..N] ──→ Collector
                        │                    │
                        │              jitter injector
                        │              (cpu / io / alloc)
                        │
                  (separate Tokio task so console
                   shows schedule delay here)
```

The real Agave path this mirrors:

```
BankingStage::new_num_threads
  → spawn_internal_central
  → receive_and_buffer_packets
  → GreedyScheduler
  → ConsumeWorker::run
  → Consumer::process_and_record_aged_transactions
  → PohRecorder record
```

---

## Caveats

- `banking-jitter/` sends small single-transfer batches — not a full validator workload
- The Tokio model's absolute numbers are illustrative; the relative shapes (which metric spikes under which mode) are the useful part
- Real BankingStage workers are OS threads + crossbeam, not Tokio tasks. The model exists because `tokio-console` can't observe crossbeam channels directly
- `console-subscriber` uses a fixed port — don't run multiple lab instances at once
