# agave-votor-scope

I pulled Votor's timing budget out of the Alpenglow paper and built a harness that measures Agave's real `banking_stage` against it. I also included a separate Tokio model so `tokio-console` can show what different task-jitter sources look like without using printf debugging.

## Project structure

```
agave-votor-scope/
├── src/                  # Tokio jitter lab (Part B — runs with tokio-console)
│   ├── main.rs           # CLI entry point
│   ├── pipeline.rs       # Banking pipeline: producer → scheduler → workers → collector
│   ├── jitter.rs         # Jitter injection modes: None, Cpu, Io, Alloc
│   ├── metrics.rs        # hdrhistogram latency recording
│   ├── workload.rs       # Synthetic transaction batch hashing
│   └── votor.rs          # Re-exports VotorBudget from votor-budget crate
├── banking-jitter/       # Real Agave BankingStage harness (Part A — path-depends on ../agave)
│   └── src/lib.rs        # Instantiates real BankingStage, sends packets, records PoH latency
├── votor-budget/         # Shared library: Votor timing constants + slot-timeout schedule
│   └── src/lib.rs        # VotorBudget struct, constants, banner, Algorithm 2 math
├── docs/
│   └── votor-timing.md   # Detailed extraction notes from the Alpenglow white paper
└── Cargo.toml            # Workspace root
```

## The problem

## The problem

Extract the timing budget Votor actually requires, build something that measures Agave's real `core/src/banking_stage.rs` against it, and diagnose where jitter comes from: CPU scheduling, I/O stalls, and allocator pauses.

## What you need to know first

`banking_stage`: Agave's block-production stage. It receives verified transaction packets and turns them into recorded entries while a validator is leader.

`Consumer`: the inner BankingStage component that processes transaction batches and records them to PoH.

`Votor`: Alpenglow's voting engine. For this assignment, the important point is that block production has to stay inside the slot budget so votes are not pushed toward timeout.

`tokio-console`: a task-level tracing tool. It is useful for Tokio tasks, but it does not directly explain Agave's BankingStage worker threads because those are mostly OS threads and crossbeam channels.

## The Votor budget

The timing budget comes from Alpenglow's Votor timeout definition, Algorithm 2, and protocol parameter table.

The paper uses two different delay symbols:

| Symbol | Value | Meaning |
| --- | ---: | --- |
| `δ` | about `80 ms` | actual network delay / one voting-round yardstick |
| `Δ` | about `400 ms` | conservative synchrony bound used for timeout sizing |
| `Δ_block` | `400 ms` | block time / per-slot production budget |
| `Δ_timeout` | `3Δ = 1200 ms` | timeout slack before skip voting |

The timeout formula is:

```text
Timeout(i) = clock() + Δ_timeout + (i - s + 1) * Δ_block
```

With a four-slot leader window, the harness prints these deadlines:

| Slot offset | Timeout from ParentReady |
| ---: | ---: |
| `slot + 0` | `1600 ms` |
| `slot + 1` | `2000 ms` |
| `slot + 2` | `2400 ms` |
| `slot + 3` | `2800 ms` |

The trap is that `Δ_timeout` is based on uppercase `Δ`, not lowercase `δ`. So it is `3 * 400 ms = 1200 ms`, not `3 * 80 ms`.

The real measurement target is stricter than the skip timeout: block production should fit inside `Δ_block = 400 ms`.

## My approach

The assignment asks for two things that cannot cleanly live in one process: observe real Agave `banking_stage`, and use `tokio-console` to diagnose task jitter.

Part A is `banking-jitter/`. This is the real-system harness. It path-depends on a sibling Agave checkout at `../../agave`, starts the actual `solana_core::banking_stage::BankingStage`, sends real `BankingPacketBatch` values through Agave's banking channels, waits for real PoH-recorded transaction entries, and records send-to-record latency with `hdrhistogram`.

Part B is `src/`. This is the Tokio jitter lab. It is shaped like a small banking pipeline: producer, scheduler, workers, collector. It has explicit jitter modes for CPU pressure, blocking I/O-style stalls, and allocator churn. This is the part to run with `tokio-console`, because the tasks are actual Tokio tasks with names.

The real Agave source path under observation is:

```text
../agave/core/src/banking_stage.rs
```

The harness follows this path:

```text
BankingStage::new_num_threads
  -> spawn_internal_central
  -> TransactionViewReceiveAndBuffer::receive_and_buffer_packets
  -> SchedulerController / GreedyScheduler
  -> ConsumeWorker::run
  -> Consumer::process_and_record_aged_transactions
  -> TransactionRecorder / PohRecorder record channel
```

## Running it

Clone Agave next to this repo's parent directory if it is not already there:

```bash
git clone https://github.com/zsh28/agave ../agave
```

Run the root unit tests:

```bash
cargo test
```

Run the real Agave BankingStage baseline measurement:

```bash
cd banking-jitter
cargo test real_banking_stage_baseline_stays_inside_delta_block -- --nocapture
```

Run all real Agave BankingStage tests, including source-path and budget checks:

```bash
cd banking-jitter
cargo test -- --nocapture
```

Run the optional real-system stress modes:

```bash
cd banking-jitter
cargo test -- --ignored --nocapture
```

Run the Tokio-console lab one mode at a time:

```bash
RUSTFLAGS="--cfg tokio_unstable" cargo run -- --jitter none --duration-secs 5
RUSTFLAGS="--cfg tokio_unstable" cargo run -- --jitter cpu --duration-secs 5
RUSTFLAGS="--cfg tokio_unstable" cargo run -- --jitter io --duration-secs 5
RUSTFLAGS="--cfg tokio_unstable" cargo run -- --jitter alloc --duration-secs 5
```

Attach `tokio-console` in a second terminal while one root run is active:

```bash
cargo install tokio-console
tokio-console
```

Do not run multiple root modes in parallel. `console-subscriber` opens a console server port, so parallel runs can report `Address already in use` even though the harness still prints local histograms.

## Results

These are from my local run on this machine. The absolute values are host-dependent; the shape and the comparison against `Δ_block = 400 ms` are the point.

### Part A: real Agave BankingStage baseline

Command:

```bash
cd banking-jitter
cargo test real_banking_stage_baseline_stays_inside_delta_block -- --nocapture
```

Output:

```text
Agave BankingStage jitter mode: None
samples: 24
Votor Δ_block: 400 ms
Votor Δ_timeout: 1200 ms
Timeout(slot + 0): 1600 ms
Timeout(slot + 1): 2000 ms
Timeout(slot + 2): 2400 ms
Timeout(slot + 3): 2800 ms
latency p50=0.100ms p90=0.165ms p99=12.199ms p999=12.199ms max=12.199ms
p99 headroom vs Δ_block: 388 ms
```

Reading: the real BankingStage baseline p99 was `12.199 ms`, which is far below the `400 ms` production budget. The p99 headroom was `388 ms`, so this controlled baseline does not threaten Votor's slot budget.

### Part A: real Agave stress modes

Command:

```bash
cd banking-jitter
cargo test -- --ignored --nocapture
```

| Mode | p50 | p90 | p99 / max | p99 headroom vs `Δ_block` | Reading |
| --- | ---: | ---: | ---: | ---: | --- |
| CPU pressure | `1.225 ms` | `3.893 ms` | `83.135 ms` | `317 ms` | external CPU contention adds tail latency |
| Allocator churn | `1.363 ms` | `3.065 ms` | `86.655 ms` | `314 ms` | memory churn adds a similar tail on this host |
| I/O-style stalls | `0.762 ms` | `3.191 ms` | `66.687 ms` | `334 ms` | blocking pauses between submissions add tail, but still below budget |

Reading: all stress modes stayed below `Δ_block = 400 ms` in this run, but they made the tail much larger than baseline. That is exactly the diagnostic signal the assignment is asking for.

### Part B: Tokio-console model stats

Commands:

```bash
RUSTFLAGS="--cfg tokio_unstable" cargo run -- --jitter none --duration-secs 5
RUSTFLAGS="--cfg tokio_unstable" cargo run -- --jitter cpu --duration-secs 5
RUSTFLAGS="--cfg tokio_unstable" cargo run -- --jitter io --duration-secs 5
RUSTFLAGS="--cfg tokio_unstable" cargo run -- --jitter alloc --duration-secs 5
```

| Mode | Schedule delay p99 | Execute p50 | Execute p99 / max | End-to-end p99 / max | Reading |
| --- | ---: | ---: | ---: | ---: | --- |
| none | `16.055 ms` | `0.191 ms` | `0.331 / 0.473 ms` | `0.806 / 2.389 ms` | baseline |
| cpu | `81.151 ms` | `0.161 ms` | `0.180 / 0.184 ms` | `0.364 / 0.396 ms` | scheduling delay rises while work stays flat |
| io | `24.431 ms` | `0.163 ms` | `12.719 / 14.175 ms` | `12.839 / 14.303 ms` | blocking call inflates execute and end-to-end tails |
| alloc | `13.687 ms` | `0.743 ms` | `0.950 / 4.847 ms` | `1.153 / 5.155 ms` | allocator churn raises execution work |

Reading: CPU pressure is mostly a scheduling problem: tasks are ready but wait longer to run. The I/O mode is different: the worker itself spends longer executing because it blocks a runtime thread. Allocator churn raises the work done per batch, visible as higher execute time rather than just schedule delay.

## What I found

The controlled real Agave baseline is comfortably inside Votor's `400 ms` block-production budget. The baseline p99 was `12.199 ms`, leaving `388 ms` of p99 headroom.

The stress modes demonstrate that the tail can grow quickly under pressure, even with tiny single-transfer batches. CPU pressure, allocator churn, and blocking-style pauses all pushed the real-system p99 into the `66-87 ms` range on this host. That is still below `400 ms`, but it is large enough to show why tail latency matters more than the median.

The Tokio model gives the clearest cause diagnosis. CPU contention shows up as high schedule delay with flat execute time. Blocking I/O shows up as a long execute tail. Allocator churn raises execute cost and can also create occasional end-to-end spikes.

## Diagnostic guide

When debugging latency output, use this ranking:

1. **Schedule delay high, execute flat** → CPU scheduling / oversubscription. Tasks are ready but waiting to run.
2. **Execute has a long tail** → blocking work inside a task, synchronous I/O, or oversized per-batch work.
3. **Execute time rises with allocation** → allocator churn or memory pressure.
4. **Real Agave p99 headroom shrinking toward zero** → BankingStage is close to the `Δ_block` budget.

The real harness uses small transfer batches. It proves the measurement path and compares controlled latency to the Votor budget, but it is not a full validator benchmark under production load.

The real harness measures send-to-record wall-clock latency through `BankingStage`; it does not expose every internal `Consumer` sub-phase as a separate table.

The Tokio lab is not Agave. It exists because `tokio-console` observes Tokio tasks, while real BankingStage workers are mostly OS threads. Its absolute numbers are illustrative; the jitter signatures are the useful part.
