# Votor timing extraction

The assignment's "Figure 2" reference is treated as the Chapter 2 Votor protocol material, not only the literal Figure 2 drawing. In the pasted paper, literal Figure 2 is the block/slice/shred Merkle hierarchy. The timing requirements used by this harness come from Section 2.6, Definition 17, Algorithm 2, and Table 10.

Definition 17 and Algorithm 2 specify the per-slot timeout schedule:

```text
Timeout(i) = clock() + Δ_timeout + (i - s + 1) · Δ_block
```

Section 2.6 describes the conservative timeout slack as:

```text
Δ_timeout = 1Δ + 2Δ = 3Δ
```

Table 10 gives the protocol parameters used by the paper's preliminary simulations. Combined with Section 1.5's conservative synchrony bound, the constants used here are:

| Symbol | Value | Role |
| --- | ---: | --- |
| δ | 80 ms | one network round yardstick |
| Δ | 400 ms | conservative synchrony bound |
| Δ_block | 400 ms | block/slot production budget |
| Δ_timeout | 1200 ms | skip-vote timeout slack |
| first slot timeout | 1600 ms | Δ_timeout + Δ_block |
| leader window | 4 slots | Table 10 parameter `w` |

For a 4-slot leader window, Algorithm 2 schedules these per-round deadlines from the local `clock()` at `ParentReady`:

| Slot offset | Timeout |
| ---: | ---: |
| `i = s` | 1600 ms |
| `i = s + 1` | 2000 ms |
| `i = s + 2` | 2400 ms |
| `i = s + 3` | 2800 ms |

The fast-path latency target after block distribution is `min(δ80%, 2δ60%)`: one voting round with 80% stake, or two voting rounds with 60% stake, whichever arrives first.

The test question is therefore: does Agave's real `core/src/banking_stage.rs` introduce enough block-production jitter to threaten the 400 ms `Δ_block` budget or, in worse cases, the Votor timeout schedule?

## Source path traced in Agave

The real system under observation is:

```text
../agave/core/src/banking_stage.rs
```

The relevant flow in that file is:

```text
BankingStage::new_num_threads
  -> spawn_internal_central
  -> TransactionViewReceiveAndBuffer::receive_and_buffer_packets
  -> GreedyScheduler / SchedulerController
  -> ConsumeWorker::run
  -> Consumer::process_and_record_aged_transactions
  -> TransactionRecorder / PohRecorder record channel
```

The `banking-jitter` crate instantiates this path rather than replacing it with a mock.
