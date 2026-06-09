//! Measurement support for observing Agave's real BankingStage against Votor's
//! timing budget.
//!
//! The root crate is a Tokio task-jitter lab. This crate deliberately points at
//! a sibling Agave checkout and measures `agave/core/src/banking_stage.rs` as the
//! real system under observation.

use votor_budget::{
    VotorBudget, BLOCK_MS as DELTA_BLOCK_MS, SYNCHRONY_BOUND_MS, TIMEOUT_MS as DELTA_TIMEOUT_MS,
    LEADER_WINDOW_SLOTS,
};

#[cfg(test)]
mod tests {
    use {
        super::*,
        agave_banking_stage_ingress_types::BankingPacketBatch,
        crossbeam_channel::unbounded,
        hdrhistogram::Histogram,
        solana_core::{
            banking_stage::{
                BankingStage,
                transaction_scheduler::scheduler_controller::SchedulerConfig,
            },
            banking_trace::{BankingTracer, Channels},
            validator::{BlockProductionMethod, SchedulerPacing},
        },
        solana_entry::entry_or_marker::EntryOrMarker,
        solana_keypair::Keypair,
        solana_ledger::{blockstore::Blockstore, genesis_utils::create_genesis_config},
        solana_perf::packet::to_packet_batches,
        solana_poh::poh_recorder::create_test_recorder,
        solana_pubkey::Pubkey,
        solana_runtime::bank::Bank,
        solana_system_transaction as system_transaction,
        std::{
            hint::black_box,
            path::Path,
            sync::{
                Arc,
                atomic::{AtomicBool, Ordering},
            },
            thread::{self, JoinHandle, sleep},
            time::{Duration, Instant},
        },
    };

    const SAMPLE_BATCHES: usize = 24;
    const ENTRY_WAIT_TIMEOUT: Duration = Duration::from_secs(10);

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum JitterMode {
        None,
        Cpu,
        Io,
        Alloc,
    }

    struct JitterGuard {
        exit: Arc<AtomicBool>,
        threads: Vec<JoinHandle<()>>,
    }

    impl JitterGuard {
        fn new(mode: JitterMode) -> Self {
            let exit = Arc::new(AtomicBool::new(false));
            let mut threads = Vec::new();
            if mode == JitterMode::Cpu {
                // CPU pressure is external to BankingStage. It competes for cores so
                // latency growth is attributable to OS scheduling contention.
                let thread_count = thread::available_parallelism()
                    .map(|n| n.get().saturating_sub(1).max(1))
                    .unwrap_or(2)
                    .min(8);
                for id in 0..thread_count {
                    let exit = exit.clone();
                    threads.push(
                        thread::Builder::new()
                            .name(format!("jitter-cpu-{id}"))
                            .spawn(move || {
                                let mut x = id as u64 + 1;
                                while !exit.load(Ordering::Relaxed) {
                                    for _ in 0..1_000_000 {
                                        x = x.rotate_left(11).wrapping_mul(6364136223846793005);
                                        black_box(x);
                                    }
                                    thread::yield_now();
                                }
                            })
                            .unwrap(),
                    );
                }
            }
            Self { exit, threads }
        }

        fn before_send(&self, mode: JitterMode, sample: usize) {
            match mode {
                JitterMode::None | JitterMode::Cpu => {}
                // This models a blocking pause near the producer side. The pause is
                // outside Agave, but its tail effect is visible in the end-to-end run.
                JitterMode::Io if sample % 4 == 0 => sleep(Duration::from_millis(15)),
                JitterMode::Alloc => {
                    // Touch each page so the allocation becomes real memory work rather
                    // than just reserving virtual address space.
                    let mut bytes = vec![0_u8; 32 * 1024 * 1024];
                    for (idx, byte) in bytes.iter_mut().enumerate().step_by(4096) {
                        *byte = idx.wrapping_add(sample) as u8;
                    }
                    black_box(bytes);
                }
                JitterMode::Io => {}
            }
        }
    }

    impl Drop for JitterGuard {
        fn drop(&mut self) {
            self.exit.store(true, Ordering::Relaxed);
            for thread in self.threads.drain(..) {
                thread.join().unwrap();
            }
        }
    }

    struct HarnessResult {
        mode: JitterMode,
        histogram: Histogram<u64>,
        samples: usize,
    }

    impl HarnessResult {
        fn print(&self) {
            let budget = VotorBudget::default();
            println!("\nAgave BankingStage jitter mode: {:?}", self.mode);
            println!("samples: {}", self.samples);
            println!("Votor Δ_block: {} ms", budget.block_ms);
            println!("Votor Δ_timeout: {} ms", budget.timeout_ms);
            for offset in 0..budget.leader_window_slots {
                println!(
                    "Timeout(slot + {offset}): {} ms",
                    budget.slot_timeout_ms(offset)
                );
            }
            println!(
                "latency p50={:.3}ms p90={:.3}ms p99={:.3}ms p999={:.3}ms max={:.3}ms",
                self.histogram.value_at_quantile(0.50) as f64 / 1000.0,
                self.histogram.value_at_quantile(0.90) as f64 / 1000.0,
                self.histogram.value_at_quantile(0.99) as f64 / 1000.0,
                self.histogram.value_at_quantile(0.999) as f64 / 1000.0,
                self.histogram.max() as f64 / 1000.0,
            );
            let p99_ms = self.histogram.value_at_quantile(0.99) / 1000;
            println!(
                "p99 headroom vs Δ_block: {} ms",
                budget.delta_block_ms.saturating_sub(p99_ms)
            );
        }
    }

    #[test]
    fn votor_budget_math() {
        let budget = VotorBudget::default();
        assert_eq!(budget.delta_timeout_ms, 3 * budget.synchrony_bound_ms);
        assert_eq!(budget.slot_timeout_ms(0), 1600);
        assert_eq!(budget.slot_timeout_ms(1), 2000);
        assert_eq!(budget.slot_timeout_ms(2), 2400);
        assert_eq!(budget.slot_timeout_ms(3), 2800);
    }

    #[test]
    fn agave_source_path_is_expected() {
        let path = Path::new("../../agave/core/src/banking_stage.rs");
        assert!(
            path.exists(),
            "missing {path:?}; clone Agave at ../../agave relative to banking-jitter/"
        );
        let src = std::fs::read_to_string(path).expect("read banking_stage.rs");
        assert!(src.contains("pub struct BankingStage"));
        assert!(src.contains("pub fn new_num_threads"));
        assert!(src.contains("spawn_internal_central"));
        assert!(src.contains("Consumer::new"));
    }

    #[test]
    fn real_banking_stage_baseline_stays_inside_delta_block() {
        agave_logger::setup();
        let result = measure_real_banking_stage(JitterMode::None, SAMPLE_BATCHES);
        result.print();
        assert!(
            result.histogram.value_at_quantile(0.99) < DELTA_BLOCK_MS * 1000,
            "baseline p99 exceeded Votor Δ_block={}ms",
            DELTA_BLOCK_MS
        );
    }

    #[test]
    #[ignore = "stress mode: demonstrates CPU scheduling jitter"]
    fn real_banking_stage_under_cpu_pressure() {
        agave_logger::setup();
        measure_real_banking_stage(JitterMode::Cpu, SAMPLE_BATCHES).print();
    }

    #[test]
    #[ignore = "stress mode: demonstrates blocking I/O-style stalls"]
    fn real_banking_stage_with_io_stalls_between_submissions() {
        agave_logger::setup();
        measure_real_banking_stage(JitterMode::Io, SAMPLE_BATCHES).print();
    }

    #[test]
    #[ignore = "stress mode: demonstrates allocator churn"]
    fn real_banking_stage_with_allocator_churn() {
        agave_logger::setup();
        measure_real_banking_stage(JitterMode::Alloc, SAMPLE_BATCHES).print();
    }

    fn measure_real_banking_stage(mode: JitterMode, samples: usize) -> HarnessResult {
        // This is the actual measurement boundary: everything after startup uses
        // Agave's real BankingStage, channels, packet conversion, workers, and PoH
        // record receiver. The measured interval is send -> recorded entry.
        let GenesisHarness {
            mint_keypair,
            start_hash,
            channels,
            exit,
            banking_stage,
            poh_service,
            entry_receiver,
            _ledger_dir,
        } = start_banking_stage();

        let jitter = JitterGuard::new(mode);
        let mut histogram = Histogram::<u64>::new(3).unwrap();

        for sample in 0..samples {
            jitter.before_send(mode, sample);
            let recipient = Pubkey::new_unique();
            let tx = system_transaction::transfer(&mint_keypair, &recipient, 1, start_hash);
            let packet_batches = to_packet_batches(&[tx], 1);
            assert_eq!(packet_batches.len(), 1);

            // Start timing immediately before the packet enters Agave's non-vote
            // BankingStage channel. Stop timing when PoH records a transaction entry.
            let sent_at = Instant::now();
            channels
                .non_vote_sender
                .send(Arc::new(packet_batches))
                .expect("send packet batch to BankingStage");
            wait_for_transaction_entry(&entry_receiver, sent_at, &mut histogram);
        }

        drop(channels.non_vote_sender);
        drop(channels.tpu_vote_sender);
        drop(channels.gossip_vote_sender);
        banking_stage.join().unwrap();
        exit.store(true, Ordering::Relaxed);
        poh_service.join().unwrap();

        HarnessResult {
            mode,
            histogram,
            samples,
        }
    }

    struct GenesisHarness {
        mint_keypair: Keypair,
        start_hash: solana_hash::Hash,
        channels: Channels,
        exit: Arc<AtomicBool>,
        banking_stage: solana_core::banking_stage::BankingStageHandle,
        poh_service: solana_poh::poh_service::PohService,
        entry_receiver: crossbeam_channel::Receiver<solana_poh::poh_recorder::WorkingBankEntryOrMarker>,
        _ledger_dir: tempfile::TempDir,
    }

    fn start_banking_stage() -> GenesisHarness {
        // Build the same minimal test environment Agave's own BankingStage tests use:
        // a Bank, BankForks, Blockstore, PohRecorder, and BankingTracer channels.
        let genesis_config_info = create_genesis_config(1_000_000_000);
        let mint_keypair = genesis_config_info.mint_keypair;
        let (bank, bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config_info.genesis_config);
        let start_hash = bank.last_blockhash();

        let banking_tracer = BankingTracer::new_disabled();
        let channels = banking_tracer.create_channels();

        let ledger_dir = tempfile::tempdir().expect("create temporary ledger directory");
        let blockstore = Arc::new(Blockstore::open(ledger_dir.path()).expect("open blockstore"));
        let (exit, poh_recorder, _poh_controller, transaction_recorder, poh_service, entry_receiver) =
            create_test_recorder(bank, blockstore, None, None);
        let (replay_vote_sender, _replay_vote_receiver) = unbounded();

        // This is the real system under observation. The scheduler and consume
        // workers are spawned from Agave's core/src/banking_stage.rs.
        let banking_stage = BankingStage::new_num_threads(
            BlockProductionMethod::CentralSchedulerGreedy,
            poh_recorder,
            transaction_recorder,
            channels.non_vote_receiver.clone(),
            channels.tpu_vote_receiver.clone(),
            channels.gossip_vote_receiver.clone(),
            tokio::sync::mpsc::channel(1).1,
            BankingStage::default_num_workers(),
            SchedulerConfig {
                scheduler_pacing: SchedulerPacing::Disabled,
            },
            None,
            replay_vote_sender,
            None,
            bank_forks,
            None,
            Arc::default(),
        );

        GenesisHarness {
            mint_keypair,
            start_hash,
            channels,
            exit,
            banking_stage,
            poh_service,
            entry_receiver,
            _ledger_dir: ledger_dir,
        }
    }

    fn wait_for_transaction_entry(
        entry_receiver: &crossbeam_channel::Receiver<solana_poh::poh_recorder::WorkingBankEntryOrMarker>,
        sent_at: Instant,
        histogram: &mut Histogram<u64>,
    ) {
        let deadline = Instant::now() + ENTRY_WAIT_TIMEOUT;
        loop {
            let now = Instant::now();
            assert!(now < deadline, "timed out waiting for BankingStage output");
            let remaining = deadline.saturating_duration_since(now);
            let (_bank, (entry_or_marker, _tick_height)) = entry_receiver
                .recv_timeout(remaining.min(Duration::from_millis(100)))
                .expect("receive entry from PohRecorder");
            if let EntryOrMarker::Entry(entry) = entry_or_marker {
                if !entry.transactions.is_empty() {
                    // Ignore tick-only entries. A non-empty entry means the submitted
                    // transaction made it through BankingStage and was recorded to PoH.
                    let micros = sent_at.elapsed().as_micros().min(u64::MAX as u128) as u64;
                    histogram.record(micros).unwrap();
                    return;
                }
            }
        }
    }

    #[allow(dead_code)]
    fn _assert_packet_batch_type(_: BankingPacketBatch) {}
}
 {}
}
