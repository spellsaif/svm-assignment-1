//! agave-votor-scope: a Tokio harness modeled on Agave's banking_stage.
//! It is for learning task jitter signatures with tokio-console. The sibling
//! banking-jitter crate is the part that path-depends on the real Agave checkout.

mod jitter;
mod metrics;
mod pipeline;
mod votor;
mod workload;

use clap::Parser;
use jitter::JitterMode;
use std::time::Duration;

#[derive(Parser)]
#[command(
    name = "agave-votor-scope",
    about = "Tokio banking_stage jitter harness"
)]
struct Args {
    #[arg(long, default_value_t = 4)]
    workers: usize,
    #[arg(long, default_value_t = workload::TARGET_NUM_TRANSACTIONS_PER_BATCH)]
    batch_size: usize,
    #[arg(long, default_value_t = 2)]
    batch_interval_ms: u64,
    #[arg(long, default_value_t = 10)]
    duration_secs: u64,
    #[arg(long, default_value_t = 200)]
    hash_rounds: u32,
    #[arg(long, value_enum, default_value = "none")]
    jitter: JitterMode,
    #[arg(long, default_value_t = 8)]
    hogs: usize,
    #[arg(long, default_value_t = 4)]
    runtime_threads: usize,
    #[arg(long, default_value_t = 10)]
    io_stall_ms: u64,
    #[arg(long, default_value_t = 64)]
    alloc_mb: usize,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    console_subscriber::init();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.runtime_threads)
        .enable_all()
        .build()?;
    runtime.block_on(run_harness(args));
    Ok(())
}

async fn run_harness(args: Args) {
    let cfg = pipeline::Config {
        workers: args.workers,
        batch_size: args.batch_size,
        batch_interval: Duration::from_millis(args.batch_interval_ms),
        duration: Duration::from_secs(args.duration_secs),
        hash_rounds: args.hash_rounds,
        jitter: args.jitter,
        hogs: args.hogs,
        io_stall: Duration::from_millis(args.io_stall_ms),
        alloc_bytes: args.alloc_mb * 1024 * 1024,
    };
    println!("agave-votor-scope: jitter mode = {:?}", args.jitter);
    println!("Attach `tokio-console` in another terminal to inspect task delay.\n");
    println!("{}", votor::budget_banner());
    let summary = pipeline::run(cfg).await;
    summary.print();
}
