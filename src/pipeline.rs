use crate::{jitter, jitter::JitterMode, metrics::Metrics, workload};
use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, Mutex};

#[derive(Clone)]
pub struct Config {
    pub workers: usize,
    pub batch_size: usize,
    pub batch_interval: Duration,
    pub duration: Duration,
    pub hash_rounds: u32,
    pub jitter: JitterMode,
    pub hogs: usize,
    pub io_stall: Duration,
    pub alloc_bytes: usize,
}

#[derive(Clone, Copy)]
struct Batch {
    id: u64,
    produced_at: Instant,
}

struct Done {
    produced_at: Instant,
    scheduled_at: Instant,
    started_at: Instant,
    finished_at: Instant,
}

pub async fn run(cfg: Config) -> Metrics {
    if cfg.jitter == JitterMode::Cpu {
        for n in 0..cfg.hogs {
            spawn_named("cpu-hog", async move {
                let mut x = n as u64 + 1;
                loop {
                    for _ in 0..2_000_000 {
                        x = x.rotate_left(7).wrapping_mul(6364136223846793005);
                        std::hint::black_box(x);
                    }
                    tokio::task::yield_now().await;
                }
            });
        }
    }

    let (producer_tx, mut producer_rx) = mpsc::channel::<Batch>(1024);
    let (worker_tx, worker_rx) = mpsc::channel::<Batch>(1024);
    let worker_rx = Arc::new(Mutex::new(worker_rx));
    let (done_tx, mut done_rx) = mpsc::channel::<Done>(1024);

    // Producer: synthetic packets entering BankingStage.
    let producer_cfg = cfg.clone();
    spawn_named("packet-producer", async move {
        let start = Instant::now();
        let mut id = 0;
        while start.elapsed() < producer_cfg.duration {
            let _ = producer_tx
                .send(Batch {
                    id,
                    produced_at: Instant::now(),
                })
                .await;
            id += 1;
            tokio::time::sleep(producer_cfg.batch_interval).await;
        }
    });

    // Scheduler: separate task so console can show ready/running delay.
    spawn_named("banking-scheduler", async move {
        while let Some(batch) = producer_rx.recv().await {
            if worker_tx.send(batch).await.is_err() {
                break;
            }
        }
    });

    // Workers: shaped like banking_stage workers.
    for worker_id in 0..cfg.workers {
        let rx = worker_rx.clone();
        let done_tx = done_tx.clone();
        let cfg = cfg.clone();
        spawn_named("banking-worker", async move {
            loop {
                let scheduled_at = Instant::now();
                let batch = {
                    let mut guard = rx.lock().await;
                    guard.recv().await
                };
                let Some(batch) = batch else { break };
                let started_at = Instant::now();
                jitter::inject(cfg.jitter, batch.id, cfg.io_stall, cfg.alloc_bytes).await;
                workload::execute_fake_batch(
                    cfg.batch_size,
                    cfg.hash_rounds + worker_id as u32 % 3,
                );
                let finished_at = Instant::now();
                let _ = done_tx
                    .send(Done {
                        produced_at: batch.produced_at,
                        scheduled_at,
                        started_at,
                        finished_at,
                    })
                    .await;
            }
        });
    }
    drop(done_tx);

    let stop = Instant::now() + cfg.duration + Duration::from_secs(2);
    let mut metrics = Metrics::new();
    while Instant::now() < stop {
        match tokio::time::timeout(Duration::from_millis(200), done_rx.recv()).await {
            Ok(Some(done)) => metrics.record(
                done.started_at.saturating_duration_since(done.scheduled_at),
                done.finished_at.saturating_duration_since(done.started_at),
                done.finished_at.saturating_duration_since(done.produced_at),
            ),
            Ok(None) => break,
            Err(_) => {}
        }
    }
    metrics
}

fn spawn_named<F>(name: &'static str, future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    // tokio-console shows task names only when Tokio is built with tokio_unstable.
    // Without that flag this still runs, but task attribution is less useful.
    #[cfg(tokio_unstable)]
    tokio::task::Builder::new()
        .name(name)
        .spawn(future)
        .expect("spawn named Tokio task");

    #[cfg(not(tokio_unstable))]
    {
        let _ = name;
        tokio::spawn(future);
    }
}
