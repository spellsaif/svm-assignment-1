use clap::ValueEnum;
use std::{hint::black_box, time::Duration};

#[derive(Debug, Copy, Clone, ValueEnum, Eq, PartialEq)]
pub enum JitterMode {
    None,
    Cpu,
    Io,
    Alloc,
}

pub async fn inject(mode: JitterMode, batch_id: u64, io_stall: Duration, alloc_bytes: usize) {
    match mode {
        JitterMode::None => {}
        JitterMode::Cpu => {
            // CPU jitter comes from hog tasks spawned by pipeline.rs.
        }
        JitterMode::Io if batch_id % 8 == 0 => {
            // Deliberately bad: blocks a Tokio worker thread so tokio-console shows the stall.
            std::thread::sleep(io_stall);
        }
        JitterMode::Alloc => {
            let mut v = vec![0_u8; alloc_bytes];
            for (i, b) in v.iter_mut().enumerate().step_by(4096) {
                *b = (i as u8).wrapping_add(batch_id as u8);
            }
            black_box(v);
        }
        _ => {}
    }
}
