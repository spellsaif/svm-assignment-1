use std::hint::black_box;

pub const TARGET_NUM_TRANSACTIONS_PER_BATCH: usize = 128;

pub fn execute_fake_batch(batch_size: usize, hash_rounds: u32) -> u64 {
    let mut acc = 0xcbf29ce484222325_u64;
    for tx in 0..batch_size as u64 {
        let mut x = tx ^ acc;
        for _ in 0..hash_rounds {
            x = x.rotate_left(13) ^ 0x9e3779b97f4a7c15_u64;
            x = x.wrapping_mul(0xbf58476d1ce4e5b9_u64);
        }
        acc ^= x;
    }
    black_box(acc)
}
