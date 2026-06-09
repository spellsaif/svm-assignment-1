//! Votor timing budget constants and slot-timeout schedule.
//!
//! Extracted from the Alpenglow white paper: Definition 17, Algorithm 2, and Table 10.
//! Both the root Tokio lab and the banking-jitter harness share these values.

use std::fmt::Write;

/// Lowercase delta: rough one-network-round yardstick (80 ms).
pub const NETWORK_ROUND_MS: u64 = 80;
/// Uppercase Delta: conservative synchrony bound used for timeouts (400 ms).
pub const SYNCHRONY_BOUND_MS: u64 = 400;
/// Δ_block: per-slot block production budget (400 ms).
pub const BLOCK_MS: u64 = 400;
/// Δ_timeout = 3Δ: skip-vote timeout slack (1200 ms).
pub const TIMEOUT_MS: u64 = 3 * SYNCHRONY_BOUND_MS;
/// Table 10: leader window size (4 slots).
pub const LEADER_WINDOW_SLOTS: u64 = 4;

/// Values used by both harnesses. Explicit constants from the paper's Votor
/// timing requirements so we measure against stated guarantees, not arbitrary latency.
#[derive(Debug, Clone, Copy)]
pub struct VotorBudget {
    /// Lowercase delta: rough one-network-round yardstick.
    pub network_round_ms: u64,
    /// Uppercase Delta: conservative synchrony bound used for timeouts.
    pub synchrony_bound_ms: u64,
    /// Δ_block: block production budget per slot.
    pub block_ms: u64,
    /// Δ_timeout = 3Δ.
    pub timeout_ms: u64,
    /// First-slot Votor timeout = Δ_timeout + Δ_block.
    pub first_slot_timeout_ms: u64,
    /// Number of slots in the leader window (Table 10 parameter w).
    pub leader_window_slots: u64,
}

impl Default for VotorBudget {
    fn default() -> Self {
        Self {
            network_round_ms: NETWORK_ROUND_MS,
            synchrony_bound_ms: SYNCHRONY_BOUND_MS,
            block_ms: BLOCK_MS,
            timeout_ms: TIMEOUT_MS,
            first_slot_timeout_ms: TIMEOUT_MS + BLOCK_MS,
            leader_window_slots: LEADER_WINDOW_SLOTS,
        }
    }
}

impl VotorBudget {
    /// Definition 17 / Algorithm 2:
    /// Timeout(i) = clock() + Δ_timeout + (i - s + 1) · Δ_block
    pub fn slot_timeout_ms(&self, slot_offset: u64) -> u64 {
        self.timeout_ms + (slot_offset + 1) * self.block_ms
    }

    /// Returns a human-readable banner with the full budget table.
    pub fn banner(&self) -> String {
        let mut out = String::new();
        writeln!(&mut out, "Votor timing budget").unwrap();
        writeln!(
            &mut out,
            "  δ  network round yardstick: {} ms",
            self.network_round_ms
        )
        .unwrap();
        writeln!(
            &mut out,
            "  Δ  synchrony bound:        {} ms",
            self.synchrony_bound_ms
        )
        .unwrap();
        writeln!(&mut out, "  Δ_block:                  {} ms", self.block_ms).unwrap();
        writeln!(
            &mut out,
            "  Δ_timeout = 3Δ:           {} ms",
            self.timeout_ms
        )
        .unwrap();
        writeln!(
            &mut out,
            "  Timeout(first slot):      {} ms",
            self.first_slot_timeout_ms
        )
        .unwrap();
        writeln!(
            &mut out,
            "  leader window slots:      {}",
            self.leader_window_slots
        )
        .unwrap();
        for offset in 0..self.leader_window_slots {
            writeln!(
                &mut out,
                "  Timeout(slot + {offset}):      {} ms",
                self.slot_timeout_ms(offset)
            )
            .unwrap();
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeout_math_matches_votor_definition() {
        let b = VotorBudget::default();
        assert_eq!(b.timeout_ms, 3 * b.synchrony_bound_ms);
        assert_eq!(b.first_slot_timeout_ms, b.timeout_ms + b.block_ms);
    }

    #[test]
    fn leader_window_timeout_schedule_matches_algorithm_2() {
        let b = VotorBudget::default();
        let timeouts: Vec<_> = (0..b.leader_window_slots)
            .map(|offset| b.slot_timeout_ms(offset))
            .collect();
        assert_eq!(timeouts, vec![1600, 2000, 2400, 2800]);
    }

    #[test]
    fn constants_match_default() {
        let b = VotorBudget::default();
        assert_eq!(b.network_round_ms, NETWORK_ROUND_MS);
        assert_eq!(b.synchrony_bound_ms, SYNCHRONY_BOUND_MS);
        assert_eq!(b.block_ms, BLOCK_MS);
        assert_eq!(b.timeout_ms, TIMEOUT_MS);
        assert_eq!(b.leader_window_slots, LEADER_WINDOW_SLOTS);
    }
}
