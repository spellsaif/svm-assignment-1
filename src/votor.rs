pub use votor_budget::VotorBudget;

pub fn budget_banner() -> String {
    VotorBudget::default().banner()
}
