use hdrhistogram::Histogram;
use std::time::Duration;

pub struct Metrics {
    pub schedule_delay: Histogram<u64>,
    pub execute: Histogram<u64>,
    pub end_to_end: Histogram<u64>,
    pub count: u64,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            schedule_delay: Histogram::new(3).unwrap(),
            execute: Histogram::new(3).unwrap(),
            end_to_end: Histogram::new(3).unwrap(),
            count: 0,
        }
    }

    pub fn record(&mut self, schedule_delay: Duration, execute: Duration, end_to_end: Duration) {
        let micros = |d: Duration| d.as_micros().min(u64::MAX as u128) as u64;
        self.schedule_delay.record(micros(schedule_delay)).ok();
        self.execute.record(micros(execute)).ok();
        self.end_to_end.record(micros(end_to_end)).ok();
        self.count += 1;
    }

    pub fn print(&self) {
        println!("\nsummary over {} batches", self.count);
        print_hist("schedule_delay", &self.schedule_delay);
        print_hist("execute", &self.execute);
        print_hist("end_to_end", &self.end_to_end);
    }
}

fn print_hist(name: &str, h: &Histogram<u64>) {
    println!(
        "{name:16} p50={:>8.3}ms p99={:>8.3}ms p999={:>8.3}ms max={:>8.3}ms",
        h.value_at_quantile(0.50) as f64 / 1000.0,
        h.value_at_quantile(0.99) as f64 / 1000.0,
        h.value_at_quantile(0.999) as f64 / 1000.0,
        h.max() as f64 / 1000.0,
    );
}
