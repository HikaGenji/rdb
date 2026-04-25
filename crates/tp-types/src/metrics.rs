//! Sliding-window latency tracker.
//!
//! Each hop in the pipeline (feed-replayer publish, tickerplant ingest,
//! tickerplant publish, rdb append) records a one-shot `record(ns)` into
//! its own [`LatencyHistogram`]. We keep a fixed-capacity ring of the most
//! recent samples and recompute percentiles on demand by sorting a copy.
//!
//! This is the simplest histogram that does the job for a prototype. If we
//! ever care about microsecond-grain p99.9 we can swap in HdrHistogram.

use std::sync::Mutex;

pub struct LatencyHistogram {
    inner: Mutex<Inner>,
}

struct Inner {
    capacity: usize,
    samples: Vec<u64>,
    next: usize,
    total: u64,
    dropped: u64,
}

impl LatencyHistogram {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                capacity,
                samples: Vec::with_capacity(capacity),
                next: 0,
                total: 0,
                dropped: 0,
            }),
        }
    }

    pub fn record(&self, ns: u64) {
        let mut g = self.inner.lock().unwrap();
        g.total = g.total.saturating_add(1);
        if g.samples.len() < g.capacity {
            g.samples.push(ns);
        } else {
            let i = g.next % g.capacity;
            g.samples[i] = ns;
            g.next = g.next.wrapping_add(1);
        }
    }

    pub fn record_drop(&self) {
        let mut g = self.inner.lock().unwrap();
        g.dropped = g.dropped.saturating_add(1);
    }

    pub fn snapshot(&self) -> LatencySnapshot {
        let g = self.inner.lock().unwrap();
        let mut copy = g.samples.clone();
        copy.sort_unstable();
        let n = copy.len();
        let pick = |q: f64| -> Option<u64> {
            if n == 0 { return None; }
            let idx = ((n - 1) as f64 * q).round() as usize;
            Some(copy[idx])
        };
        LatencySnapshot {
            samples: n,
            total: g.total,
            dropped: g.dropped,
            min: copy.first().copied(),
            p50: pick(0.50),
            p99: pick(0.99),
            max: copy.last().copied(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LatencySnapshot {
    pub samples: usize,
    pub total: u64,
    pub dropped: u64,
    pub min: Option<u64>,
    pub p50: Option<u64>,
    pub p99: Option<u64>,
    pub max: Option<u64>,
}
