// Latency histogram: each value lands in a power-of-two band split into linear
// sub-buckets, giving roughly 6.25% relative error from nanoseconds to seconds.

/// Resolution bits per band: 4 bits = 16 sub-buckets, so a value reads back within 1/16 (about 6.25%) of its true size.
const SUB_BITS: u32 = 4;
/// Sub-buckets per band: 2^SUB_BITS, which is 16.
const SUB_BUCKETS: u64 = 1 << SUB_BITS;
/// Total buckets: 16 exact small-value buckets, plus 60 bands of 16, so 976.
const NUM_BUCKETS: usize =
    (SUB_BUCKETS as usize) + (64 - SUB_BITS as usize) * (SUB_BUCKETS as usize);

/// Fixed-size latency histogram in nanoseconds.
pub(crate) struct LatencyHistogram {
    counts: [u64; NUM_BUCKETS],
    total: u64,
    min: u64,
    max: u64,
}

impl LatencyHistogram {
    pub(crate) fn new() -> Self {
        Self {
            counts: [0; NUM_BUCKETS],
            total: 0,
            min: u64::MAX,
            max: 0,
        }
    }

    /// Which bucket a value falls in.
    #[inline]
    fn bucket_of(ns: u64) -> usize {
        let magnitude = 64 - ns.leading_zeros(); // bits needed to represent the value
        if magnitude <= SUB_BITS {
            return ns as usize; // small values: one exact bucket each
        }
        let band = (magnitude - SUB_BITS) as u64; // which power-of-two band
        let shift = magnitude - SUB_BITS - 1; // drop the bits below the kept sub-bucket bits
        let sub = (ns >> shift) & (SUB_BUCKETS - 1); // linear position within the band
        (SUB_BUCKETS * band + sub) as usize
    }

    /// Representative value (band midpoint) for a bucket.
    fn bucket_midpoint(bucket: usize) -> u64 {
        let bucket = bucket as u64;
        if bucket < SUB_BUCKETS {
            return bucket; // small-value block: the bucket index is the value
        }
        let band = bucket / SUB_BUCKETS;
        let sub = bucket % SUB_BUCKETS;
        let shift = (band - 1) as u32;
        let low = (SUB_BUCKETS + sub) << shift; // bucket's low edge
        let width = 1u64 << shift;
        low + width / 2 // midpoint keeps the readout unbiased
    }

    /// Record one latency sample (in ns): bucket it, bump the count, track min/max. No alloc, no lock.
    #[inline]
    pub(crate) fn record(&mut self, ns: u64) {
        self.counts[Self::bucket_of(ns)] += 1;
        self.total += 1;
        if ns < self.min {
            self.min = ns;
        }
        if ns > self.max {
            self.max = ns;
        }
    }

    pub(crate) fn count(&self) -> u64 {
        self.total
    }

    pub(crate) fn min(&self) -> u64 {
        if self.total == 0 { 0 } else { self.min }
    }

    pub(crate) fn max(&self) -> u64 {
        self.max
    }

    /// Percentile (p from 0 to 100): the bucket whose running count crosses the rank, taken at its midpoint.
    pub(crate) fn percentile(&self, p: f64) -> u64 {
        if self.total == 0 {
            return 0;
        }
        let rank = (p / 100.0 * self.total as f64).ceil() as u64; // 1-based
        let rank = rank.max(1);
        let mut cum = 0u64;
        for (bucket, &c) in self.counts.iter().enumerate() {
            cum += c;
            if cum >= rank {
                return Self::bucket_midpoint(bucket);
            }
        }
        self.max
    }

    /// One-line summary printed during tests: count, min, p50/p90/p99, max.
    pub(crate) fn report(&self, label: &str) -> String {
        format!(
            "{label:<24} n={:<6} min={:>9} p50={:>9} p90={:>9} p99={:>9} max={:>9}",
            self.count(),
            fmt_ns(self.min()),
            fmt_ns(self.percentile(50.0)),
            fmt_ns(self.percentile(90.0)),
            fmt_ns(self.percentile(99.0)),
            fmt_ns(self.max()),
        )
    }
}

/// Format nanoseconds into the most readable unit (ns, us, or ms).
fn fmt_ns(ns: u64) -> String {
    if ns >= 1_000_000 {
        format!("{:.2}ms", ns as f64 / 1_000_000.0)
    } else if ns >= 1_000 {
        format!("{:.2}us", ns as f64 / 1_000.0)
    } else {
        format!("{ns}ns")
    }
}

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) static REACTOR_SPAN: Mutex<Option<LatencyHistogram>> = Mutex::new(None);
pub(crate) static TYPED_DELIVERY_SPAN: Mutex<Option<LatencyHistogram>> = Mutex::new(None);
pub(crate) static DISPATCH_SPAN: Mutex<Option<LatencyHistogram>> = Mutex::new(None);
pub(crate) static ORDER_FORWARD_SPAN: Mutex<Option<LatencyHistogram>> = Mutex::new(None);

static DISPATCH_RECV_START: AtomicU64 = AtomicU64::new(0);

/// Monotonic ns stamp.
fn probe_now_ns() -> u64 {
    crate::types::MonotonicInstant::now()
        .as_duration()
        .as_nanos() as u64
}

/// Start recording: create a fresh histogram to collect into; before this, samples are ignored.
fn arm(slot: &Mutex<Option<LatencyHistogram>>) {
    *slot.lock().unwrap() = Some(LatencyHistogram::new());
}

/// Add one duration sample (nanoseconds) to the histogram, only while recording is on.
fn record_value(slot: &Mutex<Option<LatencyHistogram>>, ns: u64) {
    if let Some(h) = slot.lock().unwrap().as_mut() {
        h.record(ns);
    }
}

/// Record now minus the start time, only while recording is on; the end time is read here so the lock never adds to it.
fn record_since(slot: &Mutex<Option<LatencyHistogram>>, start: u64) {
    record_value(slot, probe_now_ns().saturating_sub(start));
}

/// Take the finished histogram and stop recording.
fn take(slot: &Mutex<Option<LatencyHistogram>>) -> Option<LatencyHistogram> {
    slot.lock().unwrap().take()
}

pub(crate) fn reactor_probe_arm() {
    arm(&REACTOR_SPAN);
}
/// Start of the reactor span: stamp taken at frame entry.
pub(crate) fn reactor_probe_start() -> u64 {
    probe_now_ns()
}
/// End of the reactor span: records the time since reactor_probe_start.
pub(crate) fn reactor_probe_end(start: u64) {
    record_since(&REACTOR_SPAN, start);
}
pub(crate) fn reactor_probe_take() -> Option<LatencyHistogram> {
    take(&REACTOR_SPAN)
}

pub(crate) fn typed_delivery_probe_arm() {
    arm(&TYPED_DELIVERY_SPAN);
}
/// Start of the typed-delivery span: stamp taken just before the maintained book is fanned to the Book handlers as a typed payload.
pub(crate) fn typed_delivery_probe_start() -> u64 {
    probe_now_ns()
}
/// End of the typed-delivery span: records the time since typed_delivery_probe_start.
pub(crate) fn typed_delivery_probe_end(start: u64) {
    record_since(&TYPED_DELIVERY_SPAN, start);
}
pub(crate) fn typed_delivery_probe_take() -> Option<LatencyHistogram> {
    take(&TYPED_DELIVERY_SPAN)
}

pub(crate) fn dispatch_probe_arm() {
    arm(&DISPATCH_SPAN);
}
/// Start of the dispatch span: the dispatch loop just took an item off the ring.
pub(crate) fn dispatch_probe_start_at_recv() {
    DISPATCH_RECV_START.store(probe_now_ns(), Ordering::Relaxed);
}
/// End of the dispatch span: still on the dispatch loop, just before invoke runs.
pub(crate) fn dispatch_probe_end_before_invoke() {
    record_since(&DISPATCH_SPAN, DISPATCH_RECV_START.load(Ordering::Relaxed));
}
pub(crate) fn dispatch_probe_take() -> Option<LatencyHistogram> {
    take(&DISPATCH_SPAN)
}

pub(crate) fn order_forward_probe_arm() {
    arm(&ORDER_FORWARD_SPAN);
}
/// Start of the order-forward span: stamp taken at the entry of handle_ws_request_frame.
pub(crate) fn order_forward_probe_start() -> u64 {
    probe_now_ns()
}
/// End of the order-forward span: records the reactor's work to turn one order into bytes, token inject through send_frame.
pub(crate) fn order_forward_probe_end(start: u64) {
    record_since(&ORDER_FORWARD_SPAN, start);
}
pub(crate) fn order_forward_probe_take() -> Option<LatencyHistogram> {
    take(&ORDER_FORWARD_SPAN)
}

#[cfg(test)]
mod proof {
    use super::*;

    fn error_bound() -> f64 {
        2f64.powi(-(SUB_BITS as i32))
    }

    #[test]
    fn every_bucket_round_trips() {
        for bucket in 0..NUM_BUCKETS {
            let midpoint = LatencyHistogram::bucket_midpoint(bucket);
            assert_eq!(
                LatencyHistogram::bucket_of(midpoint),
                bucket,
                "bucket {bucket} (midpoint {midpoint} ns) did not round-trip"
            );
        }
    }

    #[test]
    fn ramp_percentiles_within_error_bound() {
        let mut h = LatencyHistogram::new();
        let n = 1_000_000u64;
        for v in 1..=n {
            h.record(v);
        }
        for (p, exact) in [(50.0, n / 2), (90.0, n * 9 / 10), (99.0, n * 99 / 100)] {
            let got = h.percentile(p) as f64;
            let rel = (got - exact as f64).abs() / exact as f64;
            assert!(
                rel <= error_bound(),
                "p{p}: got {got} vs exact {exact}, rel {rel}"
            );
        }
    }

    #[test]
    fn count_min_max_are_exact() {
        let mut h = LatencyHistogram::new();
        for sample_ns in [1_000u64, 50_000, 13_000_000] {
            h.record(sample_ns);
        }
        assert_eq!(h.count(), 3);
        assert_eq!(h.min(), 1_000);
        assert_eq!(h.max(), 13_000_000);
    }
}
