//! Bounded-LRU `cl_ord_id → (pair, sent_at)` index. Populated at add-order
//! accept; looked up at amend/cancel to charge the age-decaying trading cost.
//! Sync inserts/lookups under a `Mutex`; LRU eviction is silent (no event).

use std::sync::Mutex;

use lru::LruCache;

use crate::types::{ClOrdId, MonotonicInstant, Symbol};

#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub pair: Symbol,
    pub sent_at: MonotonicInstant,
}

/// Bounded-LRU `ClOrdId → IndexEntry` index. `cap` is a construction-only knob
/// (`cl_ord_id_index_cap`, default 1024); eviction on overflow is silent (no
/// event emitted). One `Mutex` over the cache so insert+evict is atomic.
pub struct ClOrdIdPairIndex {
    /// Unbounded cache + manual cap: avoids `LruCache::new(cap)` preallocating an unvalidated knob.
    inner: Mutex<LruCache<ClOrdId, IndexEntry>>,
    cap: usize,
}

impl ClOrdIdPairIndex {
    /// Construct with the given LRU capacity, clamped to `>= 1` (a cap of 0
    /// would violate the `len <= cap` post-condition; it never grows unbounded).
    pub fn new(cap: usize) -> Self {
        Self {
            inner: Mutex::new(LruCache::unbounded()),
            cap: cap.max(1),
        }
    }

    /// Insert or refresh an entry for `cl_ord_id` (refresh also promotes to
    /// MRU). If the index is at cap, the LRU-oldest entry is evicted silently
    /// (no event emitted).
    pub fn insert(&self, cl_ord_id: ClOrdId, pair: Symbol, sent_at: MonotonicInstant) {
        let mut inner = self.inner.lock().expect("cl_ord_id_index lock poisoned");
        inner.put(cl_ord_id, IndexEntry { pair, sent_at });
        // Refresh doesn't grow len; only genuine growth past cap evicts (MRU is safe).
        if inner.len() > self.cap {
            inner.pop_lru();
        }
    }

    /// Look up `cl_ord_id`, promote to MRU, return an owned clone (lock released before any await).
    pub fn lookup_and_promote(&self, cl_ord_id: &ClOrdId) -> Option<IndexEntry> {
        self.inner
            .lock()
            .expect("cl_ord_id_index lock poisoned")
            .get(cl_ord_id)
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn id(s: &str) -> ClOrdId {
        ClOrdId::new(s).unwrap()
    }

    fn sym(s: &str) -> Symbol {
        Symbol::new(s).unwrap()
    }

    fn t(secs: u64) -> MonotonicInstant {
        MonotonicInstant(Duration::from_secs(secs))
    }

    #[test]
    fn insert_and_lookup_hit() {
        let idx = ClOrdIdPairIndex::new(8);
        idx.insert(id("abc"), sym("BTC/USD"), t(10));
        let entry = idx.lookup_and_promote(&id("abc")).expect("hit");
        assert_eq!(entry.pair.as_str(), "BTC/USD");
        assert_eq!(entry.sent_at, t(10));
    }

    #[test]
    fn lookup_miss_returns_none() {
        let idx = ClOrdIdPairIndex::new(8);
        assert!(idx.lookup_and_promote(&id("nosuchid")).is_none());
    }

    #[test]
    fn evict_at_cap_removes_oldest() {
        let idx = ClOrdIdPairIndex::new(2);
        idx.insert(id("first"), sym("BTC/USD"), t(1));
        idx.insert(id("second"), sym("ETH/USD"), t(2));
        idx.insert(id("third"), sym("SOL/USD"), t(3));
        assert!(
            idx.lookup_and_promote(&id("first")).is_none(),
            "oldest entry must have been evicted at cap"
        );
        assert!(idx.lookup_and_promote(&id("second")).is_some());
        assert!(idx.lookup_and_promote(&id("third")).is_some());
    }

    #[test]
    fn cap_zero_is_clamped_to_one() {
        let idx = ClOrdIdPairIndex::new(0);
        idx.insert(id("a"), sym("BTC/USD"), t(1));
        idx.insert(id("b"), sym("ETH/USD"), t(2));
        idx.insert(id("c"), sym("SOL/USD"), t(3));
        assert!(idx.lookup_and_promote(&id("a")).is_none());
        assert!(idx.lookup_and_promote(&id("b")).is_none());
        assert!(idx.lookup_and_promote(&id("c")).is_some());
    }

    #[test]
    fn promote_changes_eviction_order() {
        let idx = ClOrdIdPairIndex::new(2);
        idx.insert(id("a"), sym("BTC/USD"), t(1));
        idx.insert(id("b"), sym("ETH/USD"), t(2));
        let _ = idx.lookup_and_promote(&id("a"));
        idx.insert(id("c"), sym("SOL/USD"), t(3));
        assert!(
            idx.lookup_and_promote(&id("b")).is_none(),
            "B must be evicted (it was LRU after A was promoted)"
        );
        assert!(idx.lookup_and_promote(&id("a")).is_some());
        assert!(idx.lookup_and_promote(&id("c")).is_some());
    }

    #[test]
    fn refresh_existing_key_updates_value_without_evicting() {
        let idx = ClOrdIdPairIndex::new(2);
        idx.insert(id("a"), sym("BTC/USD"), t(1));
        idx.insert(id("b"), sym("ETH/USD"), t(2));
        idx.insert(id("a"), sym("BTC/USD"), t(9));
        let entry = idx.lookup_and_promote(&id("a")).expect("refreshed entry");
        assert_eq!(entry.sent_at, t(9));
        assert!(
            idx.lookup_and_promote(&id("b")).is_some(),
            "refreshing an existing key must not evict"
        );
    }

    #[test]
    fn refresh_promotes_to_mru() {
        let idx = ClOrdIdPairIndex::new(2);
        idx.insert(id("a"), sym("BTC/USD"), t(1));
        idx.insert(id("b"), sym("ETH/USD"), t(2));
        idx.insert(id("a"), sym("BTC/USD"), t(3));
        idx.insert(id("c"), sym("SOL/USD"), t(4));
        assert!(
            idx.lookup_and_promote(&id("b")).is_none(),
            "B must be evicted (refreshing A promoted it to MRU)"
        );
        assert!(idx.lookup_and_promote(&id("a")).is_some());
        assert!(idx.lookup_and_promote(&id("c")).is_some());
    }

    #[test]
    fn huge_cap_allocates_lazily() {
        let idx = ClOrdIdPairIndex::new(usize::MAX);
        idx.insert(id("a"), sym("BTC/USD"), t(1));
        assert!(idx.lookup_and_promote(&id("a")).is_some());
    }
}
