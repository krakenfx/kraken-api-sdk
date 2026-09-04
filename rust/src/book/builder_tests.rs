//! Unit tests for the order-book builder.
use super::*;

fn lvl(price: &str, qty: &str) -> PriceLevel {
    PriceLevel {
        price: price.parse().unwrap(),
        qty: qty.parse().unwrap(),
        price_wire: price.to_string(),
        qty_wire: qty.to_string(),
    }
}

fn make_delta(timestamp: Option<MonotonicInstant>) -> BookDelta {
    BookDelta {
        symbol: Symbol::new("BTC/USD").unwrap(),
        bids: vec![lvl("68000.1", "0.5")],
        asks: vec![lvl("68000.2", "1.5")],
        checksum: 3_245_412_112,
        is_snapshot: true,
        timestamp,
        exchange_timestamp: "2026-06-02T12:00:00.000000Z".to_string(),
    }
}

fn sym() -> Symbol {
    Symbol::new("BTC/USD").unwrap()
}

/// `D10` caller maintained at `D25` wire depth (`maintained_depth > CHECKSUM_DEPTH`).
const MAINT: usize = 25;
const CALLER: usize = 10;

#[test]
fn apply_snapshot_then_compute_checksum_orders_correctly() {
    let mut b = OrderBookBuilder::new(sym(), MAINT, CALLER);
    let bids = vec![lvl("50000.00", "1.0"), lvl("49900.00", "2.0")];
    let asks = vec![lvl("50100.00", "1.5"), lvl("50200.00", "0.5")];
    let _ = b.apply_snapshot(bids, asks, 0);
    let mut bids_iter = b.bids.values();
    assert_eq!(bids_iter.next().unwrap().price_wire, "50000.00");
    assert_eq!(bids_iter.next().unwrap().price_wire, "49900.00");
    let mut asks_iter = b.asks.values();
    assert_eq!(asks_iter.next().unwrap().price_wire, "50100.00");
    assert_eq!(asks_iter.next().unwrap().price_wire, "50200.00");
}

#[test]
fn apply_snapshot_with_matching_checksum_succeeds() {
    let mut b = OrderBookBuilder::new(sym(), MAINT, CALLER);
    let bids = vec![lvl("50000.00", "1.0")];
    let asks = vec![lvl("50100.00", "1.5")];
    // Expected CRC: asks "50100.00"/"1.5" → "5010000"/"15"; bids "50000.00"/"1.0" → "5000000"/"10".
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(b"5010000");
    hasher.update(b"15");
    hasher.update(b"5000000");
    hasher.update(b"10");
    let expected = hasher.finalize();

    let result = b.apply_snapshot(bids, asks, expected);
    assert!(result.is_ok(), "expected Ok, got {result:?}");
}

#[test]
fn apply_snapshot_with_wrong_checksum_returns_mismatch() {
    let mut b = OrderBookBuilder::new(sym(), MAINT, CALLER);
    let bids = vec![lvl("50000.00", "1.0")];
    let asks = vec![lvl("50100.00", "1.5")];
    let result = b.apply_snapshot(bids, asks, 0);
    match result {
        Err(ApplyError::ChecksumMismatch {
            expected: 0,
            computed: _,
        }) => {}
        other => panic!("expected ChecksumMismatch, got {other:?}"),
    }
}

#[test]
fn apply_delta_with_zero_qty_removes_level() {
    let mut b = OrderBookBuilder::new(sym(), MAINT, CALLER);
    b.bids.insert(
        DescPrice("50000.00".parse().unwrap()),
        lvl("50000.00", "1.0"),
    );
    b.bids.insert(
        DescPrice("49900.00".parse().unwrap()),
        lvl("49900.00", "2.0"),
    );
    let _ = b.apply_delta(vec![lvl("50000.00", "0")], vec![], 0);
    assert_eq!(b.bids.len(), 1);
    assert!(b.bids.values().any(|l| l.price_wire == "49900.00"));
}

#[test]
fn apply_delta_wire_nonzero_underflow_qty_keeps_level() {
    // Sub-1e-28 qty → Decimal zero but wire-nonzero: must NOT delete. Genuine "0.0" still does.
    let mut b = OrderBookBuilder::new(sym(), MAINT, CALLER);
    b.bids.insert(
        DescPrice("50000.00".parse().unwrap()),
        lvl("50000.00", "1.0"),
    );
    let underflow = lvl("50000.00", "0.0000000000000000000000000000001");
    assert!(underflow.qty.is_zero(), "1e-31 must round to Decimal zero");
    let _ = b.apply_delta(vec![underflow], vec![], 0);
    assert_eq!(
        b.bids.len(),
        1,
        "wire-nonzero underflow must keep the level"
    );
    assert_eq!(
        b.bids.values().next().unwrap().qty_wire,
        "0.0000000000000000000000000000001"
    );

    let _ = b.apply_delta(vec![lvl("50000.00", "0.00000000")], vec![], 0);
    assert_eq!(b.bids.len(), 0, "an exact wire-zero form deletes");
}

#[test]
fn build_update_caps_at_depth() {
    let mut b = OrderBookBuilder::new(sym(), 25, 2);
    b.bids.insert(
        DescPrice("50000.00".parse().unwrap()),
        lvl("50000.00", "1.0"),
    );
    b.bids.insert(
        DescPrice("49900.00".parse().unwrap()),
        lvl("49900.00", "1.0"),
    );
    b.bids.insert(
        DescPrice("49800.00".parse().unwrap()),
        lvl("49800.00", "1.0"),
    );
    let update = b.build_update(42, None, String::new());
    assert_eq!(update.bids.len(), 2);
    assert_eq!(update.bids[0].price, "50000.00".parse().unwrap());
    assert_eq!(update.bids[1].price, "49900.00".parse().unwrap());
    assert_eq!(update.checksum, 42);
}

#[test]
fn apply_snapshot_trims_excess_levels_to_maintained_depth() {
    const MAINTAINED: usize = 25;
    let mut b = OrderBookBuilder::new(sym(), MAINTAINED, 10);
    let bids: Vec<PriceLevel> = (0..27)
        .map(|i| lvl(&format!("{}.00", 50000 - i * 100), "1.0"))
        .collect();
    let asks: Vec<PriceLevel> = (0..27)
        .map(|i| lvl(&format!("{}.00", 50100 + i * 100), "1.0"))
        .collect();
    let _ = b.apply_snapshot(bids, asks, 0);
    assert_eq!(
        b.bids.len(),
        MAINTAINED,
        "bids must be trimmed to maintained depth"
    );
    assert_eq!(
        b.asks.len(),
        MAINTAINED,
        "asks must be trimmed to maintained depth"
    );
    let bid_prices: Vec<String> = b.bids.values().map(|l| l.price_wire.clone()).collect();
    assert_eq!(bid_prices.first().unwrap(), "50000.00");
    assert_eq!(bid_prices.last().unwrap(), "47600.00");
    assert!(
        !bid_prices
            .iter()
            .any(|p| p == "47500.00" || p == "47400.00")
    );
    let ask_prices: Vec<String> = b.asks.values().map(|l| l.price_wire.clone()).collect();
    assert_eq!(ask_prices.first().unwrap(), "50100.00");
    assert_eq!(ask_prices.last().unwrap(), "52500.00");
    assert!(
        !ask_prices
            .iter()
            .any(|p| p == "52600.00" || p == "52700.00")
    );
}

#[test]
fn apply_delta_trims_excess_levels_to_maintained_depth() {
    const MAINTAINED: usize = 25;
    let mut b = OrderBookBuilder::new(sym(), MAINTAINED, 10);
    for i in 0..MAINTAINED {
        let bp = format!("{}.00", 50000 - (i as i64) * 100);
        b.bids
            .insert(DescPrice(bp.parse().unwrap()), lvl(&bp, "1.0"));
        let ap = format!("{}.00", 50100 + (i as i64) * 100);
        b.asks.insert(ap.parse().unwrap(), lvl(&ap, "1.0"));
    }
    let _ = b.apply_delta(
        vec![lvl("47400.00", "1.0")],
        vec![lvl("52600.00", "1.0")],
        0,
    );
    assert_eq!(
        b.bids.len(),
        MAINTAINED,
        "delta bids must be trimmed to maintained depth"
    );
    assert_eq!(
        b.asks.len(),
        MAINTAINED,
        "delta asks must be trimmed to maintained depth"
    );
    assert!(!b.bids.values().any(|l| l.price_wire == "47400.00"));
    assert!(!b.asks.values().any(|l| l.price_wire == "52600.00"));
}

/// Top-10 CRC over pre-sorted slices (asks low→high, bids high→low), capped at `CHECKSUM_DEPTH`.
fn expected_top10_crc(bids_hi_to_lo: &[PriceLevel], asks_lo_to_hi: &[PriceLevel]) -> u32 {
    use crate::book::checksum::CHECKSUM_DEPTH;
    crate::book::compute_book_crc32(
        asks_lo_to_hi
            .iter()
            .take(CHECKSUM_DEPTH)
            .map(|l| (l.price_wire.as_str(), l.qty_wire.as_str())),
        bids_hi_to_lo
            .iter()
            .take(CHECKSUM_DEPTH)
            .map(|l| (l.price_wire.as_str(), l.qty_wire.as_str())),
    )
}

/// 12 bids high→low and 12 asks low→high, distinct qtys so CRC depends on which levels make top-10.
fn seed_twelve_per_side() -> (Vec<PriceLevel>, Vec<PriceLevel>) {
    let bids: Vec<PriceLevel> = (0..12)
        .map(|i| lvl(&format!("{}.00", 50000 - i * 100), &format!("{}.0", i + 1)))
        .collect();
    let asks: Vec<PriceLevel> = (0..12)
        .map(|i| lvl(&format!("{}.00", 50100 + i * 100), &format!("{}.5", i + 1)))
        .collect();
    (bids, asks)
}

/// Removing a top-10 bid promotes rank-11 into slot 10; CRC matches only with D25 headroom.
#[test]
fn boundary_removal_promotes_deeper_level_and_crc_matches_with_headroom() {
    const MAINTAINED: usize = 25;
    let (bids, asks) = seed_twelve_per_side();
    let mut b = OrderBookBuilder::new(sym(), MAINTAINED, 10);
    let seed_crc = expected_top10_crc(&bids, &asks);
    b.apply_snapshot(bids.clone(), asks.clone(), seed_crc)
        .expect("seed snapshot should validate");

    let mut post_bids: Vec<PriceLevel> = bids.clone();
    let removed = post_bids.remove(0);
    assert_eq!(removed.price_wire, "50000.00");
    let expected_crc = expected_top10_crc(&post_bids, &asks);

    let res = b.apply_delta(vec![lvl("50000.00", "0")], vec![], expected_crc);
    assert!(
        res.is_ok(),
        "post-removal top-10 CRC must match with D25 headroom; got {res:?}"
    );
    let top10_bids: Vec<String> = b
        .bids
        .values()
        .take(10)
        .map(|l| l.price_wire.clone())
        .collect();
    assert_eq!(top10_bids.first().unwrap(), "49900.00", "new best bid");
    assert_eq!(
        top10_bids.last().unwrap(),
        "49000.00",
        "promoted rank-11 level"
    );
}

/// Zero-headroom book (rank-11 already trimmed) yields a top-10 CRC ≠ Kraken's post-removal top-10.
#[test]
fn boundary_removal_without_headroom_yields_wrong_crc() {
    let (bids, asks) = seed_twelve_per_side();
    let mut true_post_bids: Vec<PriceLevel> = bids.clone();
    true_post_bids.remove(0);
    let krakens_true_crc = expected_top10_crc(&true_post_bids, &asks);

    let zero_headroom_bids: Vec<PriceLevel> = true_post_bids
        .iter()
        .filter(|l| l.price_wire != "49000.00")
        .cloned()
        .collect();
    assert_eq!(
        zero_headroom_bids.len(),
        10,
        "ranks 2..=12 minus the discarded rank-11"
    );
    let buggy_crc = expected_top10_crc(&zero_headroom_bids, &asks);

    assert_ne!(
        buggy_crc, krakens_true_crc,
        "zero-headroom top-10 (missing the promoted rank-11 level) must differ \
         from Kraken's true post-removal top-10 CRC — this is the BOOK-1 spurious mismatch"
    );
}

/// `build_update` caps to caller depth (10) even when maintained at D25.
#[test]
fn build_update_caps_to_caller_depth_below_maintained() {
    const MAINTAINED: usize = 25;
    const CALLER_DEPTH: usize = 10;
    let (bids, asks) = seed_twelve_per_side();
    let mut b = OrderBookBuilder::new(sym(), MAINTAINED, CALLER_DEPTH);
    let seed_crc = expected_top10_crc(&bids, &asks);
    b.apply_snapshot(bids, asks, seed_crc)
        .expect("seed validates");
    assert_eq!(b.maintained_depth(), MAINTAINED);
    assert_eq!(b.caller_depth(), CALLER_DEPTH);
    assert_eq!(b.bids.len(), 12, "maintained book retains all 12 levels");
    assert_eq!(b.asks.len(), 12);
    let update = b.build_update(seed_crc, None, String::new());
    assert_eq!(
        update.bids.len(),
        CALLER_DEPTH,
        "caller sees exactly top-10 bids"
    );
    assert_eq!(
        update.asks.len(),
        CALLER_DEPTH,
        "caller sees exactly top-10 asks"
    );
    assert_eq!(
        update.bids.first().unwrap().price,
        "50000.00".parse().unwrap()
    );
    assert_eq!(
        update.asks.first().unwrap().price,
        "50100.00".parse().unwrap()
    );
}

#[test]
fn book_delta_serializes_for_republishing() {
    let delta = make_delta(None);
    let v = serde_json::to_value(&delta).expect("BookDelta serializes");
    assert_eq!(v["symbol"], serde_json::json!("BTC/USD"));
    assert_eq!(v["is_snapshot"], serde_json::json!(true));
    assert_eq!(v["checksum"], serde_json::json!(3_245_412_112_u32));
    assert_eq!(v["bids"][0]["price_wire"], serde_json::json!("68000.1"));
}

/// Clock emits as scalar u64 milliseconds, not `Duration`'s `{secs, nanos}`.
#[test]
fn book_delta_monotonic_clock_emits_milliseconds() {
    let delta = make_delta(Some(MonotonicInstant(std::time::Duration::from_millis(
        1_500,
    ))));
    let v = serde_json::to_value(&delta).expect("BookDelta serializes");
    assert_eq!(v["timestamp"], serde_json::json!(1_500_u64));
}

/// `timestamp` is `None` in v1 — must omit rather than render as `null`.
#[test]
fn book_delta_omits_an_absent_monotonic_clock() {
    let v = serde_json::to_value(make_delta(None)).expect("BookDelta serializes");
    assert!(v.get("timestamp").is_none(), "expected omission: {v}");
}
