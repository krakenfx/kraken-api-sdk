//! Unit tests for `SubscriptionRegistry`.
use super::*;

fn sym(s: &str) -> Symbol {
    Symbol::new(s).unwrap()
}

/// Per-pair ticker entry (the common test shape).
fn tk(url: WsUrl, pair: &str) -> SubscriptionEntry {
    SubscriptionEntry::new(
        url,
        ChannelName::Ticker,
        Some(sym(pair)),
        SubscribeParams::Ticker {
            snapshot: None,
            event_trigger: None,
        },
    )
}

#[test]
fn register_and_deregister_per_pair() {
    let mut reg = SubscriptionRegistry::new();
    reg.register(
        tk(WsUrl::Public, "BTC/USD"),
        crate::types::MonotonicInstant::now(),
        false,
    );
    reg.register(
        tk(WsUrl::Public, "ETH/USD"),
        crate::types::MonotonicInstant::now(),
        false,
    );
    assert_eq!(reg.len(), 2);
    reg.deregister(ChannelName::Ticker, Some(sym("BTC/USD")));
    assert_eq!(reg.len(), 1);
    reg.deregister(ChannelName::Ticker, Some(sym("BTC/USD")));
    assert_eq!(reg.len(), 1);
}

#[test]
fn register_returns_post_register_count_a372() {
    // register returns the post-register refcount: 1 on the fresh 0->1 insert, 2+ on duplicate
    // subscribers for the same key; release decrements.
    let mut reg = SubscriptionRegistry::new();
    assert_eq!(
        reg.register(
            tk(WsUrl::Public, "BTC/USD"),
            crate::types::MonotonicInstant::now(),
            false
        ),
        1,
        "0→1 edge"
    );
    assert_eq!(
        reg.register(
            tk(WsUrl::Public, "BTC/USD"),
            crate::types::MonotonicInstant::now(),
            false
        ),
        2,
        "1→2 duplicate"
    );
    assert_eq!(reg.len(), 1, "duplicate (channel,pair) shares one entry");
    assert_eq!(
        reg.register(
            tk(WsUrl::Public, "ETH/USD"),
            crate::types::MonotonicInstant::now(),
            false
        ),
        1,
        "distinct key 0→1"
    );
    assert_eq!(reg.release(ChannelName::Ticker, Some(sym("BTC/USD"))), 1);
    assert_eq!(reg.release(ChannelName::Ticker, Some(sym("BTC/USD"))), 0);
    assert_eq!(
        reg.register(
            tk(WsUrl::Public, "BTC/USD"),
            crate::types::MonotonicInstant::now(),
            false
        ),
        1,
        "re-register 0→1"
    );
}

#[test]
fn register_channel_wide_returns_count_a372() {
    let mut reg = SubscriptionRegistry::new();
    let mk = || {
        SubscriptionEntry::new(
            WsUrl::Auth,
            ChannelName::Balances,
            None,
            SubscribeParams::Balances,
        )
    };
    assert_eq!(
        reg.register(mk(), crate::types::MonotonicInstant::now(), false),
        1,
        "0→1 edge channel-wide"
    );
    assert_eq!(
        reg.register(mk(), crate::types::MonotonicInstant::now(), false),
        2,
        "1→2 duplicate channel-wide"
    );
    assert_eq!(reg.release(ChannelName::Balances, None), 1);
    assert_eq!(reg.release(ChannelName::Balances, None), 0);
}

#[test]
fn register_refcounts_and_release_removes_at_zero() {
    let mut reg = SubscriptionRegistry::new();
    reg.register(
        tk(WsUrl::Public, "BTC/USD"),
        crate::types::MonotonicInstant::now(),
        false,
    );
    reg.register(
        tk(WsUrl::Public, "BTC/USD"),
        crate::types::MonotonicInstant::now(),
        false,
    );
    assert_eq!(reg.len(), 1, "duplicate (channel,pair) shares one entry");
    assert_eq!(reg.release(ChannelName::Ticker, Some(sym("BTC/USD"))), 1);
    assert_eq!(reg.len(), 1);
    assert_eq!(reg.release(ChannelName::Ticker, Some(sym("BTC/USD"))), 0);
    assert!(reg.is_empty());
    assert_eq!(reg.release(ChannelName::Ticker, Some(sym("BTC/USD"))), 0);
}

#[test]
fn register_is_first_writer_wins_on_shared_pair() {
    let mut reg = SubscriptionRegistry::new();
    reg.register(
        SubscriptionEntry::new(
            WsUrl::Public,
            ChannelName::Book,
            Some(sym("BTC/USD")),
            SubscribeParams::Book {
                depth: BookDepth::D100,
            },
        ),
        crate::types::MonotonicInstant::now(),
        false,
    );
    reg.register(
        SubscriptionEntry::new(
            WsUrl::Public,
            ChannelName::Book,
            Some(sym("BTC/USD")),
            SubscribeParams::Book {
                depth: BookDepth::D10,
            },
        ),
        crate::types::MonotonicInstant::now(),
        false,
    );
    let frames = reg.compose_subscribe_frames_for_url(WsUrl::Public);
    assert_eq!(frames.len(), 1);
    assert_eq!(
        frames[0].2["params"]["depth"], 100,
        "first-writer depth kept"
    );
}

#[test]
fn channel_wide_entry_keyed_by_channel_only() {
    let mut reg = SubscriptionRegistry::new();
    reg.register(
        SubscriptionEntry::new(
            WsUrl::Auth,
            ChannelName::Balances,
            None,
            SubscribeParams::Balances,
        ),
        crate::types::MonotonicInstant::now(),
        false,
    );
    assert_eq!(reg.len(), 1);
    assert!(matches!(
        reg.handle_update(ChannelName::Balances, None, false, &serde_json::json!({})),
        BookDriveOutcome::PassThrough
    ));
    reg.deregister(ChannelName::Balances, None);
    assert!(reg.is_empty());
}

#[test]
fn handle_update_non_book_is_passthrough() {
    let mut reg = SubscriptionRegistry::new();
    assert!(matches!(
        reg.handle_update(
            ChannelName::Ticker,
            Some(&sym("BTC/USD")),
            false,
            &serde_json::json!({})
        ),
        BookDriveOutcome::PassThrough
    ));
    reg.register(
        tk(WsUrl::Public, "BTC/USD"),
        crate::types::MonotonicInstant::now(),
        false,
    );
    assert!(matches!(
        reg.handle_update(
            ChannelName::Ticker,
            Some(&sym("BTC/USD")),
            false,
            &serde_json::json!({})
        ),
        BookDriveOutcome::PassThrough
    ));
    assert!(matches!(
        reg.handle_update(
            ChannelName::Ticker,
            Some(&sym("ETH/USD")),
            false,
            &serde_json::json!({})
        ),
        BookDriveOutcome::PassThrough
    ));
}

#[test]
fn drive_update_book_snapshot_matching_crc_is_maintained() {
    let mut reg = SubscriptionRegistry::new();
    reg.register(
        SubscriptionEntry::new(
            WsUrl::Public,
            ChannelName::Book,
            Some(sym("BTC/USD")),
            SubscribeParams::Book {
                depth: BookDepth::D10,
            },
        ),
        crate::types::MonotonicInstant::now(),
        false,
    );
    // CRC32 order: asks ascending then bids descending, over wire byte strings - see order-book guide.
    let expected = crate::book::compute_book_crc32([("100.0", "1.5")], [("99.0", "2.0")]);
    let frame = serde_json::json!({
        "type": "snapshot",
        "data": {
            "symbol": "BTC/USD",
            "bids": [{ "price": "99.0", "qty": "2.0" }],
            "asks": [{ "price": "100.0", "qty": "1.5" }],
            "checksum": expected,
            "timestamp": "2026-07-10T09:44:44.411241Z",
        }
    });
    match reg.handle_update(
        ChannelName::Book,
        Some(&sym("BTC/USD")),
        true,
        &frame["data"],
    ) {
        BookDriveOutcome::Maintained(update) => {
            assert_eq!(update.symbol.as_str(), "BTC/USD");
            assert_eq!(update.bids.len(), 1);
            assert_eq!(update.asks.len(), 1);
            assert_eq!(update.checksum, expected);
            assert_eq!(update.exchange_timestamp, "2026-07-10T09:44:44.411241Z");
        }
        other => panic!("expected Maintained, got {other:?}"),
    }
}

#[test]
fn drive_update_book_crc_mismatch_is_gap() {
    let mut reg = SubscriptionRegistry::new();
    reg.register(
        SubscriptionEntry::new(
            WsUrl::Public,
            ChannelName::Book,
            Some(sym("BTC/USD")),
            SubscribeParams::Book {
                depth: BookDepth::D10,
            },
        ),
        crate::types::MonotonicInstant::now(),
        false,
    );
    let bogus = 1u32;
    let frame = serde_json::json!({
        "type": "snapshot",
        "data": {
            "symbol": "BTC/USD",
            "bids": [{ "price": "99.0", "qty": "2.0" }],
            "asks": [{ "price": "100.0", "qty": "1.5" }],
            "checksum": bogus,
        }
    });
    match reg.handle_update(
        ChannelName::Book,
        Some(&sym("BTC/USD")),
        true,
        &frame["data"],
    ) {
        BookDriveOutcome::Gap { expected, computed } => {
            assert_eq!(expected, bogus);
            assert_ne!(computed, bogus);
        }
        other => panic!("expected Gap, got {other:?}"),
    }
}

/// BookRaw has no builder — the maintained path is `Book`-only, so it
/// PassThroughs even a parseable frame.
#[test]
fn drive_update_book_raw_is_passthrough() {
    let mut reg = SubscriptionRegistry::new();
    reg.register(
        SubscriptionEntry::new(
            WsUrl::Public,
            ChannelName::BookRaw,
            Some(sym("BTC/USD")),
            SubscribeParams::BookRaw {
                depth: BookDepth::D10,
                snapshot: None,
            },
        ),
        crate::types::MonotonicInstant::now(),
        false,
    );
    let frame = serde_json::json!({
        "type": "snapshot",
        "data": {
            "symbol": "BTC/USD",
            "bids": [{ "price": "99.0", "qty": "2.0" }],
            "asks": [{ "price": "100.0", "qty": "1.5" }],
            "checksum": 12345u32,
        }
    });
    assert!(matches!(
        reg.handle_update(
            ChannelName::BookRaw,
            Some(&sym("BTC/USD")),
            true,
            &frame["data"]
        ),
        BookDriveOutcome::PassThrough
    ));
}

/// Order-book resync window: after `begin_book_resync` (the gap-recovery drop), DELTAS are
/// dropped (`AwaitingSnapshot`) — no apply, no re-gap — until a fresh SNAPSHOT reseeds and
/// clears the window.
#[test]
fn drive_update_resync_window_drops_deltas_until_snapshot() {
    let mut reg = SubscriptionRegistry::new();
    reg.register(
        SubscriptionEntry::new(
            WsUrl::Public,
            ChannelName::Book,
            Some(sym("BTC/USD")),
            SubscribeParams::Book {
                depth: BookDepth::D10,
            },
        ),
        crate::types::MonotonicInstant::now(),
        false,
    );
    let snap_crc = crate::book::compute_book_crc32([("100.0", "1.5")], [("99.0", "2.0")]);
    let snapshot = serde_json::json!({
        "type": "snapshot",
        "data": {
            "symbol": "BTC/USD",
            "bids": [{ "price": "99.0", "qty": "2.0" }],
            "asks": [{ "price": "100.0", "qty": "1.5" }],
            "checksum": snap_crc,
        }
    });
    assert!(matches!(
        reg.handle_update(
            ChannelName::Book,
            Some(&sym("BTC/USD")),
            true,
            &snapshot["data"]
        ),
        BookDriveOutcome::Maintained(_)
    ));
    reg.begin_book_resync(ChannelName::Book, &sym("BTC/USD"));
    let delta = serde_json::json!({
        "type": "update",
        "data": {
            "symbol": "BTC/USD",
            "bids": [{ "price": "98.0", "qty": "1.0" }],
            "asks": [],
            "checksum": 12345u32,
        }
    });
    assert!(matches!(
        reg.handle_update(
            ChannelName::Book,
            Some(&sym("BTC/USD")),
            false,
            &delta["data"]
        ),
        BookDriveOutcome::AwaitingSnapshot
    ));
    assert!(matches!(
        reg.handle_update(
            ChannelName::Book,
            Some(&sym("BTC/USD")),
            true,
            &snapshot["data"]
        ),
        BookDriveOutcome::Maintained(_)
    ));
    assert!(!matches!(
        reg.handle_update(
            ChannelName::Book,
            Some(&sym("BTC/USD")),
            false,
            &delta["data"]
        ),
        BookDriveOutcome::AwaitingSnapshot
    ));
}

/// If the post-gap snapshot keeps mismatching, the consecutive-gap cap fires
/// `GapBudgetExhausted`, DROPS the builder, and subsequent `Book` frames degrade to
/// `PassThrough` — no unbounded resubscribe storm.
#[test]
fn drive_update_consecutive_gap_cap_exhausts_then_degrades_to_passthrough() {
    let mut reg = SubscriptionRegistry::new();
    reg.register(
        SubscriptionEntry::new(
            WsUrl::Public,
            ChannelName::Book,
            Some(sym("BTC/USD")),
            SubscribeParams::Book {
                depth: BookDepth::D10,
            },
        ),
        crate::types::MonotonicInstant::now(),
        false,
    );
    let bad = serde_json::json!({
        "type": "snapshot",
        "data": {
            "symbol": "BTC/USD",
            "bids": [{ "price": "99.0", "qty": "2.0" }],
            "asks": [{ "price": "100.0", "qty": "1.5" }],
            "checksum": 1u32,
        }
    });
    for _ in 1..MAX_CONSECUTIVE_BOOK_GAPS {
        assert!(matches!(
            reg.handle_update(ChannelName::Book, Some(&sym("BTC/USD")), true, &bad["data"]),
            BookDriveOutcome::Gap { .. }
        ));
    }
    assert!(matches!(
        reg.handle_update(ChannelName::Book, Some(&sym("BTC/USD")), true, &bad["data"]),
        BookDriveOutcome::GapBudgetExhausted
    ));
    assert!(matches!(
        reg.handle_update(ChannelName::Book, Some(&sym("BTC/USD")), true, &bad["data"]),
        BookDriveOutcome::PassThrough
    ));
}

/// Helper: register a maintained `Book` entry for `pair` and seed a valid book.
fn seeded_book_reg(pair: &str) -> SubscriptionRegistry {
    let mut reg = SubscriptionRegistry::new();
    reg.register(
        SubscriptionEntry::new(
            WsUrl::Public,
            ChannelName::Book,
            Some(sym(pair)),
            SubscribeParams::Book {
                depth: BookDepth::D10,
            },
        ),
        crate::types::MonotonicInstant::now(),
        false,
    );
    let snap_crc = crate::book::compute_book_crc32([("100.0", "1.5")], [("99.0", "2.0")]);
    let snapshot = serde_json::json!({
        "type": "snapshot",
        "data": {
            "symbol": pair,
            "bids": [{ "price": "99.0", "qty": "2.0" }],
            "asks": [{ "price": "100.0", "qty": "1.5" }],
            "checksum": snap_crc,
        }
    });
    assert!(matches!(
        reg.handle_update(ChannelName::Book, Some(&sym(pair)), true, &snapshot["data"]),
        BookDriveOutcome::Maintained(_)
    ));
    reg
}

/// While resyncing, each reseed timeout is a `Retry` toward the shared gap budget; at the cap
/// it `Exhausted`s and DROPS maintenance, then later timeouts are `StaleNoOp`.
#[test]
fn reseed_timeout_retries_under_cap_then_exhausts_and_degrades() {
    let mut reg = seeded_book_reg("BTC/USD");
    reg.begin_book_resync(ChannelName::Book, &sym("BTC/USD"));
    for _ in 1..MAX_CONSECUTIVE_BOOK_GAPS {
        assert_eq!(
            reg.note_reseed_timeout(ChannelName::Book, &sym("BTC/USD")),
            ReseedTimeoutOutcome::Retry
        );
    }
    assert_eq!(
        reg.note_reseed_timeout(ChannelName::Book, &sym("BTC/USD")),
        ReseedTimeoutOutcome::Exhausted
    );
    assert_eq!(
        reg.note_reseed_timeout(ChannelName::Book, &sym("BTC/USD")),
        ReseedTimeoutOutcome::StaleNoOp
    );
    let snap_crc = crate::book::compute_book_crc32([("100.0", "1.5")], [("99.0", "2.0")]);
    let snapshot = serde_json::json!({
        "type": "snapshot",
        "data": { "symbol": "BTC/USD", "bids": [{ "price": "99.0", "qty": "2.0" }],
                  "asks": [{ "price": "100.0", "qty": "1.5" }], "checksum": snap_crc }
    });
    assert!(matches!(
        reg.handle_update(
            ChannelName::Book,
            Some(&sym("BTC/USD")),
            true,
            &snapshot["data"]
        ),
        BookDriveOutcome::PassThrough
    ));
}

/// A terminally rejected (tombstoned) entry's leftover reseed timer is a stale
/// no-op: revival is caller-re-subscribe only, never a timer-driven replay.
#[test]
fn reseed_timeout_on_tombstone_is_stale_no_op() {
    let mut reg = seeded_book_reg("BTC/USD");
    reg.begin_book_resync(ChannelName::Book, &sym("BTC/USD"));
    assert_eq!(
        reg.note_reseed_timeout(ChannelName::Book, &sym("BTC/USD")),
        ReseedTimeoutOutcome::Retry,
        "sanity: a live resyncing entry retries"
    );
    reg.note_terminated(
        ChannelName::Book,
        &Some(sym("BTC/USD")),
        TerminationCause::NonTransientWireRejection,
        None,
    );
    assert_eq!(
        reg.note_reseed_timeout(ChannelName::Book, &sym("BTC/USD")),
        ReseedTimeoutOutcome::StaleNoOp,
        "tombstone: a leftover reseed timer must not replay the subscribe"
    );
}

/// A reseed-snapshot timeout when the book is NOT resyncing (the fresh snapshot already
/// landed) is a stale single-shot fire → `StaleNoOp`, no spurious re-reseed of a healthy book.
#[test]
fn reseed_timeout_when_not_resyncing_is_stale_noop() {
    let mut reg = seeded_book_reg("BTC/USD");
    assert_eq!(
        reg.note_reseed_timeout(ChannelName::Book, &sym("BTC/USD")),
        ReseedTimeoutOutcome::StaleNoOp
    );
}

/// Unknown/deregistered `(Book, symbol)` reseed timeout is a `StaleNoOp` — no panic.
#[test]
fn reseed_timeout_unknown_entry_is_stale_noop() {
    let mut reg = SubscriptionRegistry::new();
    assert_eq!(
        reg.note_reseed_timeout(ChannelName::Book, &sym("BTC/USD")),
        ReseedTimeoutOutcome::StaleNoOp
    );
}

/// The reseed-timeout counter is SHARED with CRC mismatches: one real mismatch
/// plus snapshot-timeouts reach `MAX_CONSECUTIVE_BOOK_GAPS` together.
#[test]
fn reseed_timeout_shares_gap_counter_with_crc_mismatch() {
    let mut reg = seeded_book_reg("BTC/USD");
    let bad = serde_json::json!({
        "type": "snapshot",
        "data": { "symbol": "BTC/USD", "bids": [{ "price": "99.0", "qty": "2.0" }],
                  "asks": [{ "price": "100.0", "qty": "1.5" }], "checksum": 1u32 }
    });
    assert!(matches!(
        reg.handle_update(ChannelName::Book, Some(&sym("BTC/USD")), true, &bad["data"]),
        BookDriveOutcome::Gap { .. }
    ));
    reg.begin_book_resync(ChannelName::Book, &sym("BTC/USD"));
    for _ in 2..MAX_CONSECUTIVE_BOOK_GAPS {
        assert_eq!(
            reg.note_reseed_timeout(ChannelName::Book, &sym("BTC/USD")),
            ReseedTimeoutOutcome::Retry
        );
    }
    assert_eq!(
        reg.note_reseed_timeout(ChannelName::Book, &sym("BTC/USD")),
        ReseedTimeoutOutcome::Exhausted
    );
}

/// `is_book_resyncing` is true only for a maintained book in the resync window; false for
/// healthy, raw, absent, or `None`-pair entries.
#[test]
fn is_book_resyncing_reports_resync_window_only() {
    let mut reg = seeded_book_reg("BTC/USD");
    assert!(!reg.is_book_resyncing(ChannelName::Book, &Some(sym("BTC/USD"))));
    reg.begin_book_resync(ChannelName::Book, &sym("BTC/USD"));
    assert!(reg.is_book_resyncing(ChannelName::Book, &Some(sym("BTC/USD"))));
    assert!(!reg.is_book_resyncing(ChannelName::Book, &Some(sym("ETH/USD"))));
    assert!(!reg.is_book_resyncing(ChannelName::Book, &None));
}

#[test]
fn compose_filters_by_url() {
    let mut reg = SubscriptionRegistry::new();
    reg.register(
        tk(WsUrl::Public, "BTC/USD"),
        crate::types::MonotonicInstant::now(),
        false,
    );
    reg.register(
        SubscriptionEntry::new(
            WsUrl::Auth,
            ChannelName::Executions,
            None,
            SubscribeParams::Executions,
        ),
        crate::types::MonotonicInstant::now(),
        false,
    );
    let public = reg.compose_subscribe_frames_for_url(WsUrl::Public);
    assert_eq!(public.len(), 1);
    assert_eq!(public[0].0, ChannelName::Ticker);
}

#[test]
fn compose_emits_deterministic_replay_order() {
    // Golden replay order: channel-wide entries first (alphabetic by wire string), then
    // per-pair (alphabetic channel, then symbol) - NOT enum-declaration order.
    let mut reg = SubscriptionRegistry::new();
    reg.register(
        SubscriptionEntry::new(
            WsUrl::Public,
            ChannelName::Trade,
            Some(sym("ETH/USD")),
            SubscribeParams::Trade { snapshot: None },
        ),
        crate::types::MonotonicInstant::now(),
        false,
    );
    reg.register(
        tk(WsUrl::Public, "ETH/USD"),
        crate::types::MonotonicInstant::now(),
        false,
    );
    reg.register(
        tk(WsUrl::Public, "BTC/USD"),
        crate::types::MonotonicInstant::now(),
        false,
    );
    reg.register(
        SubscriptionEntry::new(
            WsUrl::Public,
            ChannelName::Status,
            None,
            SubscribeParams::Status,
        ),
        crate::types::MonotonicInstant::now(),
        false,
    );
    reg.register(
        SubscriptionEntry::new(
            WsUrl::Public,
            ChannelName::Balances,
            None,
            SubscribeParams::Balances,
        ),
        crate::types::MonotonicInstant::now(),
        false,
    );
    let order: Vec<(ChannelName, Option<Symbol>)> = reg
        .compose_subscribe_frames_for_url(WsUrl::Public)
        .into_iter()
        .map(|(c, p, _)| (c, p))
        .collect();
    assert_eq!(
        order,
        vec![
            (ChannelName::Balances, None),
            (ChannelName::Status, None),
            (ChannelName::Ticker, Some(sym("BTC/USD"))),
            (ChannelName::Ticker, Some(sym("ETH/USD"))),
            (ChannelName::Trade, Some(sym("ETH/USD"))),
        ]
    );
}

#[test]
fn book_frame_carries_depth_and_snapshot() {
    let pairs = vec![sym("BTC/USD")];
    let frame = build_subscribe_frame(
        "subscribe",
        ChannelName::Book,
        &pairs,
        SubscribeParams::Book {
            depth: BookDepth::D100,
        },
        42,
    );
    assert_eq!(frame["method"], "subscribe");
    assert_eq!(frame["params"]["channel"], "book");
    assert_eq!(frame["params"]["depth"], 100);
    assert_eq!(frame["params"]["snapshot"], true);
    assert_eq!(frame["params"]["symbol"][0], "BTC/USD");
    assert_eq!(frame["req_id"], 42);
}

/// A `D10` subscribe requests the bumped `D25` wire depth so Kraken streams the levels the
/// maintained book needs for CRC headroom; deeper depths pass through unchanged.
#[test]
fn book_frame_bumps_d10_wire_depth_to_d25() {
    let pairs = vec![sym("BTC/USD")];
    let d10 = build_subscribe_frame(
        "subscribe",
        ChannelName::Book,
        &pairs,
        SubscribeParams::Book {
            depth: BookDepth::D10,
        },
        1,
    );
    assert_eq!(
        d10["params"]["depth"], 25,
        "D10 caller subscribes at the bumped D25 wire depth"
    );
    let d25 = build_subscribe_frame(
        "subscribe",
        ChannelName::Book,
        &pairs,
        SubscribeParams::Book {
            depth: BookDepth::D25,
        },
        2,
    );
    assert_eq!(d25["params"]["depth"], 25);
}

/// Ticker `snapshot` + `event_trigger` are caller-settable; `None` omits the field (Kraken
/// default applies), a value renders on the wire.
#[test]
fn ticker_frame_carries_snapshot_and_event_trigger_md3882() {
    let pairs = vec![sym("BTC/USD")];
    let mk = |snapshot, event_trigger| {
        build_subscribe_frame(
            "subscribe",
            ChannelName::Ticker,
            &pairs,
            SubscribeParams::Ticker {
                snapshot,
                event_trigger,
            },
            0,
        )
    };

    let default = mk(None, None);
    assert!(default["params"].get("snapshot").is_none());
    assert!(default["params"].get("event_trigger").is_none());

    let bbo = mk(Some(false), Some(TickerTrigger::Bbo));
    assert_eq!(bbo["params"]["snapshot"], false);
    assert_eq!(bbo["params"]["event_trigger"], "bbo");
    assert_eq!(
        mk(None, Some(TickerTrigger::Trades))["params"]["event_trigger"],
        "trades"
    );

    let unsub = build_subscribe_frame(
        "unsubscribe",
        ChannelName::Ticker,
        &pairs,
        SubscribeParams::Ticker {
            snapshot: Some(false),
            event_trigger: Some(TickerTrigger::Bbo),
        },
        0,
    );
    assert!(unsub["params"].get("snapshot").is_none());
    assert!(unsub["params"].get("event_trigger").is_none());
}

/// Maintained `Book` always seeds (`snapshot:true`, not caller-settable); `book_raw`, `trade`,
/// `ohlc` omit `snapshot` when `None` (Kraken per-channel default applies) and render an
/// explicit `Some`.
#[test]
fn snapshot_flag_per_channel_md3882() {
    let pairs = vec![sym("BTC/USD")];

    let book = build_subscribe_frame(
        "subscribe",
        ChannelName::Book,
        &pairs,
        SubscribeParams::Book {
            depth: BookDepth::D25,
        },
        0,
    );
    assert_eq!(
        book["params"]["snapshot"], true,
        "maintained book always seeds"
    );

    // book_raw snapshot is caller-settable; None omits the field and Kraken's default applies - see streaming guide.
    let book_raw = |snapshot| {
        build_subscribe_frame(
            "subscribe",
            ChannelName::Book,
            &pairs,
            SubscribeParams::BookRaw {
                depth: BookDepth::D25,
                snapshot,
            },
            0,
        )
    };
    assert!(
        book_raw(None)["params"].get("snapshot").is_none(),
        "book_raw None omits snapshot (Kraken book default applies) — like Trade/Ohlc"
    );
    assert_eq!(
        book_raw(Some(false))["params"]["snapshot"],
        false,
        "book_raw honors explicit false"
    );
    assert_eq!(
        book_raw(Some(true))["params"]["snapshot"],
        true,
        "book_raw honors explicit true"
    );

    let trade = |snapshot| {
        build_subscribe_frame(
            "subscribe",
            ChannelName::Trade,
            &pairs,
            SubscribeParams::Trade { snapshot },
            0,
        )
    };
    assert!(trade(None)["params"].get("snapshot").is_none());
    assert_eq!(trade(Some(true))["params"]["snapshot"], true);

    let ohlc = |snapshot| {
        build_subscribe_frame(
            "subscribe",
            ChannelName::Ohlc,
            &pairs,
            SubscribeParams::Ohlc {
                interval: OhlcInterval::M5,
                snapshot,
            },
            0,
        )
    };
    assert!(ohlc(None)["params"].get("snapshot").is_none());
    assert_eq!(ohlc(Some(false))["params"]["snapshot"], false);
}

/// A `D10` `Book` entry's builder is wired with maintained depth 25 (headroom)
/// and caller depth 10 (the output cap); deeper depths keep both axes equal.
#[test]
fn book_entry_builder_decouples_maintained_from_caller_depth() {
    let d10 = SubscriptionEntry::new(
        WsUrl::Public,
        ChannelName::Book,
        Some(sym("BTC/USD")),
        SubscribeParams::Book {
            depth: BookDepth::D10,
        },
    );
    let b = d10
        .builder
        .as_ref()
        .expect("maintained Book entry has a builder");
    assert_eq!(
        b.maintained_depth(),
        25,
        "D10 maintained at the D25 wire depth"
    );
    assert_eq!(b.caller_depth(), 10, "caller still sees top-10");

    let d100 = SubscriptionEntry::new(
        WsUrl::Public,
        ChannelName::Book,
        Some(sym("ETH/USD")),
        SubscribeParams::Book {
            depth: BookDepth::D100,
        },
    );
    let b100 = d100.builder.as_ref().expect("builder present");
    assert_eq!(b100.maintained_depth(), 100);
    assert_eq!(b100.caller_depth(), 100);
}

#[test]
fn channel_wide_frame_omits_symbol() {
    let frame = build_subscribe_frame(
        "subscribe",
        ChannelName::Balances,
        &[],
        SubscribeParams::Balances,
        7,
    );
    assert_eq!(frame["params"]["channel"], "balances");
    assert!(frame["params"].get("symbol").is_none());
}

#[test]
fn authed_composer_injects_token_into_params() {
    let mut reg = SubscriptionRegistry::new();
    reg.register(
        SubscriptionEntry::new(
            WsUrl::Auth,
            ChannelName::Executions,
            None,
            SubscribeParams::Executions,
        ),
        crate::types::MonotonicInstant::now(),
        false,
    );
    let frames = reg.compose_subscribe_frames_for_url_authed(WsUrl::Auth, "tok-XYZ");
    assert_eq!(frames.len(), 1);
    let params = &frames[0].2["params"];
    assert_eq!(params["token"], "tok-XYZ", "token rides inside params");
    assert_eq!(params["channel"], "executions");
    assert!(
        frames[0].2.get("token").is_none(),
        "token must NOT appear at the top level of the frame"
    );
}

#[test]
fn inject_token_is_noop_without_params_object() {
    let mut frame = serde_json::json!({ "method": "subscribe" });
    inject_token(&mut frame, "tok");
    assert!(frame.get("token").is_none());
    assert!(frame.get("params").is_none());
}

#[test]
fn first_and_remaining_signed_subscribes_split_the_golden_batch() {
    let mut reg = SubscriptionRegistry::new();
    reg.register(
        SubscriptionEntry::new(
            WsUrl::Auth,
            ChannelName::Executions,
            None,
            SubscribeParams::Executions,
        ),
        crate::types::MonotonicInstant::now(),
        false,
    );
    reg.register(
        SubscriptionEntry::new(
            WsUrl::Auth,
            ChannelName::Balances,
            None,
            SubscribeParams::Balances,
        ),
        crate::types::MonotonicInstant::now(),
        false,
    );

    let first = reg
        .compose_first_signed_subscribe_authed(WsUrl::Auth, "tok-A")
        .expect("an auth-URL entry exists");
    assert_eq!(first.0, ChannelName::Balances, "first = golden-order head");
    assert_eq!(first.2["params"]["channel"], "balances");
    assert_eq!(first.2["params"]["token"], "tok-A", "token inside params");

    let remaining = reg.compose_remaining_signed_subscribes_authed(
        WsUrl::Auth,
        "tok-A",
        &(ChannelName::Balances, None),
    );
    assert_eq!(remaining.len(), 1, "the tail is everything but the head");
    assert_eq!(remaining[0].0, ChannelName::Executions);
    assert_eq!(remaining[0].2["params"]["channel"], "executions");
    assert_eq!(remaining[0].2["params"]["token"], "tok-A");
}

#[test]
fn remaining_excludes_the_sent_first_by_identity_not_position() {
    // compose_remaining must exclude the already-sent entry by (channel, pair) identity, not
    // position: a mid-handshake Register can sort ahead of the sent head, so a positional
    let mut reg = SubscriptionRegistry::new();
    reg.register(
        SubscriptionEntry::new(
            WsUrl::Auth,
            ChannelName::Executions,
            None,
            SubscribeParams::Executions,
        ),
        crate::types::MonotonicInstant::now(),
        false,
    );
    let first = reg
        .compose_first_signed_subscribe_authed(WsUrl::Auth, "tok")
        .expect("executions entry exists");
    assert_eq!(first.0, ChannelName::Executions);
    reg.register(
        SubscriptionEntry::new(
            WsUrl::Auth,
            ChannelName::Balances,
            None,
            SubscribeParams::Balances,
        ),
        crate::types::MonotonicInstant::now(),
        false,
    );
    let remaining =
        reg.compose_remaining_signed_subscribes_authed(WsUrl::Auth, "tok", &(first.0, first.1));
    assert_eq!(
        remaining.len(),
        1,
        "only the mid-handshake newcomer replays"
    );
    assert_eq!(
        remaining[0].0,
        ChannelName::Balances,
        "the newcomer replays; the already-sent Executions is excluded by identity"
    );
}

#[test]
fn first_signed_subscribe_none_without_auth_entries() {
    let reg = SubscriptionRegistry::new();
    assert!(
        reg.compose_first_signed_subscribe_authed(WsUrl::Auth, "tok")
            .is_none()
    );
    assert!(
        reg.compose_remaining_signed_subscribes_authed(
            WsUrl::Auth,
            "tok",
            &(ChannelName::Executions, None)
        )
        .is_empty()
    );
}

/// Channel-wide executions entry for the sequence-tracking tests.
fn ex_entry() -> SubscriptionEntry {
    SubscriptionEntry::new(
        WsUrl::Auth,
        ChannelName::Executions,
        None,
        SubscribeParams::Executions,
    )
}

/// A snapshot seeds the tracker, contiguous updates stay quiet, and a jump returns the
/// dropped-frame count (`delta - 1`), after which the tracker has advanced to the observed
/// sequence.
#[test]
fn note_sequence_detects_jump_after_snapshot_seed() {
    let mut reg = SubscriptionRegistry::new();
    reg.register(ex_entry(), crate::types::MonotonicInstant::now(), false);
    assert_eq!(reg.note_sequence(ChannelName::Executions, 1, true), None);
    assert_eq!(reg.note_sequence(ChannelName::Executions, 2, false), None);
    assert_eq!(reg.note_sequence(ChannelName::Executions, 3, false), None);
    assert_eq!(
        reg.note_sequence(ChannelName::Executions, 6, false),
        Some(2),
        "3→6 lost frames 4 and 5"
    );
    assert_eq!(
        reg.note_sequence(ChannelName::Executions, 7, false),
        None,
        "tracker advanced to the observed sequence"
    );
}

/// A mid-stream snapshot resets the epoch (no gap across the reset) and a
/// sequence regression resets rather than false-gapping later.
#[test]
fn note_sequence_snapshot_and_regression_reset() {
    let mut reg = SubscriptionRegistry::new();
    reg.register(
        SubscriptionEntry::new(
            WsUrl::Auth,
            ChannelName::Balances,
            None,
            SubscribeParams::Balances,
        ),
        crate::types::MonotonicInstant::now(),
        false,
    );
    assert_eq!(reg.note_sequence(ChannelName::Balances, 5, true), None);
    assert_eq!(reg.note_sequence(ChannelName::Balances, 6, false), None);
    assert_eq!(reg.note_sequence(ChannelName::Balances, 1, true), None);
    assert_eq!(reg.note_sequence(ChannelName::Balances, 2, false), None);
    assert_eq!(reg.note_sequence(ChannelName::Balances, 1, false), None);
    assert_eq!(reg.note_sequence(ChannelName::Balances, 2, false), None);
}

/// A stray wire ack on a tombstone is a no-op: it must not resurrect the dead
/// entry into a replay-eligible zombie that pins the auth hints.
#[test]
fn ack_on_tombstone_is_a_noop() {
    let mirror: SubscriptionMirror =
        std::sync::Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));
    let mut reg = SubscriptionRegistry::with_mirror(std::sync::Arc::clone(&mirror));
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(1), false);
    reg.note_terminated(
        ChannelName::Ticker,
        &Some(sym("BTC/USD")),
        TerminationCause::NonTransientWireRejection,
        None,
    );
    reg.note_acked(ChannelName::Ticker, &Some(sym("BTC/USD")));
    assert!(
        reg.is_terminated(ChannelName::Ticker, &Some(sym("BTC/USD"))),
        "ack must not un-tombstone the entry"
    );
    assert!(
        !reg.has_url_entry(WsUrl::Public),
        "tombstone stays absent for liveness after a stray ack"
    );
    let row_state = mirror.read().unwrap()[&(ChannelName::Ticker, Some(sym("BTC/USD")))]
        .state
        .clone();
    assert!(
        matches!(row_state, EntrySubState::Terminated { .. }),
        "projected row stays Terminated"
    );
}

/// A Failed tombstone stays visible until the LAST holder releases; each
/// per-holder release counts down, never a wire teardown or a second event.
#[test]
fn tombstone_retained_until_last_holder_releases() {
    let mirror: SubscriptionMirror =
        std::sync::Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));
    let mut reg = SubscriptionRegistry::with_mirror(std::sync::Arc::clone(&mirror));
    for _ in 0..3 {
        reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(1), false);
    }
    reg.note_terminated(
        ChannelName::Ticker,
        &Some(sym("BTC/USD")),
        TerminationCause::NonTransientWireRejection,
        None,
    );
    for remaining_holders in [2, 1] {
        assert_eq!(reg.release(ChannelName::Ticker, Some(sym("BTC/USD"))), 0);
        assert_eq!(
            reg.tombstone_holders(ChannelName::Ticker, &Some(sym("BTC/USD"))),
            remaining_holders,
            "each release counts one holder down"
        );
        assert!(
            reg.is_terminated(ChannelName::Ticker, &Some(sym("BTC/USD"))),
            "tombstone survives while holders remain"
        );
        assert!(
            mirror
                .read()
                .unwrap()
                .contains_key(&(ChannelName::Ticker, Some(sym("BTC/USD"))))
        );
    }
    assert_eq!(reg.release(ChannelName::Ticker, Some(sym("BTC/USD"))), 0);
    assert!(
        !reg.is_terminated(ChannelName::Ticker, &Some(sym("BTC/USD"))),
        "last holder's release clears the tombstone"
    );
    assert!(
        !mirror
            .read()
            .unwrap()
            .contains_key(&(ChannelName::Ticker, Some(sym("BTC/USD")))),
        "mirror row removed with the entry"
    );
}

/// Revival wipes the tombstone bookkeeping: fresh lifetime, no leftover holders.
#[test]
fn revival_clears_tombstone_holders() {
    let mut reg = SubscriptionRegistry::new();
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(1), false);
    reg.note_terminated(
        ChannelName::Ticker,
        &Some(sym("BTC/USD")),
        TerminationCause::NonTransientWireRejection,
        None,
    );
    assert_eq!(
        reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(2), false),
        1
    );
    assert!(!reg.is_terminated(ChannelName::Ticker, &Some(sym("BTC/USD"))));
    assert_eq!(
        reg.tombstone_holders(ChannelName::Ticker, &Some(sym("BTC/USD"))),
        0,
        "revival wipes the countdown"
    );
    // Same-caller round trip: the reviver retired their own dead slot, so
    // their release is the real 1→0 edge — no credit absorbs it.
    assert_eq!(reg.release(ChannelName::Ticker, Some(sym("BTC/USD"))), 0);
    assert!(!reg.has_url_entry(WsUrl::Public));
}

/// A second terminal transition on the same lifetime is a no-op — the holder snapshot must not
/// be re-taken from the already-zeroed count, and the retained terminal keeps the FIRST cause.
#[test]
fn note_terminated_second_call_is_a_noop() {
    let mirror = SubscriptionMirror::default();
    let mut reg = SubscriptionRegistry::with_mirror(std::sync::Arc::clone(&mirror));
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(1), false);
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(2), false);
    reg.note_terminated(
        ChannelName::Ticker,
        &Some(sym("BTC/USD")),
        TerminationCause::NonTransientWireRejection,
        None,
    );
    reg.note_terminated(
        ChannelName::Ticker,
        &Some(sym("BTC/USD")),
        TerminationCause::ClientClosed,
        None,
    );
    assert_eq!(
        reg.tombstone_holders(ChannelName::Ticker, &Some(sym("BTC/USD"))),
        2,
        "repeat terminal must not re-snapshot holders from the zeroed count"
    );
    {
        let m = mirror.read().unwrap();
        let row = m
            .get(&(ChannelName::Ticker, Some(sym("BTC/USD"))))
            .expect("tombstone row visible");
        assert!(
            matches!(
                &row.state,
                EntrySubState::Terminated {
                    cause: TerminationCause::NonTransientWireRejection,
                    ..
                }
            ),
            "repeat terminal must not overwrite the first cause"
        );
    }
    // Two holders from the first snapshot still count down one at a time.
    assert_eq!(reg.release(ChannelName::Ticker, Some(sym("BTC/USD"))), 0);
    assert!(reg.is_terminated(ChannelName::Ticker, &Some(sym("BTC/USD"))));
    assert_eq!(reg.release(ChannelName::Ticker, Some(sym("BTC/USD"))), 0);
    assert!(!reg.is_terminated(ChannelName::Ticker, &Some(sym("BTC/USD"))));
}

/// Revival converts the remaining countdown into stale-release credits: a dead lifetime's bare
/// unsubscribe is absorbed as a no-op instead of decrementing the revived lifetime's live
/// count.
#[test]
fn stale_bare_release_after_revival_is_credited() {
    let mut reg = SubscriptionRegistry::new();
    for t in [1, 2, 3] {
        reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(t), false);
    }
    reg.note_terminated(
        ChannelName::Ticker,
        &Some(sym("BTC/USD")),
        TerminationCause::NonTransientWireRejection,
        None,
    );
    // Revival before any straggler released: the reviver retires one dead
    // slot (presumed a returning holder); the other two convert to credits.
    assert_eq!(
        reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(4), false),
        1
    );
    assert_eq!(
        reg.stale_release_credits(ChannelName::Ticker, &Some(sym("BTC/USD"))),
        2
    );
    // Both stragglers release: absorbed, live count untouched.
    for remaining in [1, 0] {
        assert_eq!(reg.release(ChannelName::Ticker, Some(sym("BTC/USD"))), 1);
        assert_eq!(
            reg.stale_release_credits(ChannelName::Ticker, &Some(sym("BTC/USD"))),
            remaining
        );
    }
    // The reviver's own release is the real 1→0 edge.
    assert_eq!(reg.release(ChannelName::Ticker, Some(sym("BTC/USD"))), 0);
    assert!(!reg.has_url_entry(WsUrl::Public));
}

/// Credits exclude guard holders: their stale drops no-op upstream in `release_guard_ref` and
/// never reach `release`, so crediting them would leave permanently unconsumable surplus.
#[test]
fn revival_credits_exclude_guard_holders() {
    use crate::dispatch::HandlerId;
    let mut reg = SubscriptionRegistry::new();
    for t in [1, 2, 3] {
        reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(t), false);
    }
    reg.record_guard_ref(HandlerId(9), ChannelName::Ticker, &Some(sym("BTC/USD")));
    reg.note_terminated(
        ChannelName::Ticker,
        &Some(sym("BTC/USD")),
        TerminationCause::NonTransientWireRejection,
        None,
    );
    assert_eq!(
        reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(4), false),
        1
    );
    assert_eq!(
        reg.stale_release_credits(ChannelName::Ticker, &Some(sym("BTC/USD"))),
        1,
        "guard holder and the reviver's own slot excluded; one straggler credited"
    );
    // The stale guard drop no-ops upstream and burns nothing.
    assert_eq!(
        reg.release_guard_ref(HandlerId(9), ChannelName::Ticker, Some(sym("BTC/USD"))),
        1
    );
    assert_eq!(
        reg.stale_release_credits(ChannelName::Ticker, &Some(sym("BTC/USD"))),
        1
    );
}

/// A NEW-lifetime guard drop routes through the guard-delegated release path
/// and must never burn a stale-release credit.
#[test]
fn new_lifetime_guard_drop_does_not_burn_credit() {
    use crate::dispatch::HandlerId;
    let mut reg = SubscriptionRegistry::new();
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(1), false);
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(2), false);
    reg.note_terminated(
        ChannelName::Ticker,
        &Some(sym("BTC/USD")),
        TerminationCause::NonTransientWireRejection,
        None,
    );
    // Revival with one bare straggler owed (the reviver retires the other slot).
    assert_eq!(
        reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(3), false),
        1
    );
    // A second, guard-backed subscriber on the NEW lifetime.
    assert_eq!(
        reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(3), false),
        2
    );
    reg.record_guard_ref(HandlerId(11), ChannelName::Ticker, &Some(sym("BTC/USD")));
    // Its legitimate drop decrements the live count — no credit consumed.
    assert_eq!(
        reg.release_guard_ref(HandlerId(11), ChannelName::Ticker, Some(sym("BTC/USD"))),
        1
    );
    assert_eq!(
        reg.stale_release_credits(ChannelName::Ticker, &Some(sym("BTC/USD"))),
        1,
        "guard-delegated release must not touch credits"
    );
}

/// Credits exclude only the DYING lifetime's guard refs: an older lifetime's still-held guard
/// was never in the converted snapshot, and counting it deflates the credits until a straggler
/// tears the revival down.
#[test]
fn older_generation_guard_ref_does_not_deflate_revival_credits() {
    use crate::dispatch::HandlerId;
    let mut reg = SubscriptionRegistry::new();
    // Lifetime 1: one bare holder + one guard holder; guard A is never dropped.
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(1), false);
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(2), false);
    reg.record_guard_ref(HandlerId(21), ChannelName::Ticker, &Some(sym("BTC/USD")));
    reg.note_terminated(
        ChannelName::Ticker,
        &Some(sym("BTC/USD")),
        TerminationCause::NonTransientWireRejection,
        None,
    );
    // Revival 1: the dying lifetime's guard and the reviver retire both slots.
    assert_eq!(
        reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(3), false),
        1
    );
    assert_eq!(
        reg.stale_release_credits(ChannelName::Ticker, &Some(sym("BTC/USD"))),
        0
    );
    // Lifetime 2: two more bare holders join, then the lifetime dies too.
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(4), false);
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(5), false);
    reg.note_terminated(
        ChannelName::Ticker,
        &Some(sym("BTC/USD")),
        TerminationCause::NonTransientWireRejection,
        None,
    );
    // Revival 2: guard A's row is from lifetime 1 — NOT in this snapshot.
    // Three dead holders minus the reviver's slot leaves two credits.
    assert_eq!(
        reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(6), false),
        1
    );
    assert_eq!(
        reg.stale_release_credits(ChannelName::Ticker, &Some(sym("BTC/USD"))),
        2,
        "an older-generation guard row must not deflate the credits"
    );
    // Both dead-lifetime stragglers are absorbed; the revival stays live.
    assert_eq!(reg.release(ChannelName::Ticker, Some(sym("BTC/USD"))), 1);
    assert_eq!(reg.release(ChannelName::Ticker, Some(sym("BTC/USD"))), 1);
    assert!(reg.has_url_entry(WsUrl::Public));
}

/// A guard-backed reviver's drop is generation-validated and never burns a
/// credit, so its slot is not reserved: every dead bare holder stays credited.
#[test]
fn guard_backed_reviver_does_not_reserve_the_bare_slot() {
    use crate::dispatch::HandlerId;
    let mut reg = SubscriptionRegistry::new();
    // One bare holder; the lifetime dies before it releases.
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(1), false);
    reg.note_terminated(
        ChannelName::Ticker,
        &Some(sym("BTC/USD")),
        TerminationCause::NonTransientWireRejection,
        None,
    );
    // Guard-backed revival (register-then-record, the reactor's order).
    assert_eq!(
        reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(2), true),
        1
    );
    reg.record_guard_ref(HandlerId(31), ChannelName::Ticker, &Some(sym("BTC/USD")));
    assert_eq!(
        reg.stale_release_credits(ChannelName::Ticker, &Some(sym("BTC/USD"))),
        1,
        "no bare reviver slot to retire — the dead holder stays credited"
    );
    // The dead holder's straggler is absorbed instead of tearing down the
    // revival under the live guard.
    assert_eq!(reg.release(ChannelName::Ticker, Some(sym("BTC/USD"))), 1);
    assert!(reg.has_url_entry(WsUrl::Public));
    // The guard's own drop is the real 1→0 edge.
    assert_eq!(
        reg.release_guard_ref(HandlerId(31), ChannelName::Ticker, Some(sym("BTC/USD"))),
        0
    );
    assert!(!reg.has_url_entry(WsUrl::Public));
}

/// A stale-generation guard drop is a pure no-op: no release, no liveness-epoch
/// bump, so the reactor loop does not re-derive hints for a non-mutation.
#[test]
fn stale_guard_drop_does_not_bump_epoch() {
    use crate::dispatch::HandlerId;
    let mut reg = SubscriptionRegistry::new();
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(1), false);
    reg.record_guard_ref(HandlerId(7), ChannelName::Ticker, &Some(sym("BTC/USD")));
    reg.note_terminated(
        ChannelName::Ticker,
        &Some(sym("BTC/USD")),
        TerminationCause::NonTransientWireRejection,
        None,
    );
    // Revival stamps a new generation, making the recorded ref stale.
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(2), false);
    let epoch = reg.liveness_epoch();
    assert_eq!(
        reg.release_guard_ref(HandlerId(7), ChannelName::Ticker, Some(sym("BTC/USD"))),
        1,
        "stale drop releases nothing"
    );
    assert_eq!(
        reg.liveness_epoch(),
        epoch,
        "a no-op must not trigger a hint re-derive"
    );
}

/// A revived tombstone starts a fresh sequence epoch: the dead lifetime's
/// last-seen sequence must not turn the new stream's first frames into a gap.
#[test]
fn revival_resets_the_sequence_tracker() {
    let mut reg = SubscriptionRegistry::new();
    reg.register(ex_entry(), crate::types::MonotonicInstant::now(), false);
    assert_eq!(reg.note_sequence(ChannelName::Executions, 1, true), None);
    assert_eq!(
        reg.note_sequence(ChannelName::Executions, 5, false),
        Some(3)
    );
    // Terminal reject tombstones the entry (tracker state left behind), then a
    // caller revives it — the new lifetime must not inherit last-seen 5.
    reg.note_terminated(
        ChannelName::Executions,
        &None,
        TerminationCause::NonTransientWireRejection,
        None,
    );
    reg.register(ex_entry(), crate::types::MonotonicInstant::now(), false);
    assert_eq!(
        reg.note_sequence(ChannelName::Executions, 9, false),
        None,
        "first frame of the revived lifetime seeds a fresh epoch — no false gap"
    );
    assert_eq!(reg.note_sequence(ChannelName::Executions, 10, false), None);
    assert_eq!(
        reg.note_sequence(ChannelName::Executions, 12, false),
        Some(1),
        "real gaps within the new lifetime are still detected"
    );
}

/// No entry (not subscribed) → nothing to track; a first update without a
/// snapshot seeds silently; a jump beyond u32 saturates.
#[test]
fn note_sequence_no_entry_first_seed_and_saturation() {
    let mut reg = SubscriptionRegistry::new();
    assert_eq!(reg.note_sequence(ChannelName::Executions, 5, false), None);
    reg.register(ex_entry(), crate::types::MonotonicInstant::now(), false);
    assert_eq!(
        reg.note_sequence(ChannelName::Executions, 1, false),
        None,
        "first observation seeds — what came before is unknowable"
    );
    assert_eq!(
        reg.note_sequence(ChannelName::Executions, u64::MAX, false),
        Some(u32::MAX),
        "giant delta saturates"
    );
    // The frame after a u64::MAX sequence: no overflow panic, no false gap —
    // a lower sequence is a regression reset.
    assert_eq!(reg.note_sequence(ChannelName::Executions, 1, false), None);
    assert_eq!(reg.note_sequence(ChannelName::Executions, 2, false), None);
}

fn now_at(ms: u64) -> MonotonicInstant {
    // Test-only forge: MonotonicInstant wraps a Duration since process epoch;
    // the pub(crate) field is reachable from in-crate tests.
    MonotonicInstant(std::time::Duration::from_millis(ms))
}

fn row_state(
    reg: &SubscriptionRegistry,
    channel: ChannelName,
    pair: Option<&str>,
) -> EntrySubState {
    reg.mirror
        .read()
        .unwrap()
        .get(&(channel, pair.map(sym)))
        .expect("mirror row present")
        .state
        .clone()
}

#[test]
fn register_inserts_pending_row_and_dup_keeps_first_stamp() {
    let mut reg = SubscriptionRegistry::new();
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(10), false);
    assert_eq!(
        row_state(&reg, ChannelName::Ticker, Some("BTC/USD")),
        EntrySubState::Pending
    );
    // Duplicate subscriber: state and registered_at stay the first writer's.
    reg.note_acked(ChannelName::Ticker, &Some(sym("BTC/USD")));
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(99), false);
    let m = reg.mirror.read().unwrap();
    let row = m.get(&(ChannelName::Ticker, Some(sym("BTC/USD")))).unwrap();
    assert_eq!(row.state, EntrySubState::Acked);
    assert_eq!(row.registered_at, now_at(10));
}

#[test]
fn note_acked_and_terminated_drive_states_and_projection() {
    let mut reg = SubscriptionRegistry::new();
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(1), false);
    reg.note_acked(ChannelName::Ticker, &Some(sym("BTC/USD")));
    assert_eq!(
        row_state(&reg, ChannelName::Ticker, Some("BTC/USD")),
        EntrySubState::Acked
    );
    reg.note_terminated(
        ChannelName::Ticker,
        &Some(sym("BTC/USD")),
        TerminationCause::NonTransientWireRejection,
        Some("EGeneral:Permission denied".to_string()),
    );
    let st = row_state(&reg, ChannelName::Ticker, Some("BTC/USD"));
    assert!(matches!(st, EntrySubState::Terminated { .. }));
    match SubscriptionRegistry::project_state(&st) {
        SubscriptionState::Failed(SubscribeFailureCause::NonTransientWireRejection {
            code,
            message,
        }) => {
            assert_eq!(code, "EGeneral:Permission denied");
            assert_eq!(message, code);
        }
        other => panic!("wrong projection: {other:?}"),
    }
    // Total projection map for the remaining states.
    assert_eq!(
        SubscriptionRegistry::project_state(&EntrySubState::Pending),
        SubscriptionState::Pending
    );
    assert_eq!(
        SubscriptionRegistry::project_state(&EntrySubState::Acked),
        SubscriptionState::Active
    );
    assert_eq!(
        SubscriptionRegistry::project_state(&EntrySubState::Terminated {
            cause: TerminationCause::ClientClosed,
            last_error: None,
        }),
        SubscriptionState::Failed(SubscribeFailureCause::ClientClosed)
    );
    // Notes on missing keys are no-ops, never inserts.
    reg.note_acked(ChannelName::Trade, &None);
    assert!(
        !reg.mirror
            .read()
            .unwrap()
            .contains_key(&(ChannelName::Trade, None))
    );
}

#[test]
fn tombstone_revives_pending_on_resubscribe_with_fresh_stamp() {
    let mut reg = SubscriptionRegistry::new();
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(5), false);
    reg.note_terminated(
        ChannelName::Ticker,
        &Some(sym("BTC/USD")),
        TerminationCause::NonTransientWireRejection,
        Some("boom".to_string()),
    );
    // Termination zeroed the refcount, so the re-subscribe is a fresh 0→1
    // edge and revives the mirror row to Pending with a fresh stamp.
    assert_eq!(
        reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(50), false),
        1,
        "revival restarts at count 1 — one unsubscribe fully clears a retry"
    );
    {
        let m = reg.mirror.read().unwrap();
        let row = m.get(&(ChannelName::Ticker, Some(sym("BTC/USD")))).unwrap();
        assert_eq!(row.state, EntrySubState::Pending);
        assert_eq!(row.registered_at, now_at(50));
    }
    // A single release clears the revived entry outright.
    assert_eq!(reg.release(ChannelName::Ticker, Some(sym("BTC/USD"))), 0);
    assert!(reg.is_empty());
    assert!(reg.mirror.read().unwrap().is_empty());
}

#[test]
fn release_to_zero_and_deregister_remove_the_row() {
    let mut reg = SubscriptionRegistry::new();
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(1), false);
    assert_eq!(reg.release(ChannelName::Ticker, Some(sym("BTC/USD"))), 0);
    assert!(reg.mirror.read().unwrap().is_empty());
    // Direct forced deregister also drops the row (tombstones included).
    reg.register(tk(WsUrl::Public, "ETH/USD"), now_at(2), false);
    reg.note_terminated(
        ChannelName::Ticker,
        &Some(sym("ETH/USD")),
        TerminationCause::NonTransientWireRejection,
        None,
    );
    reg.deregister(ChannelName::Ticker, Some(sym("ETH/USD")));
    assert!(reg.mirror.read().unwrap().is_empty());
}

#[test]
fn deregister_all_drains_sorted_and_clears_mirror() {
    let mut reg = SubscriptionRegistry::new();
    reg.register(tk(WsUrl::Public, "ETH/USD"), now_at(1), false);
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(2), false);
    reg.register(
        SubscriptionEntry::new(
            WsUrl::Public,
            ChannelName::Book,
            Some(sym("BTC/USD")),
            SubscribeParams::Book {
                depth: BookDepth::D10,
            },
        ),
        now_at(3),
        false,
    );
    reg.register(
        SubscriptionEntry::new(
            WsUrl::Auth,
            ChannelName::Executions,
            None,
            SubscribeParams::Executions,
        ),
        now_at(4),
        false,
    );
    // Tombstone one entry: forced removal takes it too.
    reg.note_terminated(
        ChannelName::Ticker,
        &Some(sym("ETH/USD")),
        TerminationCause::NonTransientWireRejection,
        None,
    );

    assert!(reg.is_terminated(ChannelName::Ticker, &Some(sym("ETH/USD"))));
    assert!(!reg.is_terminated(ChannelName::Ticker, &Some(sym("BTC/USD"))));

    let removed = reg.deregister_all(None);
    let kinds: Vec<RemovedEntryKind> = removed.iter().map(|(_, k)| *k).collect();
    assert_eq!(
        kinds,
        vec![
            RemovedEntryKind::Live,
            RemovedEntryKind::Live,
            RemovedEntryKind::Live,
            RemovedEntryKind::Tombstone,
        ]
    );
    let keys: Vec<(ChannelName, Option<String>)> = removed
        .iter()
        .map(|(e, _)| (e.channel, e.pair.as_ref().map(|s| s.as_str().to_string())))
        .collect();
    assert_eq!(
        keys,
        vec![
            (ChannelName::Executions, None),
            (ChannelName::Book, Some("BTC/USD".to_string())),
            (ChannelName::Ticker, Some("BTC/USD".to_string())),
            (ChannelName::Ticker, Some("ETH/USD".to_string())),
        ]
    );
    assert!(reg.is_empty());
    assert!(reg.mirror.read().unwrap().is_empty());
    assert!(reg.deregister_all(None).is_empty());
}

#[test]
fn deregister_all_channel_scoped_leaves_others() {
    let mut reg = SubscriptionRegistry::new();
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(1), false);
    reg.register(
        SubscriptionEntry::new(
            WsUrl::Public,
            ChannelName::Book,
            Some(sym("BTC/USD")),
            SubscribeParams::Book {
                depth: BookDepth::D10,
            },
        ),
        now_at(2),
        false,
    );
    let removed = reg.deregister_all(Some(ChannelName::Book));
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].0.channel, ChannelName::Book);
    assert_eq!(reg.len(), 1);
    assert!(
        reg.mirror
            .read()
            .unwrap()
            .contains_key(&(ChannelName::Ticker, Some(sym("BTC/USD"))))
    );
}

#[test]
fn tombstone_revival_takes_the_reviving_callers_params() {
    fn book(depth: BookDepth) -> SubscriptionEntry {
        SubscriptionEntry::new(
            WsUrl::Public,
            ChannelName::Book,
            Some(sym("BTC/USD")),
            SubscribeParams::Book { depth },
        )
    }
    let mut reg = SubscriptionRegistry::new();
    assert_eq!(reg.register(book(BookDepth::D10), now_at(1), false), 1);
    reg.note_terminated(
        ChannelName::Book,
        &Some(sym("BTC/USD")),
        TerminationCause::NonTransientWireRejection,
        None,
    );
    // Revival (0→1): the reviving caller's depth replaces the dead entry's —
    // the wire subscribe on this edge is composed from the new params.
    assert_eq!(reg.register(book(BookDepth::D100), now_at(2), false), 1);
    assert!(matches!(
        reg.params_for(ChannelName::Book, &Some(sym("BTC/USD"))),
        Some(SubscribeParams::Book {
            depth: BookDepth::D100
        })
    ));
    // A live duplicate stays first-writer-wins: D100 is retained.
    assert_eq!(reg.register(book(BookDepth::D500), now_at(3), false), 2);
    assert!(matches!(
        reg.params_for(ChannelName::Book, &Some(sym("BTC/USD"))),
        Some(SubscribeParams::Book {
            depth: BookDepth::D100
        })
    ));
}

#[test]
fn has_url_entry_ignores_terminated_tombstones() {
    let mut reg = SubscriptionRegistry::new();
    reg.register(
        SubscriptionEntry::new(
            WsUrl::Auth,
            ChannelName::Executions,
            None,
            SubscribeParams::Executions,
        ),
        now_at(1),
        false,
    );
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(2), false);
    assert!(reg.has_url_entry(WsUrl::Auth));
    reg.note_terminated(
        ChannelName::Executions,
        &None,
        TerminationCause::NonTransientWireRejection,
        Some("EGeneral:Permission denied".to_string()),
    );
    // All-tombstone auth registry reads EMPTY: a bare order during
    // Authenticating is the auth probe again and send-ready can assert.
    assert!(!reg.has_url_entry(WsUrl::Auth));
    // The live public entry still counts for its own url.
    assert!(reg.has_url_entry(WsUrl::Public));
}

#[test]
fn release_guard_ref_on_missing_key_consumes_record() {
    use crate::dispatch::HandlerId;
    let mut reg = SubscriptionRegistry::new();
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(1), false);
    reg.record_guard_ref(HandlerId(7), ChannelName::Ticker, &Some(sym("BTC/USD")));
    assert_eq!(reg.guard_ref_count(), 1);
    // Single-key removal while the guard is still alive.
    reg.deregister(ChannelName::Ticker, Some(sym("BTC/USD")));
    // The guard's later release consumes its record instead of leaking it.
    assert_eq!(
        reg.release_guard_ref(HandlerId(7), ChannelName::Ticker, Some(sym("BTC/USD"))),
        0
    );
    assert_eq!(reg.guard_ref_count(), 0);
    // Same invariant through the forced-teardown path.
    reg.register(tk(WsUrl::Public, "ETH/USD"), now_at(2), false);
    reg.record_guard_ref(HandlerId(8), ChannelName::Ticker, &Some(sym("ETH/USD")));
    reg.deregister_all(None);
    assert_eq!(
        reg.release_guard_ref(HandlerId(8), ChannelName::Ticker, Some(sym("ETH/USD"))),
        0
    );
    assert_eq!(reg.guard_ref_count(), 0);
}

#[test]
fn stale_guard_ref_from_dead_lifetime_cannot_release_revival() {
    use crate::dispatch::HandlerId;
    let key_pair = Some(sym("BTC/USD"));
    let mut reg = SubscriptionRegistry::new();
    // Guard A registers and records its ref against the first generation.
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(1), false);
    reg.record_guard_ref(HandlerId(7), ChannelName::Ticker, &key_pair);
    // Terminal reject tombstones the entry; A is now a dead-lifetime ref.
    reg.note_terminated(
        ChannelName::Ticker,
        &key_pair,
        TerminationCause::NonTransientWireRejection,
        None,
    );
    // Caller C revives the key: new generation, count 1, fresh ref.
    assert_eq!(
        reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(2), false),
        1
    );
    reg.record_guard_ref(HandlerId(9), ChannelName::Ticker, &key_pair);
    // A's late drop releases NOTHING — the revival stays live at count 1.
    assert_eq!(
        reg.release_guard_ref(HandlerId(7), ChannelName::Ticker, key_pair.clone()),
        1
    );
    assert!(reg.params_for(ChannelName::Ticker, &key_pair).is_some());
    assert!(!reg.is_terminated(ChannelName::Ticker, &key_pair));
    // C's own drop is the real 1→0: entry and mirror row are gone.
    assert_eq!(
        reg.release_guard_ref(HandlerId(9), ChannelName::Ticker, key_pair.clone()),
        0
    );
    assert!(reg.params_for(ChannelName::Ticker, &key_pair).is_none());
}

/// Credits survive a RE-TERMINATION as a separate ledger, so a prior lifetime's bare
/// stragglers are absorbed instead of counting the NEW tombstone down before its own holder
/// released (T5).
#[test]
fn reterminated_tombstone_absorbs_prior_lifetime_stragglers() {
    let key = Some(sym("BTC/USD"));
    let mut reg = SubscriptionRegistry::new();
    // Lifetime 1: bare holders A and B; terminate.
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(1), false);
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(2), false);
    reg.note_terminated(
        ChannelName::Ticker,
        &key,
        TerminationCause::NonTransientWireRejection,
        None,
    );
    assert_eq!(reg.tombstone_holders(ChannelName::Ticker, &key), 2);
    // A foreign bare caller revives: 2 dead holders − its own presumed slot.
    assert_eq!(
        reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(3), false),
        1
    );
    assert_eq!(reg.stale_release_credits(ChannelName::Ticker, &key), 1);
    // The revived lifetime terminates too: the countdown is ITS holder only,
    // and the outstanding credit SURVIVES beside it.
    reg.note_terminated(
        ChannelName::Ticker,
        &key,
        TerminationCause::NonTransientWireRejection,
        None,
    );
    assert_eq!(reg.tombstone_holders(ChannelName::Ticker, &key), 1);
    assert_eq!(
        reg.stale_release_credits(ChannelName::Ticker, &key),
        1,
        "a prior lifetime's credit is not discarded by re-termination"
    );
    // A's stale release is absorbed by the credit — the tombstone stays
    // visible with its own holder still outstanding.
    assert_eq!(reg.release(ChannelName::Ticker, key.clone()), 0);
    assert!(
        reg.is_terminated(ChannelName::Ticker, &key),
        "a dead-lifetime straggler must not clear the new tombstone"
    );
    assert_eq!(reg.tombstone_holders(ChannelName::Ticker, &key), 1);
    assert_eq!(reg.stale_release_credits(ChannelName::Ticker, &key), 0);
    // Only a genuine holder release counts it down to removal.
    assert_eq!(reg.release(ChannelName::Ticker, key.clone()), 0);
    assert!(reg.params_for(ChannelName::Ticker, &key).is_none());
}

/// The credit ledger ACCUMULATES across successive revivals: a second revival must not
/// overwrite credits still owed to an older lifetime's stragglers, or their release tears the
/// new live subscription down.
#[test]
fn credits_accumulate_across_successive_revivals() {
    let key = Some(sym("BTC/USD"));
    let mut reg = SubscriptionRegistry::new();
    // Lifetime 1: bare holders A and B; terminate.
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(1), false);
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(2), false);
    reg.note_terminated(
        ChannelName::Ticker,
        &key,
        TerminationCause::NonTransientWireRejection,
        None,
    );
    // Lifetime 2: a foreign bare reviver; one credit owed to A/B.
    assert_eq!(
        reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(3), false),
        1
    );
    assert_eq!(reg.stale_release_credits(ChannelName::Ticker, &key), 1);
    // Lifetime 2 dies; its own holder is the countdown, the credit survives.
    reg.note_terminated(
        ChannelName::Ticker,
        &key,
        TerminationCause::NonTransientWireRejection,
        None,
    );
    assert_eq!(reg.stale_release_credits(ChannelName::Ticker, &key), 1);
    // Lifetime 3: another foreign bare reviver. Its own conversion adds 0
    // (one dead holder, one reviver slot), but the OLDER credit must persist.
    assert_eq!(
        reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(4), false),
        1
    );
    assert_eq!(
        reg.stale_release_credits(ChannelName::Ticker, &key),
        1,
        "a later revival must not overwrite credits owed to an older lifetime"
    );
    // A's ancient release is absorbed; lifetime 3 stays live.
    assert_eq!(reg.release(ChannelName::Ticker, key.clone()), 1);
    assert!(
        reg.params_for(ChannelName::Ticker, &key).is_some(),
        "an older lifetime's straggler must not tear down the live revival"
    );
    assert!(!reg.is_terminated(ChannelName::Ticker, &key));
}

/// A stale guard drop against a RE-TERMINATED key returns the tombstone's 0
/// while releasing nothing — the holder countdown must not move.
#[test]
fn stale_guard_drop_on_reterminated_tombstone_releases_nothing() {
    use crate::dispatch::HandlerId;
    let key_pair = Some(sym("BTC/USD"));
    let mut reg = SubscriptionRegistry::new();
    // Guard A on lifetime 1; terminal reject; bare B revives (new generation).
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(1), false);
    reg.record_guard_ref(HandlerId(7), ChannelName::Ticker, &key_pair);
    reg.note_terminated(
        ChannelName::Ticker,
        &key_pair,
        TerminationCause::NonTransientWireRejection,
        None,
    );
    assert_eq!(
        reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(2), false),
        1
    );
    // Lifetime 2 dies too: B is the lone tombstone holder.
    reg.note_terminated(
        ChannelName::Ticker,
        &key_pair,
        TerminationCause::NonTransientWireRejection,
        None,
    );
    assert_eq!(reg.tombstone_holders(ChannelName::Ticker, &key_pair), 1);
    // A's ancient drop returns the tombstone's 0 but must not count B down.
    assert_eq!(
        reg.release_guard_ref(HandlerId(7), ChannelName::Ticker, key_pair.clone()),
        0
    );
    assert_eq!(
        reg.tombstone_holders(ChannelName::Ticker, &key_pair),
        1,
        "a stale drop must not consume a tombstone-holder slot"
    );
    assert!(reg.is_terminated(ChannelName::Ticker, &key_pair));
}

#[test]
fn liveness_epoch_bumps_on_every_liveness_mutation() {
    let mut reg = SubscriptionRegistry::new();
    let e0 = reg.liveness_epoch();
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(1), false);
    let e1 = reg.liveness_epoch();
    assert!(e1 > e0, "register bumps");
    reg.note_terminated(
        ChannelName::Ticker,
        &Some(sym("BTC/USD")),
        TerminationCause::NonTransientWireRejection,
        None,
    );
    let e2 = reg.liveness_epoch();
    assert!(
        e2 > e1,
        "note_terminated bumps — the hint sync must see tombstones"
    );
    reg.deregister(ChannelName::Ticker, Some(sym("BTC/USD")));
    let e3 = reg.liveness_epoch();
    assert!(e3 > e2, "deregister bumps");
    reg.register(tk(WsUrl::Public, "ETH/USD"), now_at(2), false);
    let e4 = reg.liveness_epoch();
    reg.release(ChannelName::Ticker, Some(sym("ETH/USD")));
    assert!(reg.liveness_epoch() > e4, "release bumps");
    reg.register(tk(WsUrl::Public, "SOL/USD"), now_at(3), false);
    let e5 = reg.liveness_epoch();
    reg.deregister_all(None);
    assert!(reg.liveness_epoch() > e5, "deregister_all bumps");
}

#[test]
fn reconnect_replay_skips_terminated_tombstones() {
    // A Failed tombstone is revived only by a caller re-subscribe — the reconnect replay
    // composer must never resurrect it (and never re-reject it into a second terminal event
    let mut reg = SubscriptionRegistry::new();
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(1), false);
    reg.register(tk(WsUrl::Public, "ETH/USD"), now_at(2), false);
    reg.note_terminated(
        ChannelName::Ticker,
        &Some(sym("ETH/USD")),
        TerminationCause::NonTransientWireRejection,
        Some("EGeneral:Permission denied".to_string()),
    );
    let frames = reg.compose_subscribe_frames_for_url(WsUrl::Public);
    assert_eq!(frames.len(), 1, "tombstone must be excluded from replay");
    assert_eq!(frames[0].1, Some(sym("BTC/USD")));
    // The tombstone itself is untouched: still Failed until a caller acts.
    assert!(matches!(
        row_state(&reg, ChannelName::Ticker, Some("ETH/USD")),
        EntrySubState::Terminated { .. }
    ));
}

/// A stray bare release cannot consume a guard holder's slot: with every remaining holder
/// guard-backed, the release is a pure no-op — count, key, and epoch untouched.
#[test]
fn bare_release_cannot_consume_guard_held_slot() {
    let mut reg = SubscriptionRegistry::new();
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(1), true);
    reg.record_guard_ref(HandlerId(41), ChannelName::Ticker, &Some(sym("BTC/USD")));
    let epoch = reg.liveness_epoch();
    assert_eq!(
        reg.release(ChannelName::Ticker, Some(sym("BTC/USD"))),
        1,
        "stray bare release must not decrement a guard-held count"
    );
    assert!(
        reg.params_for(ChannelName::Ticker, &Some(sym("BTC/USD")))
            .is_some(),
        "guard-held subscription survives the stray release"
    );
    assert_eq!(reg.liveness_epoch(), epoch, "pure no-op: no epoch bump");
    // The guard's own matched drop still tears down normally.
    assert_eq!(
        reg.release_guard_ref(HandlerId(41), ChannelName::Ticker, Some(sym("BTC/USD"))),
        0
    );
    assert!(
        reg.params_for(ChannelName::Ticker, &Some(sym("BTC/USD")))
            .is_none()
    );
}

/// The gate covers channel-wide (`pair = None`) entries the same way: a stray
/// bare release cannot consume a guard-held `executions` ref.
#[test]
fn bare_release_cannot_consume_guard_held_channel_wide_slot() {
    let mut reg = SubscriptionRegistry::new();
    reg.register(
        SubscriptionEntry::new(
            WsUrl::Auth,
            ChannelName::Executions,
            None,
            SubscribeParams::Executions,
        ),
        now_at(1),
        true,
    );
    reg.record_guard_ref(HandlerId(44), ChannelName::Executions, &None);
    assert_eq!(
        reg.release(ChannelName::Executions, None),
        1,
        "stray bare release must not decrement a guard-held channel-wide count"
    );
    assert!(reg.params_for(ChannelName::Executions, &None).is_some());
    // The guard's own matched drop still tears down normally.
    assert_eq!(
        reg.release_guard_ref(HandlerId(44), ChannelName::Executions, None),
        0
    );
    assert!(reg.params_for(ChannelName::Executions, &None).is_none());
}

/// A mixed entry (guard + plain subscriber) absorbs exactly one bare release —
/// the plain subscriber's own — and no-ops the surplus one.
#[test]
fn surplus_bare_release_stops_at_the_guard_backed_floor() {
    let mut reg = SubscriptionRegistry::new();
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(1), true);
    reg.record_guard_ref(HandlerId(42), ChannelName::Ticker, &Some(sym("BTC/USD")));
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(2), false);
    assert_eq!(
        reg.release(ChannelName::Ticker, Some(sym("BTC/USD"))),
        1,
        "the plain subscriber's own release decrements"
    );
    assert_eq!(
        reg.release(ChannelName::Ticker, Some(sym("BTC/USD"))),
        1,
        "the surplus release no-ops at the guard-backed floor"
    );
    assert!(
        reg.params_for(ChannelName::Ticker, &Some(sym("BTC/USD")))
            .is_some()
    );
}

/// A stray bare release cannot drain a tombstone countdown made of guard
/// holders: the Failed row stays visible until its own holder releases.
#[test]
fn bare_release_cannot_drain_guard_backed_tombstone() {
    let mut reg = SubscriptionRegistry::new();
    reg.register(tk(WsUrl::Public, "BTC/USD"), now_at(1), true);
    reg.record_guard_ref(HandlerId(43), ChannelName::Ticker, &Some(sym("BTC/USD")));
    reg.note_terminated(
        ChannelName::Ticker,
        &Some(sym("BTC/USD")),
        TerminationCause::NonTransientWireRejection,
        None,
    );
    reg.release(ChannelName::Ticker, Some(sym("BTC/USD")));
    assert_eq!(
        reg.tombstone_holders(ChannelName::Ticker, &Some(sym("BTC/USD"))),
        1,
        "stray bare release must not count down a guard-backed tombstone"
    );
    assert!(
        reg.is_terminated(ChannelName::Ticker, &Some(sym("BTC/USD"))),
        "Failed row stays visible for its guard holder"
    );
    // The guard holder's own drop still counts it down and clears the row.
    reg.release_guard_ref(HandlerId(43), ChannelName::Ticker, Some(sym("BTC/USD")));
    assert!(
        reg.params_for(ChannelName::Ticker, &Some(sym("BTC/USD")))
            .is_none()
    );
}
