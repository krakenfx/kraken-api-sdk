//! Bounded stateful sweep over the subscription accounting.
//! The registry must attribute ANONYMOUS bare releases to one of several
//! overlapping lifetimes of the same key, using only scalars it can observe —

use super::*;

const DEPTH: usize = 7;

fn key_pair() -> Option<Symbol> {
    Some(Symbol::new("BTC/USD").unwrap())
}

fn entry() -> SubscriptionEntry {
    SubscriptionEntry::new(
        WsUrl::Public,
        ChannelName::Ticker,
        key_pair(),
        SubscribeParams::Ticker {
            snapshot: None,
            event_trigger: None,
        },
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    /// Bare `subscribe_*`: a holder with NO identity on the release side.
    RegisterBare,
    /// `on_*_for` combiner: mints a fresh id, generation-validated on drop.
    RegisterGuard,
    /// Bare `unsubscribe_*` matching an outstanding hold (oldest first) —
    /// anonymous on the wire; the matching exists only in the model.
    ReleaseBare,
    /// Bare `unsubscribe_*` with NO outstanding hold behind it: the stray /
    /// surplus / double-called release the API cannot tell from a real one.
    ReleaseBareStray,
    /// Drop the earliest-registered guard still held (a stale-generation drop
    /// after a revival, in the interesting sequences).
    DropOldestGuard,
    /// Drop the latest-registered guard still held.
    DropNewestGuard,
    /// Terminal wire reject.
    Terminate,
}

const ALPHABET: [Op; 7] = [
    Op::RegisterBare,
    Op::RegisterGuard,
    Op::ReleaseBare,
    Op::ReleaseBareStray,
    Op::DropOldestGuard,
    Op::DropNewestGuard,
    Op::Terminate,
];

/// The ruled policy, restated as an executable model: every bare hold tagged with the lifetime
/// it joined, plus mirrors of the registry's scalars so the invariants stay EQUALITIES against
/// the ruled formula.
#[derive(Debug, Clone, Default)]
struct Model {
    /// Current entry generation; holders of one lifetime share it.
    lifetime: u64,
    /// Idealised per-holder ledger: the lifetime each outstanding bare hold joined, oldest
    /// first.
    bare_holds: Vec<u64>,
    /// Bare holders of the CURRENT lifetime, still to release (registry mirror).
    live_bare: usize,
    /// Guards the caller still holds: (id, the lifetime its row recorded).
    /// A guard whose lifetime != `lifetime` is stale — its drop no-ops.
    guards: Vec<(u8, u64)>,
    /// Bare holders of the CURRENT tombstone that have not released yet.
    tomb_bare: usize,
    /// Guard holders of the CURRENT tombstone that have not released yet.
    tomb_guard: usize,
    /// What the canon formula says the credit ledger must hold.
    expected_credits: usize,
    terminated: bool,
    absent: bool,
    next_id: u8,
    /// Latched when a stray or cross-lifetime bare release consumes a real live/tombstone slot
    /// (not absorbed by a credit or the gate): the ruled-open bare-vs-bare misattribution.
    known_open: bool,
}

impl Model {
    /// Guards recorded against the CURRENT lifetime (generation-matching).
    fn live_guards(&self) -> usize {
        self.guards
            .iter()
            .filter(|(_, g)| *g == self.lifetime)
            .count()
    }

    /// Holders of the current LIVE lifetime — the population whose existence
    /// forbids a teardown.
    fn live_holders(&self) -> usize {
        self.live_bare + self.live_guards()
    }

    /// Holders the current tombstone is still waiting on.
    fn tombstone_owed(&self) -> usize {
        self.tomb_bare + self.tomb_guard
    }
}

fn check(reg: &SubscriptionRegistry, m: &Model, trace: &[Op]) {
    let k = key_pair();
    let ctx = || format!("after {trace:?}");
    let present = reg.params_for(ChannelName::Ticker, &k).is_some();

    if !present {
        // I1 — THE SAFETY INVARIANT.
        assert!(
            m.live_guards() == 0,
            "I1 key removed while {} generation-matching guard holder(s) remained {}",
            m.live_guards(),
            ctx()
        );
        // Bare holders may be stranded ONLY behind a recorded misattribution
        // (`known_open`) — the ruled-open bare-vs-bare class.
        let cur_holds = m.bare_holds.iter().filter(|t| **t == m.lifetime).count();
        assert!(
            cur_holds == 0 || m.known_open,
            "I1 key removed while {cur_holds} bare holder(s) of the removed lifetime \
             remained, with no misattribution recorded {}",
            ctx()
        );
        assert!(
            m.tombstone_owed() == 0 || m.known_open,
            "I1 tombstone removed before its own {} holder(s) released {}",
            m.tombstone_owed(),
            ctx()
        );
        // I2 — a removed key keeps no accounting residue.
        assert_eq!(
            reg.tombstone_holders(ChannelName::Ticker, &k),
            0,
            "I2 tombstone countdown survived key removal {}",
            ctx()
        );
        assert_eq!(
            reg.stale_release_credits(ChannelName::Ticker, &k),
            0,
            "I2 credits survived key removal {}",
            ctx()
        );
        assert!(
            !reg.has_url_entry(WsUrl::Public),
            "I2 removed key still counts for liveness {}",
            ctx()
        );
        return;
    }

    let is_tomb = reg.is_terminated(ChannelName::Ticker, &k);

    // I3 — tombstone state agrees with the model.
    assert_eq!(
        is_tomb,
        m.terminated,
        "I3 tombstone state disagrees with the model {}",
        ctx()
    );

    // I4 — live holders exist ⇒ the key is live and counts for liveness.
    if !m.terminated && m.live_holders() > 0 {
        assert!(
            !is_tomb,
            "I4 live holders exist but the entry is a tombstone {}",
            ctx()
        );
        assert!(
            reg.has_url_entry(WsUrl::Public),
            "I4 live holders exist but the key is absent for liveness {}",
            ctx()
        );
    }

    // I5 — THE LEDGER EQUALS THE FORMULA.
    assert_eq!(
        reg.stale_release_credits(ChannelName::Ticker, &k) as usize,
        m.expected_credits,
        "I5 credit ledger diverged from the ruled formula {}",
        ctx()
    );

    // I6 — the tombstone countdown equals its own lifetime's outstanding holders.
    if is_tomb {
        assert_eq!(
            reg.tombstone_holders(ChannelName::Ticker, &k) as usize,
            m.tombstone_owed(),
            "I6 tombstone countdown diverged from its own holders {}",
            ctx()
        );
    }

    // I7 — no guard-ref row outlives the guards the caller still holds.
    assert!(
        reg.guard_ref_count() <= m.guards.len(),
        "I7 registry holds more guard refs than the caller holds guards {}",
        ctx()
    );
}

fn step(reg: &mut SubscriptionRegistry, m: &mut Model, op: Op, clock: &mut u64) {
    let k = key_pair();
    *clock += 1;
    let now = MonotonicInstant(std::time::Duration::from_secs(*clock));
    match op {
        Op::RegisterBare | Op::RegisterGuard => {
            let guard_backed = op == Op::RegisterGuard;
            if m.absent {
                m.absent = false;
                m.lifetime += 1;
            } else if m.terminated {
                // REVIVAL: the countdown converts to credits minus the dying lifetime's guard
                // rows and a bare reviver's reserved slot; ACCUMULATE.
                let dying = m.lifetime;
                let dying_guard_rows = m.guards.iter().filter(|(_, g)| *g == dying).count();
                let reviver_slot = usize::from(!guard_backed);
                let owed = m
                    .tombstone_owed()
                    .saturating_sub(dying_guard_rows)
                    .saturating_sub(reviver_slot);
                m.expected_credits += owed;
                m.tomb_bare = 0;
                m.tomb_guard = 0;
                m.terminated = false;
                m.lifetime += 1;
            }
            reg.register(entry(), now, guard_backed);
            if guard_backed {
                let id = m.next_id;
                m.next_id += 1;
                m.guards.push((id, m.lifetime));
                reg.record_guard_ref(HandlerId(u64::from(id)), ChannelName::Ticker, &k);
            } else {
                m.live_bare += 1;
                m.bare_holds.push(m.lifetime);
            }
        }
        Op::ReleaseBare => {
            // Matched: consumes the OLDEST outstanding hold — possibly a dead
            // lifetime's straggler, which the credit ledger exists to absorb.
            let tag = (!m.bare_holds.is_empty()).then(|| m.bare_holds.remove(0));
            release_bare(reg, m, tag);
        }
        Op::ReleaseBareStray => release_bare(reg, m, None),
        Op::DropOldestGuard | Op::DropNewestGuard => {
            if !m.guards.is_empty() {
                let idx = if op == Op::DropOldestGuard {
                    0
                } else {
                    m.guards.len() - 1
                };
                let (id, guard_gen) = m.guards.remove(idx);
                // A generation-MATCHING drop releases (and never burns a credit); a
                // stale-generation drop is a pure no-op.
                if guard_gen == m.lifetime && !m.absent && m.terminated {
                    m.tomb_guard = m.tomb_guard.saturating_sub(1);
                }
                reg.release_guard_ref(HandlerId(u64::from(id)), ChannelName::Ticker, k.clone());
            }
        }
        Op::Terminate => {
            if !m.absent && !m.terminated {
                m.terminated = true;
                m.tomb_bare = m.live_bare;
                m.tomb_guard = m.live_guards();
                m.live_bare = 0;
            }
            reg.note_terminated(
                ChannelName::Ticker,
                &k,
                TerminationCause::NonTransientWireRejection,
                None,
            );
        }
    }
}

/// Mirror one anonymous release through the registry's arms — credit first, then the bare-slot
/// gate, then the live/tombstone decrement — latching `known_open` when a stray or
/// cross-lifetime release consumes a real slot.
fn release_bare(reg: &mut SubscriptionRegistry, m: &mut Model, tag: Option<u64>) {
    let foreign = tag != Some(m.lifetime);
    if m.expected_credits > 0 {
        m.expected_credits -= 1;
    } else if m.absent {
        // Missing key: idempotent no-op.
    } else if m.terminated {
        if m.tomb_bare > 0 {
            m.tomb_bare -= 1;
            if foreign {
                m.known_open = true;
            }
        }
        // else: the remaining countdown is all guard-backed — the gate no-ops.
    } else if m.live_bare > 0 {
        m.live_bare -= 1;
        if foreign {
            m.known_open = true;
        }
    }
    // else: every live holder is guard-backed — the gate no-ops.
    reg.release(ChannelName::Ticker, key_pair());
}

/// After the invariants are checked, adopt the registry's removal timing (deferred removal is
/// the ruled lingering direction; credits die with the key).
fn collapse(reg: &SubscriptionRegistry, m: &mut Model) {
    if reg.params_for(ChannelName::Ticker, &key_pair()).is_none() {
        m.absent = true;
        m.terminated = false;
        m.live_bare = 0;
        m.tomb_bare = 0;
        m.tomb_guard = 0;
        m.expected_credits = 0;
    }
}

/// Is `op` generable in this state?
fn enabled(op: Op, m: &Model) -> bool {
    match op {
        Op::ReleaseBare => !m.bare_holds.is_empty(),
        Op::ReleaseBareStray => true,
        Op::DropOldestGuard | Op::DropNewestGuard => !m.guards.is_empty(),
        _ => true,
    }
}

/// Run one sequence from a fresh registry, checking after every step.
fn drive(ops: &[Op]) -> bool {
    let mut reg = SubscriptionRegistry::new();
    let mut model = Model::default();
    let mut clock = 0u64;
    for (i, op) in ops.iter().enumerate() {
        if !enabled(*op, &model) {
            return false;
        }
        step(&mut reg, &mut model, *op, &mut clock);
        check(&reg, &model, &ops[..=i]);
        collapse(&reg, &mut model);
    }
    true
}

/// Exhaustive breadth: every sequence up to `DEPTH`. Deterministic, so the
/// first failure is a minimal trace.
#[test]
fn sweep_accounting_invariants_hold_over_every_short_sequence() {
    fn recurse(prefix: &mut Vec<Op>, depth: usize, sequences: &mut u64) {
        if depth == 0 {
            return;
        }
        for op in ALPHABET {
            prefix.push(op);
            if drive(prefix) {
                *sequences += 1;
                recurse(prefix, depth - 1, sequences);
            }
            prefix.pop();
        }
    }

    let mut prefix = Vec::with_capacity(DEPTH);
    let mut sequences = 0u64;
    recurse(&mut prefix, DEPTH, &mut sequences);
    // Guard the guard: a silently-empty sweep would pass vacuously.
    assert!(
        sequences > 100_000,
        "sweep covered only {sequences} sequences — alphabet or depth regressed"
    );
}

/// Exhaustive DEPTH-first breadth is too shallow for the defects that need several lifetimes
/// before the harm is even observable (a credit owed by lifetime 1 surviving to lifetime 3,
/// say).
#[test]
fn sweep_deep_multi_lifetime_lineages() {
    use Op::{
        DropOldestGuard, RegisterBare, RegisterGuard, ReleaseBare, ReleaseBareStray, Terminate,
    };
    // Each prefix leaves ≥1 dead lifetime with un-released bare holders, so the
    // credit ledger is non-empty and cross-lifetime attribution is in play.
    const PREFIXES: [&[Op]; 6] = [
        // Two bare holders, terminate, foreign bare revival, terminate again:
        // the re-terminated tombstone with a surviving ledger.
        &[
            RegisterBare,
            RegisterBare,
            Terminate,
            RegisterBare,
            Terminate,
        ],
        // Same, then a third lifetime — the ledger must survive two revivals.
        &[
            RegisterBare,
            RegisterBare,
            Terminate,
            RegisterBare,
            Terminate,
            RegisterBare,
        ],
        // Three bare holders so the ledger carries more than one credit.
        &[
            RegisterBare,
            RegisterBare,
            RegisterBare,
            Terminate,
            RegisterBare,
        ],
        // A guard held ACROSS a termination boundary, then a revival: its row
        // is stale and must not deflate the ledger or count the tombstone down.
        &[RegisterGuard, RegisterBare, Terminate, RegisterBare],
        // Guard-backed revival: no bare reviver slot is reserved.
        &[RegisterBare, RegisterBare, Terminate, RegisterGuard],
        // A guard surviving two lifetimes, with a straggler still owed.
        &[
            RegisterGuard,
            RegisterBare,
            Terminate,
            RegisterBare,
            Terminate,
            RegisterBare,
        ],
    ];
    // Continuations are drawn from the release-heavy end of the alphabet: the harm always
    // surfaces on a release, and a register only adds lifetimes the prefixes already cover.
    const CONT: [Op; 5] = [
        ReleaseBare,
        ReleaseBareStray,
        DropOldestGuard,
        Terminate,
        RegisterBare,
    ];
    const CONT_DEPTH: usize = 5;

    fn recurse(seq: &mut Vec<Op>, depth: usize, count: &mut u64) {
        if depth == 0 {
            return;
        }
        for op in CONT {
            seq.push(op);
            if drive(seq) {
                *count += 1;
                recurse(seq, depth - 1, count);
            }
            seq.pop();
        }
    }

    let mut count = 0u64;
    for prefix in PREFIXES {
        let mut seq = prefix.to_vec();
        recurse(&mut seq, CONT_DEPTH, &mut count);
    }
    assert!(
        count > 10_000,
        "deep sweep covered only {count} sequences — prefixes or depth regressed"
    );
}

/// KNOWN-OPEN over-release, pinned: a still-owed bare release lands on the key's next lifetime
/// and tears it down.
#[test]
fn orphaned_straggler_residue_is_reproducible() {
    let k = key_pair();
    let mut reg = SubscriptionRegistry::new();
    let now = |t: u64| MonotonicInstant(std::time::Duration::from_secs(t));
    // Lifetime 1: two bare holders; A never releases.
    reg.register(entry(), now(1), false);
    reg.register(entry(), now(2), false);
    // Both released -> key removed, and any ledger dies with it.
    assert_eq!(reg.release(ChannelName::Ticker, k.clone()), 1);
    assert_eq!(reg.release(ChannelName::Ticker, k.clone()), 0);
    assert!(reg.params_for(ChannelName::Ticker, &k).is_none());
    // Lifetime 2 takes the key on the fresh-insert path: no credits are minted.
    reg.register(entry(), now(3), false);
    assert_eq!(reg.stale_release_credits(ChannelName::Ticker, &k), 0);
    // A third (unmatched, from the caller's perspective legitimate) release now
    // lands on lifetime 2 and removes it.
    assert_eq!(reg.release(ChannelName::Ticker, k.clone()), 0);
    assert!(
        reg.params_for(ChannelName::Ticker, &k).is_none(),
        "documents the residue: an orphaned release tears down a later lifetime"
    );
}

/// KNOWN-OPEN reviver shortfall, pinned: the reserved reviver slot assumes a returning dead
/// holder whose release is still owed; a distinct (or already- released) reviver leaves the
/// ledger one credit short.
#[test]
fn reviver_shortfall_residue_is_reproducible() {
    let k = key_pair();
    let mut reg = SubscriptionRegistry::new();
    let now = |t: u64| MonotonicInstant(std::time::Duration::from_secs(t));
    // Lifetime 1: A and B subscribe bare; the pair is rejected non-transiently.
    reg.register(entry(), now(1), false);
    reg.register(entry(), now(2), false);
    reg.note_terminated(
        ChannelName::Ticker,
        &k,
        TerminationCause::NonTransientWireRejection,
        None,
    );
    // A cleans up after the failure (counts the tombstone down 2 -> 1)...
    assert_eq!(reg.release(ChannelName::Ticker, k.clone()), 0);
    // ...then retries. The revival reserves the reviver slot for A's own
    // countdown slot — already consumed above — so B's straggler gets no credit.
    reg.register(entry(), now(3), false);
    assert_eq!(reg.stale_release_credits(ChannelName::Ticker, &k), 0);
    // B's single, matched cleanup release lands on A's healthy retry.
    assert_eq!(reg.release(ChannelName::Ticker, k.clone()), 0);
    assert!(
        reg.params_for(ChannelName::Ticker, &k).is_none(),
        "documents the shortfall: the dead lifetime's last straggler tears down the revival"
    );
}
