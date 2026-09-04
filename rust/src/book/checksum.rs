//! CRC32 for Spot WS v2 book-channel checksum validation.
//! Construction and CRC variant: docs/guides/order-book.md.

/// Per-level wire strings as Kraken sent them. `(price_str, qty_str)`.
/// Callers MUST pass original wire strings — not `Decimal::to_string()`.
pub type WireLevel<'a> = (&'a str, &'a str);

/// Levels per side the checksum covers — Kraken CRCs the top 10 regardless of
/// subscribed depth. The maintained book must retain STRICTLY MORE (promotion headroom).
pub(crate) const CHECKSUM_DEPTH: usize = 10;

/// Canonical book-channel CRC32. `asks` lowest-first, `bids` highest-first, each
/// capped at 10; the caller sorts. Pure, zero-alloc on the hot path.
pub fn compute_book_crc32<'a, A, B>(asks_top_n: A, bids_top_n: B) -> u32
where
    A: IntoIterator<Item = WireLevel<'a>>,
    B: IntoIterator<Item = WireLevel<'a>>,
{
    let mut hasher = crc32fast::Hasher::new();
    for (price, qty) in asks_top_n {
        update_stripped(&mut hasher, price);
        update_stripped(&mut hasher, qty);
    }
    for (price, qty) in bids_top_n {
        update_stripped(&mut hasher, price);
        update_stripped(&mut hasher, qty);
    }
    hasher.finalize()
}

/// Strip buffer chunk size — 32 covers real wire strings in one chunk.
const STRIP_BUF_LEN: usize = 32;

/// Strip `.` and leading zeros, feed through a fixed stack buffer (CRC32 is chunk-invariant).
fn update_stripped(hasher: &mut crc32fast::Hasher, s: &str) {
    let mut buf = [0u8; STRIP_BUF_LEN];
    let mut len = 0usize;
    let mut leading = true;
    for b in s.bytes() {
        if b == b'.' {
            continue;
        }
        if leading && b == b'0' {
            continue;
        }
        leading = false;
        if len == STRIP_BUF_LEN {
            hasher.update(&buf);
            len = 0;
        }
        buf[len] = b;
        len += 1;
    }
    hasher.update(&buf[..len]);
}

#[cfg(test)]
#[path = "checksum_tests.rs"]
mod tests;
