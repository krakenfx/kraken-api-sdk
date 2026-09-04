//! Maintained order-book state and wire-frame primitives. Wire decimal strings
//! stay alongside typed `Decimal` so CRC32 matches Kraken; mismatch → resubscribe.

mod builder;
mod checksum;
mod wire;

pub(crate) use builder::{ApplyError, OrderBookBuilder};
pub use builder::{BookDelta, BookLevel, OrderBookUpdate, PriceLevel};
pub use checksum::compute_book_crc32;
pub(crate) use wire::{parse_book_frame, parse_book_frame_owned};
