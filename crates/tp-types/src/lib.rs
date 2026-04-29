//! Wire-format types and shared helpers for the rdb tickerplant prototype.
//!
//! The two record types are POD (`bytemuck::Pod`) and `repr(C)`, with explicit
//! padding so they can be transmitted byte-for-byte over iceoryx2 shared
//! memory without per-field serialization. They are also marked
//! [`iceoryx2::prelude::ZeroCopySend`] so they can be carried by an
//! iceoryx2 publish/subscribe service.

use std::time::{SystemTime, UNIX_EPOCH};

use bytemuck::{Pod, Zeroable};
use iceoryx2::prelude::ZeroCopySend;
use serde::{Deserialize, Serialize};

pub mod metrics;
pub mod query_proto;
pub mod ipc_cfg;

pub mod topics {
    //! Canonical iceoryx2 service names used across the prototype.
    pub const TRADES_RAW: &str = "rdb/trades/raw";
    pub const QUOTES_RAW: &str = "rdb/quotes/raw";
    pub const BOOK_L2_RAW: &str = "rdb/book_l2/raw";
    pub const TRADES_AGG: &str = "rdb/trades/agg";
    pub const QUOTES_AGG: &str = "rdb/quotes/agg";
    pub const BOOK_L2_AGG: &str = "rdb/book_l2/agg";
}

/// Side of a trade. Wire value matches [`Trade::side`].
#[repr(u8)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy = 0,
    Sell = 1,
}

impl Side {
    pub fn as_str(self) -> &'static str {
        match self {
            Side::Buy => "buy",
            Side::Sell => "sell",
        }
    }
    pub fn from_u8(v: u8) -> Side {
        if v == 0 { Side::Buy } else { Side::Sell }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
pub struct Trade {
    pub seq: u64,
    pub ts_exchange_ns: u64,
    pub ts_local_ns: u64,
    pub symbol_id: u32,
    pub _pad0: u32,
    pub price: i64,
    pub qty: i64,
    pub side: u8,
    pub _pad1: [u8; 7],
}

unsafe impl ZeroCopySend for Trade {
    unsafe fn type_name() -> &'static str {
        "rdb::Trade::v1"
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
pub struct QuoteL1 {
    pub seq: u64,
    pub ts_exchange_ns: u64,
    pub ts_local_ns: u64,
    pub symbol_id: u32,
    pub _pad: u32,
    pub bid_price: i64,
    pub bid_qty: i64,
    pub ask_price: i64,
    pub ask_qty: i64,
}

unsafe impl ZeroCopySend for QuoteL1 {
    unsafe fn type_name() -> &'static str {
        "rdb::QuoteL1::v1"
    }
}

/// Number of price levels carried per side in [`BookL2`]. Fixed at compile
/// time so the wire record stays a `repr(C)` POD with deterministic size.
/// Bumping this is a breaking wire change; bump `type_name` accordingly.
pub const BOOK_L2_LEVELS: usize = 5;

/// L2 order-book snapshot for a single symbol, top-of-book + 4 deeper
/// levels per side. 192 bytes when `BOOK_L2_LEVELS = 5`.
///
/// Levels are ordered from best to worst: index 0 = best bid / best ask.
/// Unused trailing levels carry `price = 0` and `qty = 0` (a publisher
/// with shallower depth zero-pads the tail).
#[repr(C)]
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
pub struct BookL2 {
    pub seq: u64,
    pub ts_exchange_ns: u64,
    pub ts_local_ns: u64,
    pub symbol_id: u32,
    pub _pad: u32,
    pub bid_prices: [i64; BOOK_L2_LEVELS],
    pub bid_qtys:   [i64; BOOK_L2_LEVELS],
    pub ask_prices: [i64; BOOK_L2_LEVELS],
    pub ask_qtys:   [i64; BOOK_L2_LEVELS],
}

unsafe impl ZeroCopySend for BookL2 {
    unsafe fn type_name() -> &'static str {
        "rdb::BookL2::v1"
    }
}

/// Decoded L2 level inside a [`FeedEvent::BookL2`] JSONL row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L2Level {
    pub price: f64,
    pub qty: f64,
}

/// Returns wall-clock nanos since the unix epoch.
///
/// Wall clock is sufficient on a single host because all processes run against
/// the same `CLOCK_REALTIME`. NTP jitter is irrelevant over the millisecond
/// horizons we measure here.
pub fn wall_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Free-form JSON Lines record produced by the simulated feed.
///
/// One per line in a JSONL file. The replayer turns these into [`Trade`] /
/// [`QuoteL1`] before publishing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FeedEvent {
    Trade {
        symbol: String,
        ts_exchange_ns: u64,
        price: f64,
        qty: f64,
        side: Side,
    },
    Quote {
        symbol: String,
        ts_exchange_ns: u64,
        bid_price: f64,
        bid_qty: f64,
        ask_price: f64,
        ask_qty: f64,
    },
    BookL2 {
        symbol: String,
        ts_exchange_ns: u64,
        /// Up to [`BOOK_L2_LEVELS`] bid levels, best-first. Excess is dropped
        /// by the replayer; shorter rows are zero-padded.
        bids: Vec<L2Level>,
        /// Up to [`BOOK_L2_LEVELS`] ask levels, best-first.
        asks: Vec<L2Level>,
    },
}
