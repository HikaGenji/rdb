//! Wire-format types and shared helpers for the rdb prototype.
//!
//! The two record types are POD (`bytemuck::Pod`) and `repr(C)`, with explicit
//! padding so they can be transmitted byte-for-byte over the network via
//! zenoh. `bytemuck::bytes_of` serialises them on the publish side;
//! `bytemuck::try_from_bytes` deserialises them on the subscribe side.

use std::time::{SystemTime, UNIX_EPOCH};

use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Serialize};

pub mod metrics;
pub mod query_proto;

pub mod topics {
    //! Zenoh key expressions used across the prototype.
    pub const TRADES_RAW: &str = "rdb/trades/raw";
    pub const QUOTES_RAW: &str = "rdb/quotes/raw";
    pub const TRADES_AGG: &str = "rdb/trades/agg";
    pub const QUOTES_AGG: &str = "rdb/quotes/agg";
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
#[serde(tag = "kind", rename_all = "lowercase")]
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
}
