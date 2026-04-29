//! `rdb feed-replayer` — JSONL → iceoryx2 publisher.
//!
//! Reads a JSONL file (one [`tp_types::FeedEvent`] per line), encodes each
//! event into a fixed binary [`tp_types::Trade`] / [`tp_types::QuoteL1`],
//! and publishes to two iceoryx2 services.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use clap::ValueEnum;
use iceoryx2::prelude::*;
use tracing::{info, warn};

use tp_config::SymbolTable;
use tp_types::{ipc_cfg, topics, wall_ns, BookL2, FeedEvent, QuoteL1, Trade, BOOK_L2_LEVELS};

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Pace {
    /// Publish every event as fast as possible.
    Firehose,
    /// Sleep between events to match the inter-event delays in the file.
    WallClock,
}

#[derive(clap::Args, Debug)]
pub struct Args {
    /// Path to the symbols TOML file.
    #[arg(long)]
    symbols: PathBuf,

    /// Path to the JSONL file containing FeedEvents.
    #[arg(long)]
    input: PathBuf,

    /// Pacing mode.
    #[arg(long, value_enum, default_value_t = Pace::Firehose)]
    pace: Pace,

    /// Stop after publishing this many events (0 = unbounded).
    #[arg(long, default_value_t = 0)]
    limit: u64,
}

pub fn run(args: Args) -> anyhow::Result<()> {
    let symbols = SymbolTable::from_path(&args.symbols)
        .with_context(|| format!("loading symbols {}", args.symbols.display()))?;
    info!(symbol_count = symbols.len(), "loaded symbols");

    let node = NodeBuilder::new().create::<ipc::Service>()?;
    let trades_svc = node
        .service_builder(&topics::TRADES_RAW.try_into()?)
        .publish_subscribe::<Trade>()
        .subscriber_max_buffer_size(ipc_cfg::SUBSCRIBER_MAX_BUFFER_SIZE)
        .history_size(ipc_cfg::HISTORY_SIZE)
        .max_publishers(ipc_cfg::MAX_PUBLISHERS)
        .max_subscribers(ipc_cfg::MAX_SUBSCRIBERS)
        .open_or_create()?;
    let quotes_svc = node
        .service_builder(&topics::QUOTES_RAW.try_into()?)
        .publish_subscribe::<QuoteL1>()
        .subscriber_max_buffer_size(ipc_cfg::SUBSCRIBER_MAX_BUFFER_SIZE)
        .history_size(ipc_cfg::HISTORY_SIZE)
        .max_publishers(ipc_cfg::MAX_PUBLISHERS)
        .max_subscribers(ipc_cfg::MAX_SUBSCRIBERS)
        .open_or_create()?;
    let book_l2_svc = node
        .service_builder(&topics::BOOK_L2_RAW.try_into()?)
        .publish_subscribe::<BookL2>()
        .subscriber_max_buffer_size(ipc_cfg::SUBSCRIBER_MAX_BUFFER_SIZE)
        .history_size(ipc_cfg::HISTORY_SIZE)
        .max_publishers(ipc_cfg::MAX_PUBLISHERS)
        .max_subscribers(ipc_cfg::MAX_SUBSCRIBERS)
        .open_or_create()?;

    let trade_pub   = trades_svc.publisher_builder().create()?;
    let quote_pub   = quotes_svc.publisher_builder().create()?;
    let book_l2_pub = book_l2_svc.publisher_builder().create()?;

    let file = File::open(&args.input)
        .with_context(|| format!("opening {}", args.input.display()))?;
    let reader = BufReader::new(file);

    let mut sent: u64 = 0;
    let mut last_ts_exchange: Option<u64> = None;
    let mut skipped: u64 = 0;

    for (lineno, line) in reader.lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() { continue; }
        let evt: FeedEvent = match serde_json::from_str(&line) {
            Ok(e) => e,
            Err(e) => {
                warn!(lineno, error = %e, "skipping malformed JSONL row");
                skipped += 1;
                continue;
            }
        };

        if let Pace::WallClock = args.pace {
            if let Some(prev) = last_ts_exchange {
                let curr = match &evt {
                    FeedEvent::Trade  { ts_exchange_ns, .. } => *ts_exchange_ns,
                    FeedEvent::Quote  { ts_exchange_ns, .. } => *ts_exchange_ns,
                    FeedEvent::BookL2 { ts_exchange_ns, .. } => *ts_exchange_ns,
                };
                if curr > prev {
                    std::thread::sleep(Duration::from_nanos(curr - prev));
                }
            }
        }

        match evt {
            FeedEvent::Trade { symbol, ts_exchange_ns, price, qty, side } => {
                let Some(symbol_id) = symbols.id_of(&symbol) else {
                    warn!(symbol, "unknown symbol in feed; dropping");
                    skipped += 1;
                    continue;
                };
                let trade = Trade {
                    seq: 0,
                    ts_exchange_ns,
                    ts_local_ns: wall_ns(),
                    symbol_id,
                    _pad0: 0,
                    price: symbols.encode_price(symbol_id, price),
                    qty:   symbols.encode_qty(symbol_id, qty),
                    side: side as u8,
                    _pad1: [0; 7],
                };
                let sample = trade_pub.loan_uninit()?.write_payload(trade);
                sample.send()?;
                last_ts_exchange = Some(ts_exchange_ns);
            }
            FeedEvent::Quote { symbol, ts_exchange_ns, bid_price, bid_qty, ask_price, ask_qty } => {
                let Some(symbol_id) = symbols.id_of(&symbol) else {
                    warn!(symbol, "unknown symbol in feed; dropping");
                    skipped += 1;
                    continue;
                };
                let quote = QuoteL1 {
                    seq: 0,
                    ts_exchange_ns,
                    ts_local_ns: wall_ns(),
                    symbol_id,
                    _pad: 0,
                    bid_price: symbols.encode_price(symbol_id, bid_price),
                    bid_qty:   symbols.encode_qty(symbol_id, bid_qty),
                    ask_price: symbols.encode_price(symbol_id, ask_price),
                    ask_qty:   symbols.encode_qty(symbol_id, ask_qty),
                };
                let sample = quote_pub.loan_uninit()?.write_payload(quote);
                sample.send()?;
                last_ts_exchange = Some(ts_exchange_ns);
            }
            FeedEvent::BookL2 { symbol, ts_exchange_ns, bids, asks } => {
                let Some(symbol_id) = symbols.id_of(&symbol) else {
                    warn!(symbol, "unknown symbol in feed; dropping");
                    skipped += 1;
                    continue;
                };
                let mut book = BookL2 {
                    seq: 0,
                    ts_exchange_ns,
                    ts_local_ns: wall_ns(),
                    symbol_id,
                    _pad: 0,
                    bid_prices: [0i64; BOOK_L2_LEVELS],
                    bid_qtys:   [0i64; BOOK_L2_LEVELS],
                    ask_prices: [0i64; BOOK_L2_LEVELS],
                    ask_qtys:   [0i64; BOOK_L2_LEVELS],
                };
                for (lvl, l) in bids.iter().take(BOOK_L2_LEVELS).enumerate() {
                    book.bid_prices[lvl] = symbols.encode_price(symbol_id, l.price);
                    book.bid_qtys[lvl]   = symbols.encode_qty(symbol_id, l.qty);
                }
                for (lvl, l) in asks.iter().take(BOOK_L2_LEVELS).enumerate() {
                    book.ask_prices[lvl] = symbols.encode_price(symbol_id, l.price);
                    book.ask_qtys[lvl]   = symbols.encode_qty(symbol_id, l.qty);
                }
                let sample = book_l2_pub.loan_uninit()?.write_payload(book);
                sample.send()?;
                last_ts_exchange = Some(ts_exchange_ns);
            }
        }

        sent += 1;
        if args.limit != 0 && sent >= args.limit { break; }
    }

    info!(sent, skipped, "feed-replayer finished");
    std::thread::sleep(Duration::from_millis(50));
    Ok(())
}
