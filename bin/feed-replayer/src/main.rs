//! Simulated feed handler.
//!
//! Reads a JSONL file (one [`tp_types::FeedEvent`] per line), encodes each
//! event into a fixed binary [`tp_types::Trade`] / [`tp_types::QuoteL1`],
//! and publishes to two zenoh topics. The `--pace` flag chooses between
//! firehose mode (publish as fast as possible) and wall-clock pacing using
//! the `ts_exchange_ns` deltas in the file.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use bytemuck::bytes_of;
use clap::{Parser, ValueEnum};
use tracing::{info, warn};

use tp_config::SymbolTable;
use tp_types::{topics, wall_ns, FeedEvent, QuoteL1, Side, Trade};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Pace {
    /// Publish every event as fast as possible.
    Firehose,
    /// Sleep between events to match the inter-event delays in the file.
    WallClock,
}

#[derive(Parser, Debug)]
#[command(name = "feed-replayer")]
struct Args {
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

    /// Zenoh configuration file (JSON5 / YAML). Uses default peer-mode config
    /// when not supplied. Point to a config with a router endpoint for
    /// cross-subnet deployments.
    #[arg(long)]
    zenoh_config: Option<PathBuf>,
}

fn main() -> anyhow::Result<()> {
    init_tracing();
    let args = Args::parse();
    let symbols = SymbolTable::from_path(&args.symbols)
        .with_context(|| format!("loading symbols {}", args.symbols.display()))?;
    info!(symbol_count = symbols.len(), "loaded symbols");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let zconfig = zenoh_config(&args.zenoh_config)?;
    let session = rt.block_on(zenoh::open(zconfig))
        .context("opening zenoh session")?;
    let trade_pub = rt.block_on(session.declare_publisher(topics::TRADES_RAW))
        .context("declaring trades publisher")?;
    let quote_pub = rt.block_on(session.declare_publisher(topics::QUOTES_RAW))
        .context("declaring quotes publisher")?;
    info!("zenoh session open");

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
                    FeedEvent::Trade { ts_exchange_ns, .. } => *ts_exchange_ns,
                    FeedEvent::Quote { ts_exchange_ns, .. } => *ts_exchange_ns,
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
                rt.block_on(trade_pub.put(bytes_of(&trade).to_vec()))
                    .context("publishing trade")?;
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
                rt.block_on(quote_pub.put(bytes_of(&quote).to_vec()))
                    .context("publishing quote")?;
                last_ts_exchange = Some(ts_exchange_ns);
            }
        }

        sent += 1;
        if args.limit != 0 && sent >= args.limit { break; }
    }

    info!(sent, skipped, "feed-replayer finished");
    // Give subscribers a moment to drain the last batch before the session
    // closes and the zenoh transport tears down.
    std::thread::sleep(Duration::from_millis(100));
    let _ = Side::Buy; // suppress unused-import lint
    Ok(())
}

fn zenoh_config(path: &Option<PathBuf>) -> anyhow::Result<zenoh::Config> {
    match path {
        Some(p) => zenoh::Config::from_file(p)
            .with_context(|| format!("loading zenoh config {}", p.display())),
        None => Ok(zenoh::Config::default()),
    }
}

fn init_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = fmt()
        .json()
        .with_writer(std::io::stderr)
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}
