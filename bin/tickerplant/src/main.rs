//! Tickerplant.
//!
//! Subscribes to the raw `*/raw` topics, assigns monotonically increasing
//! per-stream sequence numbers, persists each record to an mmap-backed WAL
//! (one per stream), and republishes onto `*/agg`. Both incoming streams
//! are polled from a single thread.
//!
//! For prototype simplicity there is no signal handler. Pass
//! `--idle-exit-secs N` to make the process exit after N seconds without
//! any incoming messages, or kill it with SIGKILL.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use iceoryx2::prelude::*;
use tracing::info;

use tp_config::SymbolTable;
use tp_types::{ipc_cfg, metrics::LatencyHistogram, topics, wall_ns, QuoteL1, Trade};
use tp_wal::WalWriter;

#[derive(Parser, Debug)]
#[command(name = "tickerplant")]
struct Args {
    /// Path to symbols TOML file (loaded for validation only; the
    /// tickerplant does not need to interpret prices).
    #[arg(long)]
    symbols: PathBuf,

    /// Directory in which to write the WAL files.
    #[arg(long, default_value = "/tmp/rdb-wal")]
    wal_dir: PathBuf,

    /// Idle sleep when no messages are available (microseconds).
    #[arg(long, default_value_t = 200)]
    idle_sleep_us: u64,

    /// Stop after this many seconds without any incoming message. 0 = run
    /// until killed externally.
    #[arg(long, default_value_t = 0)]
    idle_exit_secs: u64,

    /// Hard cap on total trades+quotes to process. 0 = unbounded.
    #[arg(long, default_value_t = 0)]
    limit: u64,

    /// Capacity (samples) for each per-hop latency histogram.
    #[arg(long, default_value_t = 4096)]
    hist_capacity: usize,
}

fn main() -> anyhow::Result<()> {
    init_tracing();
    let args = Args::parse();

    let symbols = SymbolTable::from_path(&args.symbols)
        .with_context(|| format!("loading symbols {}", args.symbols.display()))?;
    info!(symbol_count = symbols.len(), "loaded symbols");

    std::fs::create_dir_all(&args.wal_dir)?;
    let trade_wal_path = args.wal_dir.join("trades.wal");
    let quote_wal_path = args.wal_dir.join("quotes.wal");
    let mut trade_wal: WalWriter<Trade> = WalWriter::create(&trade_wal_path)?;
    let mut quote_wal: WalWriter<QuoteL1> = WalWriter::create(&quote_wal_path)?;
    info!(
        trade_wal = %trade_wal_path.display(),
        quote_wal = %quote_wal_path.display(),
        "WAL files opened"
    );

    let node = NodeBuilder::new().create::<ipc::Service>()?;
    let trades_in = node
        .service_builder(&topics::TRADES_RAW.try_into()?)
        .publish_subscribe::<Trade>()
        .subscriber_max_buffer_size(ipc_cfg::SUBSCRIBER_MAX_BUFFER_SIZE)
        .history_size(ipc_cfg::HISTORY_SIZE)
        .max_publishers(ipc_cfg::MAX_PUBLISHERS)
        .max_subscribers(ipc_cfg::MAX_SUBSCRIBERS)
        .open_or_create()?;
    let quotes_in = node
        .service_builder(&topics::QUOTES_RAW.try_into()?)
        .publish_subscribe::<QuoteL1>()
        .subscriber_max_buffer_size(ipc_cfg::SUBSCRIBER_MAX_BUFFER_SIZE)
        .history_size(ipc_cfg::HISTORY_SIZE)
        .max_publishers(ipc_cfg::MAX_PUBLISHERS)
        .max_subscribers(ipc_cfg::MAX_SUBSCRIBERS)
        .open_or_create()?;
    let trades_out = node
        .service_builder(&topics::TRADES_AGG.try_into()?)
        .publish_subscribe::<Trade>()
        .subscriber_max_buffer_size(ipc_cfg::SUBSCRIBER_MAX_BUFFER_SIZE)
        .history_size(ipc_cfg::HISTORY_SIZE)
        .max_publishers(ipc_cfg::MAX_PUBLISHERS)
        .max_subscribers(ipc_cfg::MAX_SUBSCRIBERS)
        .open_or_create()?;
    let quotes_out = node
        .service_builder(&topics::QUOTES_AGG.try_into()?)
        .publish_subscribe::<QuoteL1>()
        .subscriber_max_buffer_size(ipc_cfg::SUBSCRIBER_MAX_BUFFER_SIZE)
        .history_size(ipc_cfg::HISTORY_SIZE)
        .max_publishers(ipc_cfg::MAX_PUBLISHERS)
        .max_subscribers(ipc_cfg::MAX_SUBSCRIBERS)
        .open_or_create()?;

    let trade_sub = trades_in.subscriber_builder().create()?;
    let quote_sub = quotes_in.subscriber_builder().create()?;
    let trade_pub = trades_out.publisher_builder().create()?;
    let quote_pub = quotes_out.publisher_builder().create()?;

    let trade_lat = LatencyHistogram::new(args.hist_capacity);
    let quote_lat = LatencyHistogram::new(args.hist_capacity);

    let mut trade_seq: u64 = 0;
    let mut quote_seq: u64 = 0;
    let mut last_msg_at = wall_ns();
    let mut last_log_at = wall_ns();
    let log_period_ns = 1_000_000_000u64;

    let idle_sleep = Duration::from_micros(args.idle_sleep_us);
    let idle_exit_ns = args.idle_exit_secs.saturating_mul(1_000_000_000);

    loop {
        let mut did_work = false;

        while let Some(sample) = trade_sub.receive()? {
            let mut trade = *sample;
            let now = wall_ns();
            trade_lat.record(now.saturating_sub(trade.ts_local_ns));
            trade_seq += 1;
            trade.seq = trade_seq;
            trade_wal.append(&trade)?;
            let s = trade_pub.loan_uninit()?.write_payload(trade);
            s.send()?;
            did_work = true;
        }

        while let Some(sample) = quote_sub.receive()? {
            let mut q = *sample;
            let now = wall_ns();
            quote_lat.record(now.saturating_sub(q.ts_local_ns));
            quote_seq += 1;
            q.seq = quote_seq;
            quote_wal.append(&q)?;
            let s = quote_pub.loan_uninit()?.write_payload(q);
            s.send()?;
            did_work = true;
        }

        let now = wall_ns();
        if did_work {
            last_msg_at = now;
        } else if idle_exit_ns != 0 && now.saturating_sub(last_msg_at) > idle_exit_ns {
            info!("idle timeout reached; exiting");
            break;
        } else {
            std::thread::sleep(idle_sleep);
        }

        if args.limit != 0 && trade_seq + quote_seq >= args.limit {
            info!(trade_seq, quote_seq, "limit reached; exiting");
            break;
        }

        if now.saturating_sub(last_log_at) > log_period_ns {
            let t = trade_lat.snapshot();
            let q = quote_lat.snapshot();
            info!(
                trade_seq, quote_seq,
                trade_p50_ns = ?t.p50, trade_p99_ns = ?t.p99, trade_samples = t.samples,
                quote_p50_ns = ?q.p50, quote_p99_ns = ?q.p99, quote_samples = q.samples,
                "tickerplant stats"
            );
            last_log_at = now;
        }
    }

    info!(trade_seq, quote_seq, "tickerplant finalising WALs");
    trade_wal.flush()?;
    quote_wal.flush()?;
    trade_wal.finalize()?;
    quote_wal.finalize()?;

    let t = trade_lat.snapshot();
    let q = quote_lat.snapshot();
    info!(
        trade_count = trade_seq, quote_count = quote_seq,
        trade_p50_ns = ?t.p50, trade_p99_ns = ?t.p99,
        quote_p50_ns = ?q.p50, quote_p99_ns = ?q.p99,
        "tickerplant final stats"
    );
    drop(symbols);
    Ok(())
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
