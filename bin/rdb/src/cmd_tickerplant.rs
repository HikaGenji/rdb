//! `rdb tickerplant` — sequence-numbering relay with WAL.
//!
//! Subscribes to `*/raw`, assigns monotonically increasing per-stream
//! sequence numbers, persists each record to an mmap-backed WAL, and
//! republishes onto `*/agg`.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use iceoryx2::prelude::*;
use tracing::info;

use tp_config::SymbolTable;
use tp_types::{ipc_cfg, metrics::LatencyHistogram, topics, wall_ns, BookL2, QuoteL1, Trade};
use tp_wal::WalWriter;

#[derive(clap::Args, Debug)]
pub struct Args {
    /// Path to symbols TOML file.
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

    /// Group-commit batch size: call fsync after this many appends. 0
    /// disables count-based fsync (fall back to the timer).
    #[arg(long, default_value_t = 64)]
    wal_fsync_batch: u64,

    /// Group-commit timer: call fsync at least this often, in milliseconds.
    /// 0 disables the timer (fall back to count-based fsync).
    #[arg(long, default_value_t = 10)]
    wal_fsync_interval_ms: u64,
}

pub fn run(args: Args) -> anyhow::Result<()> {
    let symbols = SymbolTable::from_path(&args.symbols)
        .with_context(|| format!("loading symbols {}", args.symbols.display()))?;
    info!(symbol_count = symbols.len(), "loaded symbols");

    std::fs::create_dir_all(&args.wal_dir)?;
    let trade_wal_path   = args.wal_dir.join("trades.wal");
    let quote_wal_path   = args.wal_dir.join("quotes.wal");
    let book_l2_wal_path = args.wal_dir.join("book_l2.wal");
    let mut trade_wal:   WalWriter<Trade>   = WalWriter::open_or_create(&trade_wal_path)?;
    let mut quote_wal:   WalWriter<QuoteL1> = WalWriter::open_or_create(&quote_wal_path)?;
    let mut book_l2_wal: WalWriter<BookL2>  = WalWriter::open_or_create(&book_l2_wal_path)?;
    info!(
        trade_wal   = %trade_wal_path.display(),
        quote_wal   = %quote_wal_path.display(),
        book_l2_wal = %book_l2_wal_path.display(),
        trade_wal_count   = trade_wal.count(),
        quote_wal_count   = quote_wal.count(),
        book_l2_wal_count = book_l2_wal.count(),
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
    let book_l2_in = node
        .service_builder(&topics::BOOK_L2_RAW.try_into()?)
        .publish_subscribe::<BookL2>()
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
    let book_l2_out = node
        .service_builder(&topics::BOOK_L2_AGG.try_into()?)
        .publish_subscribe::<BookL2>()
        .subscriber_max_buffer_size(ipc_cfg::SUBSCRIBER_MAX_BUFFER_SIZE)
        .history_size(ipc_cfg::HISTORY_SIZE)
        .max_publishers(ipc_cfg::MAX_PUBLISHERS)
        .max_subscribers(ipc_cfg::MAX_SUBSCRIBERS)
        .open_or_create()?;

    let trade_sub   = trades_in.subscriber_builder().create()?;
    let quote_sub   = quotes_in.subscriber_builder().create()?;
    let book_l2_sub = book_l2_in.subscriber_builder().create()?;
    let trade_pub   = trades_out.publisher_builder().create()?;
    let quote_pub   = quotes_out.publisher_builder().create()?;
    let book_l2_pub = book_l2_out.publisher_builder().create()?;

    let trade_lat   = LatencyHistogram::new(args.hist_capacity);
    let quote_lat   = LatencyHistogram::new(args.hist_capacity);
    let book_l2_lat = LatencyHistogram::new(args.hist_capacity);

    // Resume per-stream sequence numbers from the WAL count so a restart
    // does not produce overlapping seq values.
    let mut trade_seq:   u64 = trade_wal.count();
    let mut quote_seq:   u64 = quote_wal.count();
    let mut book_l2_seq: u64 = book_l2_wal.count();
    let mut last_msg_at = wall_ns();
    let mut last_log_at = wall_ns();
    let mut last_fsync_at = wall_ns();
    let mut unflushed_appends: u64 = 0;
    let log_period_ns = 1_000_000_000u64;
    let fsync_interval_ns = args.wal_fsync_interval_ms.saturating_mul(1_000_000);

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
            unflushed_appends += 1;
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
            unflushed_appends += 1;
            let s = quote_pub.loan_uninit()?.write_payload(q);
            s.send()?;
            did_work = true;
        }

        while let Some(sample) = book_l2_sub.receive()? {
            let mut b = *sample;
            let now = wall_ns();
            book_l2_lat.record(now.saturating_sub(b.ts_local_ns));
            book_l2_seq += 1;
            b.seq = book_l2_seq;
            book_l2_wal.append(&b)?;
            unflushed_appends += 1;
            let s = book_l2_pub.loan_uninit()?.write_payload(b);
            s.send()?;
            did_work = true;
        }

        let now = wall_ns();
        let due_by_count = args.wal_fsync_batch > 0 && unflushed_appends >= args.wal_fsync_batch;
        let due_by_timer = fsync_interval_ns > 0
            && unflushed_appends > 0
            && now.saturating_sub(last_fsync_at) >= fsync_interval_ns;
        if due_by_count || due_by_timer {
            trade_wal.sync()?;
            quote_wal.sync()?;
            book_l2_wal.sync()?;
            unflushed_appends = 0;
            last_fsync_at = now;
        }

        if did_work {
            last_msg_at = now;
        } else if idle_exit_ns != 0 && now.saturating_sub(last_msg_at) > idle_exit_ns {
            info!("idle timeout reached; exiting");
            break;
        } else {
            std::thread::sleep(idle_sleep);
        }

        if args.limit != 0 && trade_seq + quote_seq + book_l2_seq >= args.limit {
            info!(trade_seq, quote_seq, book_l2_seq, "limit reached; exiting");
            break;
        }

        if now.saturating_sub(last_log_at) > log_period_ns {
            let t = trade_lat.snapshot();
            let q = quote_lat.snapshot();
            let b = book_l2_lat.snapshot();
            info!(
                trade_seq, quote_seq, book_l2_seq,
                trade_p50_ns   = ?t.p50, trade_p99_ns   = ?t.p99, trade_samples   = t.samples,
                quote_p50_ns   = ?q.p50, quote_p99_ns   = ?q.p99, quote_samples   = q.samples,
                book_l2_p50_ns = ?b.p50, book_l2_p99_ns = ?b.p99, book_l2_samples = b.samples,
                "tickerplant stats"
            );
            last_log_at = now;
        }
    }

    if unflushed_appends > 0 {
        trade_wal.sync()?;
        quote_wal.sync()?;
        book_l2_wal.sync()?;
    }

    info!(trade_seq, quote_seq, book_l2_seq, "tickerplant finalising WALs");
    trade_wal.flush()?;
    quote_wal.flush()?;
    book_l2_wal.flush()?;
    trade_wal.finalize()?;
    quote_wal.finalize()?;
    book_l2_wal.finalize()?;

    let t = trade_lat.snapshot();
    let q = quote_lat.snapshot();
    info!(
        trade_count = trade_seq, quote_count = quote_seq, book_l2_count = book_l2_seq,
        trade_p50_ns = ?t.p50, trade_p99_ns = ?t.p99,
        quote_p50_ns = ?q.p50, quote_p99_ns = ?q.p99,
        "tickerplant final stats"
    );
    drop(symbols);
    Ok(())
}
