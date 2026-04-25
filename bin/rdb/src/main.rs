//! In-memory RDB.
//!
//! Subscribes to the tickerplant's `*/agg` topics, appends each record to
//! the corresponding per-symbol [`tp_arrow::SymbolStore`], and serves SQL
//! queries on a Unix domain socket. Each query opens a fresh DuckDB
//! in-memory database, snapshots the per-symbol stores into two
//! consolidated `RecordBatch`es (`trades` and `quotes`), zero-copies them
//! into DuckDB via the `arrow(?, ?)` table function, and runs the query.
//!
//! For prototype simplicity this binary owns a dedicated ingest thread
//! and a dedicated query-server thread; query latency does not block the
//! ingest path beyond the brief snapshot lock.

use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use arrow_array::RecordBatch;
use clap::Parser;
use duckdb::vtab::arrow::{arrow_recordbatch_to_query_params, ArrowVTab};
use duckdb::Connection;
use iceoryx2::prelude::*;
use tracing::{error, info, warn};

use tp_arrow::{encode_ipc_stream, StoreSet};
use tp_config::SymbolTable;
use tp_types::{
    ipc_cfg, metrics::LatencyHistogram, query_proto, topics, wall_ns, QuoteL1, Trade,
};

#[derive(Parser, Debug)]
#[command(name = "rdb")]
struct Args {
    /// Symbols TOML file.
    #[arg(long)]
    symbols: PathBuf,

    /// Unix socket path on which the SQL endpoint listens.
    #[arg(long, default_value = "/tmp/rdb.sock")]
    socket: PathBuf,

    /// Idle sleep in the ingest loop (microseconds).
    #[arg(long, default_value_t = 200)]
    idle_sleep_us: u64,

    /// Stop after this many seconds of ingest idleness. 0 = never.
    #[arg(long, default_value_t = 0)]
    idle_exit_secs: u64,

    /// Hard cap on appended trade+quote rows. 0 = unbounded.
    #[arg(long, default_value_t = 0)]
    limit: u64,

    /// Histogram capacity per latency hop.
    #[arg(long, default_value_t = 4096)]
    hist_capacity: usize,
}

fn main() -> anyhow::Result<()> {
    init_tracing();
    let args = Args::parse();

    let symbols = SymbolTable::from_path(&args.symbols)
        .with_context(|| format!("loading symbols {}", args.symbols.display()))?;
    let stores = Arc::new(StoreSet::from_symbols(&symbols));
    info!(symbol_count = symbols.len(), "loaded symbols");

    let trade_lat = Arc::new(LatencyHistogram::new(args.hist_capacity));
    let quote_lat = Arc::new(LatencyHistogram::new(args.hist_capacity));

    let socket_path = args.socket.clone();
    if socket_path.exists() {
        std::fs::remove_file(&socket_path).ok();
    }
    let listener = UnixListener::bind(&socket_path)
        .with_context(|| format!("binding {}", socket_path.display()))?;
    info!(socket = %socket_path.display(), "query endpoint listening");

    let stores_for_query = stores.clone();
    let _query_thread = std::thread::Builder::new()
        .name("rdb-query".into())
        .spawn(move || {
            run_query_server(listener, stores_for_query);
        })?;

    run_ingest_loop(args, stores, trade_lat, quote_lat)
}

fn run_ingest_loop(
    args: Args,
    stores: Arc<StoreSet>,
    trade_lat: Arc<LatencyHistogram>,
    quote_lat: Arc<LatencyHistogram>,
) -> anyhow::Result<()> {
    let node = NodeBuilder::new().create::<ipc::Service>()?;
    let trades_svc = node
        .service_builder(&topics::TRADES_AGG.try_into()?)
        .publish_subscribe::<Trade>()
        .subscriber_max_buffer_size(ipc_cfg::SUBSCRIBER_MAX_BUFFER_SIZE)
        .history_size(ipc_cfg::HISTORY_SIZE)
        .max_publishers(ipc_cfg::MAX_PUBLISHERS)
        .max_subscribers(ipc_cfg::MAX_SUBSCRIBERS)
        .open_or_create()?;
    let quotes_svc = node
        .service_builder(&topics::QUOTES_AGG.try_into()?)
        .publish_subscribe::<QuoteL1>()
        .subscriber_max_buffer_size(ipc_cfg::SUBSCRIBER_MAX_BUFFER_SIZE)
        .history_size(ipc_cfg::HISTORY_SIZE)
        .max_publishers(ipc_cfg::MAX_PUBLISHERS)
        .max_subscribers(ipc_cfg::MAX_SUBSCRIBERS)
        .open_or_create()?;

    let trade_sub = trades_svc.subscriber_builder().create()?;
    let quote_sub = quotes_svc.subscriber_builder().create()?;

    let mut last_msg_at = wall_ns();
    let mut last_log_at = wall_ns();
    let log_period_ns = 1_000_000_000u64;
    let idle_sleep = Duration::from_micros(args.idle_sleep_us);
    let idle_exit_ns = args.idle_exit_secs.saturating_mul(1_000_000_000);

    let mut trade_count: u64 = 0;
    let mut quote_count: u64 = 0;

    loop {
        let mut did_work = false;

        while let Some(sample) = trade_sub.receive()? {
            let trade = *sample;
            let now = wall_ns();
            trade_lat.record(now.saturating_sub(trade.ts_local_ns));
            if let Some(store) = stores.store_for(trade.symbol_id) {
                store.trades.lock().push(&trade);
                trade_count += 1;
            } else {
                warn!(symbol_id = trade.symbol_id, "trade for unknown symbol_id");
                trade_lat.record_drop();
            }
            did_work = true;
        }

        while let Some(sample) = quote_sub.receive()? {
            let q = *sample;
            let now = wall_ns();
            quote_lat.record(now.saturating_sub(q.ts_local_ns));
            if let Some(store) = stores.store_for(q.symbol_id) {
                store.quotes.lock().push(&q);
                quote_count += 1;
            } else {
                warn!(symbol_id = q.symbol_id, "quote for unknown symbol_id");
                quote_lat.record_drop();
            }
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

        if args.limit != 0 && trade_count + quote_count >= args.limit {
            info!(trade_count, quote_count, "limit reached; exiting");
            break;
        }

        if now.saturating_sub(last_log_at) > log_period_ns {
            let t = trade_lat.snapshot();
            let q = quote_lat.snapshot();
            info!(
                trade_count, quote_count,
                trade_p50_ns = ?t.p50, trade_p99_ns = ?t.p99, trade_samples = t.samples,
                quote_p50_ns = ?q.p50, quote_p99_ns = ?q.p99, quote_samples = q.samples,
                "rdb stats"
            );
            last_log_at = now;
        }
    }

    let t = trade_lat.snapshot();
    let q = quote_lat.snapshot();
    info!(
        trade_count, quote_count,
        trade_p50_ns = ?t.p50, trade_p99_ns = ?t.p99,
        quote_p50_ns = ?q.p50, quote_p99_ns = ?q.p99,
        "rdb final stats"
    );
    Ok(())
}

fn run_query_server(listener: UnixListener, stores: Arc<StoreSet>) {
    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                let stores = stores.clone();
                std::thread::spawn(move || {
                    if let Err(e) = handle_connection(stream, stores) {
                        warn!(error = %e, "query connection failed");
                    }
                });
            }
            Err(e) => {
                error!(error = %e, "accept failed");
                break;
            }
        }
    }
}

fn handle_connection(mut stream: UnixStream, stores: Arc<StoreSet>) -> anyhow::Result<()> {
    let sql = query_proto::read_request(&mut stream)?;
    info!(sql_chars = sql.len(), "query received");

    match run_query(sql.as_str(), &stores) {
        Ok(payload) => query_proto::write_response(&mut stream, query_proto::STATUS_OK, &payload)?,
        Err(e) => {
            let msg = format!("{:#}", e);
            query_proto::write_response(&mut stream, query_proto::STATUS_ERR, msg.as_bytes())?;
        }
    }
    Ok(())
}

fn run_query(sql: &str, stores: &StoreSet) -> anyhow::Result<Vec<u8>> {
    let trades_rb = stores.snapshot_trades()?;
    let quotes_rb = stores.snapshot_quotes()?;

    let conn = Connection::open_in_memory()?;
    conn.register_table_function::<ArrowVTab>("arrow")?;

    // Bind both batches into temporary tables. CREATE TABLE AS materializes
    // the rows once into DuckDB-managed storage, after which we can run
    // arbitrary user SQL against `trades` / `quotes`.
    create_temp_table(&conn, "trades", trades_rb)?;
    create_temp_table(&conn, "quotes", quotes_rb)?;

    let mut stmt = conn.prepare(sql)?;
    let arrow_iter = stmt.query_arrow([])?;
    let result_schema = arrow_iter.get_schema();
    let batches: Vec<RecordBatch> = arrow_iter.collect();
    let bytes = encode_ipc_stream(&batches, &result_schema)?;
    Ok(bytes)
}

fn create_temp_table(conn: &Connection, name: &str, rb: RecordBatch) -> anyhow::Result<()> {
    let params = arrow_recordbatch_to_query_params(rb);
    let sql = format!("CREATE TEMP TABLE {name} AS SELECT * FROM arrow(?, ?)");
    let mut stmt = conn.prepare(&sql)?;
    stmt.execute(params)?;
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
