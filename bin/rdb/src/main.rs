//! In-memory RDB.
//!
//! Subscribes to the tickerplant's `*/agg` topics, appends each record to
//! the corresponding per-symbol [`tp_arrow::SymbolStore`], and serves SQL
//! queries on a Unix domain socket. Each query opens a fresh DuckDB
//! in-memory database, snapshots the per-symbol stores into two
//! consolidated `RecordBatch`es and registers them as `trades_live` /
//! `quotes_live`.
//!
//! When `--hdb <dir>` is supplied, Parquet files written by `hdb-rollup`
//! are also mounted:
//!
//! ```text
//! trades_live  – today's in-memory rows
//! trades_hist  – read_parquet('<dir>/trades/*.parquet')   (if any exist)
//! trades       – UNION ALL of the two above
//! quotes_live / quotes_hist / quotes – same pattern
//! ```
//!
//! Clients that only know about `trades` / `quotes` continue to work
//! unchanged; they automatically pick up historical rows once an HDB
//! directory is configured.

use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
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

    /// HDB directory produced by `hdb-rollup`. When set, Parquet files
    /// inside `<dir>/trades/` and `<dir>/quotes/` are mounted alongside
    /// the live in-memory tables.
    #[arg(long)]
    hdb: Option<PathBuf>,

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

    let hdb_dir: Option<Arc<PathBuf>> = args.hdb.as_ref().map(|p| Arc::new(p.clone()));
    if let Some(ref hdb) = hdb_dir {
        info!(hdb = %hdb.display(), "HDB directory configured");
    }

    let stores_for_query = stores.clone();
    let _query_thread = std::thread::Builder::new()
        .name("rdb-query".into())
        .spawn(move || {
            run_query_server(listener, stores_for_query, hdb_dir);
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

fn run_query_server(listener: UnixListener, stores: Arc<StoreSet>, hdb_dir: Option<Arc<PathBuf>>) {
    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                let stores = stores.clone();
                let hdb_dir = hdb_dir.clone();
                std::thread::spawn(move || {
                    if let Err(e) = handle_connection(stream, stores, hdb_dir) {
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

fn handle_connection(
    mut stream: UnixStream,
    stores: Arc<StoreSet>,
    hdb_dir: Option<Arc<PathBuf>>,
) -> anyhow::Result<()> {
    let sql = query_proto::read_request(&mut stream)?;
    info!(sql_chars = sql.len(), "query received");

    match run_query(sql.as_str(), &stores, hdb_dir.as_ref().map(|p| p.as_path())) {
        Ok(payload) => query_proto::write_response(&mut stream, query_proto::STATUS_OK, &payload)?,
        Err(e) => {
            let msg = format!("{:#}", e);
            query_proto::write_response(&mut stream, query_proto::STATUS_ERR, msg.as_bytes())?;
        }
    }
    Ok(())
}

fn run_query(sql: &str, stores: &StoreSet, hdb_dir: Option<&Path>) -> anyhow::Result<Vec<u8>> {
    let trades_rb = stores.snapshot_trades()?;
    let quotes_rb = stores.snapshot_quotes()?;

    let conn = Connection::open_in_memory()?;
    conn.register_table_function::<ArrowVTab>("arrow")?;

    // Always materialise live data as *_live tables.
    create_arrow_table(&conn, "trades_live", trades_rb)?;
    create_arrow_table(&conn, "quotes_live", quotes_rb)?;

    // Mount historical Parquet files and build the union views.
    mount_hdb(&conn, hdb_dir)?;

    let mut stmt = conn.prepare(sql)?;
    let arrow_iter = stmt.query_arrow([])?;
    let result_schema = arrow_iter.get_schema();
    let batches: Vec<RecordBatch> = arrow_iter.collect();
    let bytes = encode_ipc_stream(&batches, &result_schema)?;
    Ok(bytes)
}

/// Register an Arrow RecordBatch as a materialised DuckDB temporary table.
fn create_arrow_table(conn: &Connection, name: &str, rb: RecordBatch) -> anyhow::Result<()> {
    let params = arrow_recordbatch_to_query_params(rb);
    let sql = format!("CREATE TEMP TABLE {name} AS SELECT * FROM arrow(?, ?)");
    let mut stmt = conn.prepare(&sql)?;
    stmt.execute(params)?;
    Ok(())
}

/// Mount HDB Parquet files and wire up the `trades` / `quotes` views.
///
/// If `hdb_dir` is None, or a subdirectory has no `.parquet` files, the
/// corresponding view is a plain alias for the live table.
fn mount_hdb(conn: &Connection, hdb_dir: Option<&Path>) -> anyhow::Result<()> {
    let trades_view = build_union_view(conn, "trades", hdb_dir)?;
    let quotes_view = build_union_view(conn, "quotes", hdb_dir)?;
    conn.execute_batch(&format!("{trades_view}\n{quotes_view}"))?;
    Ok(())
}

/// Returns a `CREATE VIEW <name> AS …` DDL string.
fn build_union_view(conn: &Connection, name: &str, hdb_dir: Option<&Path>) -> anyhow::Result<String> {
    let _ = conn; // reserved for future schema validation
    let live = format!("{name}_live");

    let hist_glob = hdb_dir.map(|d| d.join(name).join("*.parquet"));
    let has_hist = hist_glob
        .as_ref()
        .map(|g| has_parquet_files(g.parent().unwrap()))
        .unwrap_or(false);

    if has_hist {
        let glob = hist_glob.unwrap();
        Ok(format!(
            "CREATE VIEW {name}_hist AS SELECT * FROM read_parquet('{glob}');\n\
             CREATE VIEW {name} AS SELECT * FROM {live} UNION ALL SELECT * FROM {name}_hist;",
            glob = glob.display(),
        ))
    } else {
        Ok(format!("CREATE VIEW {name} AS SELECT * FROM {live};"))
    }
}

fn has_parquet_files(dir: &Path) -> bool {
    dir.read_dir()
        .map(|entries| {
            entries.flatten().any(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext.eq_ignore_ascii_case("parquet"))
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false)
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
