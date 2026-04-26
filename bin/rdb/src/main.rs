//! In-memory RDB.
//!
//! Subscribes to the tickerplant's `*/agg` topics, appends each record to
//! the corresponding per-symbol [`tp_arrow::SymbolStore`], and serves SQL
//! queries on a Unix domain socket.
//!
//! ## Query concurrency
//!
//! A pool of `--query-workers` (default 4) persistent DuckDB connections is
//! shared across incoming queries. Each connection is initialised once
//! (open + ArrowVTab registration + HDB Parquet metadata cached on first
//! use). Per query, only the live `trades_live` / `quotes_live` temp tables
//! are dropped and recreated from a fresh Arrow snapshot; the HDB views and
//! DuckDB internals stay warm. This eliminates the ~1–5 ms per-query
//! connection-open overhead and lets DuckDB reuse cached Parquet file
//! metadata across queries on the same worker.
//!
//! ## Memory cap
//!
//! `--row-cap N` (default 0 = unlimited) sets a per-symbol row limit on the
//! in-memory stores. When a push would exceed the cap the oldest row is
//! evicted in O(1) via `VecDeque::pop_front`.
//!
//! ## HDB views
//!
//! When `--hdb <dir>` is supplied, Parquet files written by `hdb-rollup`
//! are mounted at query time using two-level Hive partitioning:
//!
//! ```text
//! trades_live  – today's in-memory rows
//! trades_hist  – read_parquet('<dir>/trades/**/*.parquet', hive_partitioning=true)
//!                (if any files exist; date=…/symbol=… resolved from path)
//! trades       – UNION ALL of the two above
//! quotes_live / quotes_hist / quotes – same pattern
//! ```

use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use arrow_array::RecordBatch;
use clap::Parser;
use crossbeam_channel as chan;
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
    /// the live in-memory tables using Hive partitioning.
    #[arg(long)]
    hdb: Option<PathBuf>,

    /// Number of persistent DuckDB query workers. Each worker holds one
    /// open connection; incoming queries queue when all workers are busy.
    #[arg(long, default_value_t = 4)]
    query_workers: usize,

    /// Per-symbol row cap for in-memory stores. 0 = unlimited.
    /// When set, the oldest row is evicted in O(1) on each push that
    /// would exceed this limit.
    #[arg(long, default_value_t = 0)]
    row_cap: usize,

    /// Intra-day rollup interval in seconds. When > 0 and --hdb is set, a
    /// background thread wakes every N seconds, snapshots and clears the live
    /// stores, and writes the rows to a dated Parquet chunk in the HDB
    /// directory. This bounds peak memory without relying on --row-cap
    /// eviction, which silently drops rows.  0 = disabled.
    #[arg(long, default_value_t = 0)]
    rollup_interval_secs: u64,

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

    let pool = ConnPool::new(args.query_workers)
        .context("initialising DuckDB connection pool")?;
    info!(workers = args.query_workers, "DuckDB connection pool ready");

    if args.rollup_interval_secs > 0 {
        match &hdb_dir {
            Some(hdb) => {
                let stores_r = stores.clone();
                let hdb_r = hdb.clone();
                let interval_secs = args.rollup_interval_secs;
                std::thread::Builder::new()
                    .name("rdb-rollup".into())
                    .spawn(move || run_rollup_thread(stores_r, hdb_r, interval_secs))?;
                info!(interval_secs, "intra-day rollup thread started");
            }
            None => {
                warn!("--rollup-interval-secs ignored: --hdb not set");
            }
        }
    }

    let stores_q = stores.clone();
    let _query_thread = std::thread::Builder::new()
        .name("rdb-query".into())
        .spawn(move || {
            run_query_server(listener, stores_q, hdb_dir, pool);
        })?;

    run_ingest_loop(args, stores, trade_lat, quote_lat)
}

// ---------------------------------------------------------------------------
// Connection pool
// ---------------------------------------------------------------------------

/// A fixed-size pool of persistent DuckDB connections.
///
/// Implemented as a bounded channel: `recv` blocks until a connection is
/// available; `send` returns it. The channel capacity equals the pool size,
/// so `send` never blocks.
struct ConnPool {
    tx: chan::Sender<Connection>,
    rx: chan::Receiver<Connection>,
}

impl ConnPool {
    fn new(size: usize) -> anyhow::Result<Arc<Self>> {
        let (tx, rx) = chan::bounded(size);
        for _ in 0..size {
            let conn = Connection::open_in_memory()?;
            conn.register_table_function::<ArrowVTab>("arrow")?;
            tx.send(conn).unwrap();
        }
        Ok(Arc::new(Self { tx, rx }))
    }

    fn acquire(&self) -> Connection {
        self.rx.recv().expect("pool sender dropped")
    }

    fn release(&self, conn: Connection) {
        let _ = self.tx.send(conn);
    }
}

// ---------------------------------------------------------------------------
// Query server
// ---------------------------------------------------------------------------

fn run_query_server(
    listener: UnixListener,
    stores: Arc<StoreSet>,
    hdb_dir: Option<Arc<PathBuf>>,
    pool: Arc<ConnPool>,
) {
    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                let stores = stores.clone();
                let hdb_dir = hdb_dir.clone();
                let pool = pool.clone();
                std::thread::spawn(move || {
                    if let Err(e) = handle_connection(stream, stores, hdb_dir, pool) {
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
    pool: Arc<ConnPool>,
) -> anyhow::Result<()> {
    let sql = query_proto::read_request(&mut stream)?;
    info!(sql_chars = sql.len(), "query received");

    let conn = pool.acquire();
    let result = run_query(sql.as_str(), &stores, hdb_dir.as_ref().map(|p| p.as_path()), &conn);
    pool.release(conn);

    match result {
        Ok(payload) => query_proto::write_response(&mut stream, query_proto::STATUS_OK, &payload)?,
        Err(e) => {
            let msg = format!("{:#}", e);
            query_proto::write_response(&mut stream, query_proto::STATUS_ERR, msg.as_bytes())?;
        }
    }
    Ok(())
}

fn run_query(sql: &str, stores: &StoreSet, hdb_dir: Option<&Path>, conn: &Connection) -> anyhow::Result<Vec<u8>> {
    let trades_rb = stores.snapshot_trades()?;
    let quotes_rb = stores.snapshot_quotes()?;

    // Drop previous live tables and recreate from the fresh snapshot.
    // Views (trades, quotes, *_hist) are also recreated so the HDB glob
    // picks up any new files written by hdb-rollup since the last query.
    conn.execute_batch(
        "DROP TABLE  IF EXISTS trades_live;  DROP TABLE  IF EXISTS quotes_live;
         DROP VIEW   IF EXISTS trades_hist;  DROP VIEW   IF EXISTS quotes_hist;
         DROP VIEW   IF EXISTS trades;       DROP VIEW   IF EXISTS quotes;",
    )?;

    create_arrow_table(conn, "trades_live", trades_rb)?;
    create_arrow_table(conn, "quotes_live", quotes_rb)?;
    mount_hdb(conn, hdb_dir)?;

    let mut stmt = conn.prepare(sql)?;
    let arrow_iter = stmt.query_arrow([])?;
    let result_schema = arrow_iter.get_schema();
    let batches: Vec<RecordBatch> = arrow_iter.collect();
    encode_ipc_stream(&batches, &result_schema)
}

/// Register an Arrow RecordBatch as a materialised DuckDB temporary table.
fn create_arrow_table(conn: &Connection, name: &str, rb: RecordBatch) -> anyhow::Result<()> {
    let params = arrow_recordbatch_to_query_params(rb);
    let sql = format!("CREATE TEMP TABLE {name} AS SELECT * FROM arrow(?, ?)");
    let mut stmt = conn.prepare(&sql)?;
    stmt.execute(params)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// HDB mounting
// ---------------------------------------------------------------------------

fn mount_hdb(conn: &Connection, hdb_dir: Option<&Path>) -> anyhow::Result<()> {
    let trades_ddl = build_union_view("trades", hdb_dir);
    let quotes_ddl = build_union_view("quotes", hdb_dir);
    conn.execute_batch(&format!("{trades_ddl}\n{quotes_ddl}"))?;
    Ok(())
}

/// Returns DDL that creates `<name>_hist` (if Parquet files exist) and
/// `<name>` as UNION ALL of live + hist (or a plain alias when no HDB).
fn build_union_view(name: &str, hdb_dir: Option<&Path>) -> String {
    let live = format!("{name}_live");
    let has_hist = hdb_dir
        .map(|d| has_parquet_files(&d.join(name)))
        .unwrap_or(false);

    if has_hist {
        // Two-level Hive layout: <hdb>/<name>/date=YYYY-MM-DD/symbol=<sym>/
        // `hive_partitioning=true` lets DuckDB skip entire date or symbol
        // directories based on query predicates (file-skip on both axes).
        // `symbol` and `date` come from the path; all other columns come from
        // the file. Selecting columns explicitly by name keeps the schema
        // stable regardless of column order in the Parquet files and makes
        // the UNION ALL with the live table unambiguous.
        let glob = hdb_dir.unwrap().join(name).join("**").join("*.parquet");
        let cols = hist_columns(name);
        format!(
            "CREATE VIEW {name}_hist AS \
               SELECT {cols} \
               FROM read_parquet('{glob}', hive_partitioning=true);\n\
             CREATE VIEW {name} AS \
               SELECT * FROM {live} UNION ALL SELECT * FROM {name}_hist;",
            glob = glob.display(),
        )
    } else {
        format!("CREATE VIEW {name} AS SELECT * FROM {live};")
    }
}

/// Explicit column list for `<name>_hist`, matching the live table schema.
///
/// `symbol` is resolved from the Hive `symbol=…` partition directory;
/// `date` (also Hive) is intentionally omitted.  All other columns come
/// from the Parquet file itself.
fn hist_columns(name: &str) -> &'static str {
    match name {
        "trades" =>
            "symbol, symbol_id, seq, ts_exchange_ns, ts_local_ns, price, qty, side",
        "quotes" =>
            "symbol, symbol_id, seq, ts_exchange_ns, ts_local_ns, \
             bid_price, bid_qty, ask_price, ask_qty",
        _ => "*",
    }
}

/// Recursively checks whether `dir` contains at least one `.parquet` file.
fn has_parquet_files(dir: &Path) -> bool {
    dir.read_dir()
        .map(|entries| {
            entries.flatten().any(|e| {
                let p = e.path();
                if p.is_dir() {
                    has_parquet_files(&p)
                } else {
                    p.extension()
                        .map(|x| x.eq_ignore_ascii_case("parquet"))
                        .unwrap_or(false)
                }
            })
        })
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Ingest loop
// ---------------------------------------------------------------------------

fn run_ingest_loop(
    args: Args,
    stores: Arc<StoreSet>,
    trade_lat: Arc<LatencyHistogram>,
    quote_lat: Arc<LatencyHistogram>,
) -> anyhow::Result<()> {
    let row_cap = args.row_cap;

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
                store.trades.lock().push(&trade, row_cap);
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
                store.quotes.lock().push(&q, row_cap);
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

// ---------------------------------------------------------------------------
// Intra-day rollup thread
// ---------------------------------------------------------------------------

fn run_rollup_thread(stores: Arc<StoreSet>, hdb_dir: Arc<PathBuf>, interval_secs: u64) {
    let conn = match Connection::open_in_memory() {
        Ok(c) => c,
        Err(e) => { error!(error = %e, "rollup: failed to open DuckDB connection"); return; }
    };
    if let Err(e) = conn.register_table_function::<ArrowVTab>("arrow") {
        error!(error = %e, "rollup: failed to register ArrowVTab"); return;
    }

    let interval = Duration::from_secs(interval_secs);
    loop {
        std::thread::sleep(interval);

        let ts_secs = unix_secs_now();
        let date = unix_secs_to_date(ts_secs);

        // Snapshot without clearing: rows stay in memory until the Parquet
        // files are durably on disk (atomic rename). If any write fails we
        // simply log and continue; those rows will be included next cycle.
        let batch = match stores.snapshot_for_rollup() {
            Ok(b) => b,
            Err(e) => { error!(error = %e, "rollup: snapshot failed"); continue; }
        };

        // Collect (tmp_path, final_path) pairs across both tables.
        let mut pending: Vec<(PathBuf, PathBuf)> = Vec::new();
        let mut all_ok = true;

        for (table, rb) in [("trades", &batch.trades), ("quotes", &batch.quotes)] {
            if rb.num_rows() == 0 {
                continue;
            }
            match rollup_write_symbol_chunks(&conn, rb.clone(), &hdb_dir, table, &date, ts_secs) {
                Ok(mut pairs) => pending.append(&mut pairs),
                Err(e) => {
                    error!(error = %e, table, "rollup: write failed");
                    all_ok = false;
                }
            }
        }

        // Only rename once every per-symbol tmp file has been written.
        if all_ok {
            for (tmp, out) in &pending {
                match std::fs::rename(tmp, out) {
                    Ok(()) => info!(path = %out.display(), "rollup: chunk written"),
                    Err(e) => {
                        error!(error = %e, path = %out.display(), "rollup: rename failed");
                        all_ok = false;
                    }
                }
            }
        }

        // On any failure clean up all tmp files (remove_file is idempotent
        // for files that were already renamed successfully).
        if !all_ok {
            for (tmp, _) in &pending {
                let _ = std::fs::remove_file(tmp);
            }
            warn!("rollup: skipping commit — rows retained for next cycle");
        } else {
            stores.commit_rollup(&batch);
        }
    }
}

/// Load `rb` into a temporary DuckDB table, then write one `.parquet.tmp`
/// file per distinct symbol under
/// `<hdb_dir>/<table>/date=<date>/symbol=<sym>/<ts_secs>.parquet.tmp`.
///
/// Returns `(tmp_path, final_path)` pairs for the caller to rename atomically.
/// On error, any `.tmp` files already written by this call are removed before
/// returning so the caller's cleanup loop stays simple.
fn rollup_write_symbol_chunks(
    conn: &Connection,
    rb: RecordBatch,
    hdb_dir: &Path,
    table: &str,
    date: &str,
    ts_secs: u64,
) -> anyhow::Result<Vec<(PathBuf, PathBuf)>> {
    conn.execute_batch("DROP TABLE IF EXISTS _rollup")?;
    let params = arrow_recordbatch_to_query_params(rb);
    conn.prepare("CREATE TEMP TABLE _rollup AS SELECT * FROM arrow(?, ?)")?
        .execute(params)?;

    let mut sym_stmt = conn.prepare("SELECT DISTINCT symbol FROM _rollup ORDER BY symbol")?;
    let symbols: Vec<String> = sym_stmt
        .query_map([], |row| row.get(0))?
        .collect::<Result<_, _>>()?;

    let mut written: Vec<(PathBuf, PathBuf)> = Vec::with_capacity(symbols.len());

    let result = (|| -> anyhow::Result<()> {
        for sym in &symbols {
            let sym_dir = hdb_dir
                .join(table)
                .join(format!("date={date}"))
                .join(format!("symbol={sym}"));
            std::fs::create_dir_all(&sym_dir)
                .with_context(|| format!("creating {}", sym_dir.display()))?;
            let tmp = sym_dir.join(format!("{ts_secs}.parquet.tmp"));
            let out = sym_dir.join(format!("{ts_secs}.parquet"));

            // Exclude `symbol` from the file; it is encoded in the path.
            conn.execute_batch(&format!(
                "COPY (SELECT * EXCLUDE (symbol) FROM _rollup WHERE symbol = {}) \
                 TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD)",
                sql_quote(sym),
                tmp.display()
            ))
            .with_context(|| format!("writing {}", tmp.display()))?;
            written.push((tmp, out));
        }
        Ok(())
    })();

    let _ = conn.execute_batch("DROP TABLE IF EXISTS _rollup");

    match result {
        Ok(()) => Ok(written),
        Err(e) => {
            for (tmp, _) in &written {
                let _ = std::fs::remove_file(tmp);
            }
            Err(e)
        }
    }
}

/// Wrap `s` in single quotes, escaping any embedded single quotes.
fn sql_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn unix_secs_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Convert Unix seconds to a `YYYY-MM-DD` string (UTC, no external deps).
fn unix_secs_to_date(secs: u64) -> String {
    let z = secs / 86400 + 719468;
    let era = z / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}")
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
