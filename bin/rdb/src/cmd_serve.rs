//! `rdb serve` — in-memory query server.
//!
//! Subscribes to the tickerplant's `*/agg` topics, appends each record to
//! the corresponding per-symbol [`tp_arrow::SymbolStore`], and serves SQL
//! queries on a Unix domain socket.

use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use arrow_array::{ArrayRef, Float64Array, RecordBatch, StringArray, UInt32Array, UInt64Array};
use crossbeam_channel as chan;
use duckdb::vtab::arrow::{arrow_recordbatch_to_query_params, ArrowVTab};
use duckdb::Connection;
use iceoryx2::prelude::*;
use tracing::{error, info, warn};

use tp_arrow::{encode_ipc_stream, StoreSet};
use tp_config::SymbolTable;
use tp_types::{
    ipc_cfg, metrics::LatencyHistogram, query_proto, topics, wall_ns, BookL2, QuoteL1, Side, Trade,
};
use tp_wal as wal;

use crate::sql_guard;

#[derive(clap::Args, Debug)]
pub struct Args {
    /// Symbols TOML file.
    #[arg(long)]
    symbols: PathBuf,

    /// Unix socket path on which the SQL endpoint listens.
    #[arg(long, default_value = "/tmp/rdb.sock")]
    socket: PathBuf,

    /// HDB directory produced by `rdb hdb-rollup`. When set, Parquet files
    /// inside `<dir>/trades/` and `<dir>/quotes/` are mounted alongside
    /// the live in-memory tables using Hive partitioning.
    #[arg(long)]
    hdb: Option<PathBuf>,

    /// Persistent DuckDB file backing the user catalog. User-issued DDL
    /// (`CREATE TABLE`, `INSERT`, …) lands here and survives restarts. The
    /// streaming `trades`/`quotes` fixtures are mounted as temp views per
    /// query and never touch this file.
    #[arg(long, default_value = "data/rdb.duckdb")]
    db: PathBuf,

    /// Number of persistent DuckDB query workers.
    #[arg(long, default_value_t = 4)]
    query_workers: usize,

    /// Per-symbol row cap for in-memory stores. 0 = unlimited.
    #[arg(long, default_value_t = 0)]
    row_cap: usize,

    /// Intra-day rollup interval in seconds. Requires --hdb. 0 = disabled.
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

    /// Tickerplant WAL directory. When set, on startup the server reads
    /// `<dir>/trades.wal` and `<dir>/quotes.wal` and replays each record
    /// into the in-memory stores before opening the iceoryx2 subscribers.
    /// Records older than `--wal-replay-since-secs` (when set) are skipped.
    #[arg(long)]
    wal_dir: Option<PathBuf>,

    /// When `--wal-dir` is set, only replay records whose `ts_local_ns`
    /// is within this many seconds of `now`. 0 = replay everything.
    #[arg(long, default_value_t = 0)]
    wal_replay_since_secs: u64,

    /// Bucket size in seconds for the live OHLCV bars table
    /// (`trades_bars`). 0 = bars disabled. Default 60.
    #[arg(long, default_value_t = 60)]
    bar_interval_secs: u64,

    /// Per-symbol row cap on the bars table. 0 = unlimited.
    #[arg(long, default_value_t = 0)]
    bar_row_cap: usize,

    /// Log any query whose wall-clock duration meets or exceeds this many
    /// milliseconds. 0 disables slow-query logging (the per-query
    /// histogram still records). Default 250 ms.
    #[arg(long, default_value_t = 250)]
    slow_query_ms: u64,
}

pub fn run(args: Args) -> anyhow::Result<()> {
    let symbols = SymbolTable::from_path(&args.symbols)
        .with_context(|| format!("loading symbols {}", args.symbols.display()))?;
    let stores = Arc::new(StoreSet::from_symbols(&symbols));
    info!(symbol_count = symbols.len(), "loaded symbols");

    let bar_interval_ns: u64 = args.bar_interval_secs.saturating_mul(1_000_000_000);
    let bar_row_cap = args.bar_row_cap;

    if let Some(wal_dir) = args.wal_dir.as_ref() {
        replay_wal(
            wal_dir,
            &stores,
            args.row_cap,
            args.wal_replay_since_secs,
            bar_interval_ns,
            bar_row_cap,
        )
        .with_context(|| format!("replaying WAL from {}", wal_dir.display()))?;
    }

    let trade_lat   = Arc::new(LatencyHistogram::new(args.hist_capacity));
    let quote_lat   = Arc::new(LatencyHistogram::new(args.hist_capacity));
    let book_l2_lat = Arc::new(LatencyHistogram::new(args.hist_capacity));
    let query_lat   = Arc::new(LatencyHistogram::new(args.hist_capacity));
    let slow_query_ms = args.slow_query_ms;

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

    if let Some(parent) = args.db.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
    }
    let pool = ConnPool::new(args.query_workers, &args.db)
        .with_context(|| format!("initialising DuckDB connection pool on {}", args.db.display()))?;
    info!(
        workers = args.query_workers,
        db = %args.db.display(),
        "DuckDB connection pool ready"
    );

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

    let hub = Arc::new(SubscriberHub::default());

    let stores_q = stores.clone();
    let hub_q = hub.clone();
    let query_lat_q = query_lat.clone();
    let bars_enabled = bar_interval_ns > 0;
    let _query_thread = std::thread::Builder::new()
        .name("rdb-query".into())
        .spawn(move || {
            run_query_server(
                listener, stores_q, hdb_dir, pool, bars_enabled, hub_q,
                query_lat_q, slow_query_ms,
            );
        })?;

    run_ingest_loop(
        args, stores, trade_lat, quote_lat, book_l2_lat, query_lat,
        bar_interval_ns, bar_row_cap, hub,
    )
}

// ---------------------------------------------------------------------------
// Subscriber hub — fan-out from ingest to streaming SUBSCRIBE clients
// ---------------------------------------------------------------------------

/// Unbounded? No — we use bounded channels so a slow subscriber simply drops
/// records (the broadcaster `try_send`s, treating Full as "skip this row but
/// keep the subscriber"). 16k records is ~1 MB at 64 B/record, generous for
/// burstiness without letting a stuck client run the server out of memory.
const SUBSCRIBER_QUEUE_CAP: usize = 16_384;

#[derive(Default)]
struct SubscriberHub {
    trades:  parking_lot::Mutex<Vec<chan::Sender<Trade>>>,
    quotes:  parking_lot::Mutex<Vec<chan::Sender<QuoteL1>>>,
    book_l2: parking_lot::Mutex<Vec<chan::Sender<BookL2>>>,
}

impl SubscriberHub {
    fn subscribe_trades(&self) -> chan::Receiver<Trade> {
        let (tx, rx) = chan::bounded(SUBSCRIBER_QUEUE_CAP);
        self.trades.lock().push(tx);
        rx
    }

    fn subscribe_quotes(&self) -> chan::Receiver<QuoteL1> {
        let (tx, rx) = chan::bounded(SUBSCRIBER_QUEUE_CAP);
        self.quotes.lock().push(tx);
        rx
    }

    fn subscribe_book_l2(&self) -> chan::Receiver<BookL2> {
        let (tx, rx) = chan::bounded(SUBSCRIBER_QUEUE_CAP);
        self.book_l2.lock().push(tx);
        rx
    }

    fn broadcast_trade(&self, t: &Trade) {
        let mut subs = self.trades.lock();
        subs.retain(|tx| match tx.try_send(*t) {
            Ok(()) => true,
            Err(chan::TrySendError::Full(_)) => true,
            Err(chan::TrySendError::Disconnected(_)) => false,
        });
    }

    fn broadcast_quote(&self, q: &QuoteL1) {
        let mut subs = self.quotes.lock();
        subs.retain(|tx| match tx.try_send(*q) {
            Ok(()) => true,
            Err(chan::TrySendError::Full(_)) => true,
            Err(chan::TrySendError::Disconnected(_)) => false,
        });
    }

    fn broadcast_book_l2(&self, b: &BookL2) {
        let mut subs = self.book_l2.lock();
        subs.retain(|tx| match tx.try_send(*b) {
            Ok(()) => true,
            Err(chan::TrySendError::Full(_)) => true,
            Err(chan::TrySendError::Disconnected(_)) => false,
        });
    }
}

// ---------------------------------------------------------------------------
// Connection pool
// ---------------------------------------------------------------------------

struct ConnPool {
    tx: chan::Sender<Connection>,
    rx: chan::Receiver<Connection>,
}

impl ConnPool {
    fn new(size: usize, db_path: &Path) -> anyhow::Result<Arc<Self>> {
        let (tx, rx) = chan::bounded(size);
        // Open one root connection to the persistent file; additional workers
        // share the same Database handle via try_clone so they all see the
        // same catalog (CREATE TABLE on one worker is visible to all).
        let root = Connection::open(db_path)
            .with_context(|| format!("opening DuckDB at {}", db_path.display()))?;
        root.register_table_function::<ArrowVTab>("arrow")?;
        for _ in 1..size {
            let conn = root.try_clone()?;
            conn.register_table_function::<ArrowVTab>("arrow")?;
            tx.send(conn).unwrap();
        }
        tx.send(root).unwrap();
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
    bars_enabled: bool,
    hub: Arc<SubscriberHub>,
    query_lat: Arc<LatencyHistogram>,
    slow_query_ms: u64,
) {
    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                let stores = stores.clone();
                let hdb_dir = hdb_dir.clone();
                let pool = pool.clone();
                let hub = hub.clone();
                let query_lat = query_lat.clone();
                std::thread::spawn(move || {
                    if let Err(e) = handle_connection(
                        stream, stores, hdb_dir, pool, bars_enabled, hub,
                        query_lat, slow_query_ms,
                    ) {
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
    bars_enabled: bool,
    hub: Arc<SubscriberHub>,
    query_lat: Arc<LatencyHistogram>,
    slow_query_ms: u64,
) -> anyhow::Result<()> {
    let sql = query_proto::read_request(&mut stream)?;
    info!(sql_chars = sql.len(), "query received");

    if query_proto::looks_like_subscribe(&sql) {
        return run_subscribe(stream, &sql, &stores, &hub);
    }

    let started = Instant::now();
    let conn = pool.acquire();
    let result = run_query(
        sql.as_str(),
        &stores,
        hdb_dir.as_ref().map(|p| p.as_path()),
        &conn,
        bars_enabled,
    );
    pool.release(conn);
    let elapsed = started.elapsed();
    query_lat.record(elapsed.as_nanos() as u64);
    if slow_query_ms > 0 && elapsed.as_millis() as u64 >= slow_query_ms {
        // Truncate the SQL preview to keep one log line bounded.
        let preview: String = sql.chars().take(200).collect();
        warn!(elapsed_ms = elapsed.as_millis() as u64, sql = %preview, "slow query");
    }

    match result {
        Ok(payload) => query_proto::write_response(&mut stream, query_proto::STATUS_OK, &payload)?,
        Err(e) => {
            let msg = format!("{:#}", e);
            query_proto::write_response(&mut stream, query_proto::STATUS_ERR, msg.as_bytes())?;
        }
    }
    Ok(())
}

fn run_query(
    sql: &str,
    stores: &StoreSet,
    hdb_dir: Option<&Path>,
    conn: &Connection,
    bars_enabled: bool,
) -> anyhow::Result<Vec<u8>> {
    sql_guard::check(sql)?;

    let trades_rb  = stores.snapshot_trades()?;
    let quotes_rb  = stores.snapshot_quotes()?;
    let book_l2_rb = stores.snapshot_book_l2()?;

    // Reset only the temp namespace — user catalog objects in the persistent
    // database are untouched. Every streaming fixture below is created in
    // `temp.` so this DROP is the inverse of the (re)creation that follows.
    conn.execute_batch(
        "DROP TABLE IF EXISTS temp.trades_live; DROP TABLE IF EXISTS temp.quotes_live;
         DROP TABLE IF EXISTS temp.book_l2_live;
         DROP TABLE IF EXISTS temp.trades_bars;
         DROP VIEW  IF EXISTS temp.trades_hist; DROP VIEW  IF EXISTS temp.quotes_hist;
         DROP VIEW  IF EXISTS temp.trades;      DROP VIEW  IF EXISTS temp.quotes;",
    )?;

    create_arrow_table(conn, "trades_live",  trades_rb)?;
    create_arrow_table(conn, "quotes_live",  quotes_rb)?;
    create_arrow_table(conn, "book_l2_live", book_l2_rb)?;
    if bars_enabled {
        let bars_rb = stores.snapshot_bars()?;
        create_arrow_table(conn, "trades_bars", bars_rb)?;
    }
    mount_hdb(conn, hdb_dir)?;

    let mut stmt = conn.prepare(sql)?;
    let arrow_iter = stmt.query_arrow([])?;
    let result_schema = arrow_iter.get_schema();
    let batches: Vec<RecordBatch> = arrow_iter.collect();
    encode_ipc_stream(&batches, &result_schema)
}

fn create_arrow_table(conn: &Connection, name: &str, rb: RecordBatch) -> anyhow::Result<()> {
    let params = arrow_recordbatch_to_query_params(rb);
    let sql = format!("CREATE TEMP TABLE {name} AS SELECT * FROM arrow(?, ?)");
    let mut stmt = conn.prepare(&sql)?;
    stmt.execute(params)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Streaming SUBSCRIBE handler
// ---------------------------------------------------------------------------

fn run_subscribe(
    mut stream: UnixStream,
    sql: &str,
    stores: &StoreSet,
    hub: &SubscriberHub,
) -> anyhow::Result<()> {
    let topic = parse_subscribe_topic(sql)?;
    info!(topic, "subscribe accepted");
    // Initial OK ack so the client can detect a successful subscription.
    if query_proto::write_response(&mut stream, query_proto::STATUS_OK, &[]).is_err() {
        return Ok(());
    }
    match topic.as_str() {
        "trades"   => stream_trades(&mut stream, stores, hub),
        "quotes"   => stream_quotes(&mut stream, stores, hub),
        "book_l2"  => stream_book_l2(&mut stream, stores, hub),
        other => {
            let msg = format!(
                "subscribe: unknown topic '{other}' (try trades, quotes, book_l2)"
            );
            let _ = query_proto::write_response(
                &mut stream, query_proto::STATUS_ERR, msg.as_bytes(),
            );
            Ok(())
        }
    }
}

/// Parse `SUBSCRIBE <topic>` into the lower-case topic name.
fn parse_subscribe_topic(sql: &str) -> anyhow::Result<String> {
    let mut iter = sql.split_whitespace();
    let verb = iter.next().unwrap_or("");
    if !verb.eq_ignore_ascii_case("SUBSCRIBE") {
        anyhow::bail!("not a SUBSCRIBE statement");
    }
    let topic = iter.next().unwrap_or("").trim_end_matches(';').to_ascii_lowercase();
    if topic.is_empty() {
        anyhow::bail!("SUBSCRIBE requires a topic name");
    }
    Ok(topic)
}

const STREAM_FLUSH_ROWS: usize = 256;
const STREAM_FLUSH_MS:   u64   = 50;

fn stream_trades(
    stream: &mut UnixStream,
    stores: &StoreSet,
    hub: &SubscriberHub,
) -> anyhow::Result<()> {
    let rx = hub.subscribe_trades();
    let mut buf: Vec<Trade> = Vec::with_capacity(STREAM_FLUSH_ROWS);
    let mut last_flush = Instant::now();
    let timeout = Duration::from_millis(STREAM_FLUSH_MS);
    loop {
        // Block for the first record, then drain the rest non-blockingly.
        match rx.recv_timeout(timeout) {
            Ok(t) => buf.push(t),
            Err(chan::RecvTimeoutError::Timeout) => {}
            Err(chan::RecvTimeoutError::Disconnected) => break,
        }
        while buf.len() < STREAM_FLUSH_ROWS {
            match rx.try_recv() {
                Ok(t) => buf.push(t),
                Err(_) => break,
            }
        }
        let due = !buf.is_empty()
            && (buf.len() >= STREAM_FLUSH_ROWS || last_flush.elapsed() >= timeout);
        if due {
            let rb = trades_to_batch(&buf, stores)?;
            let payload = encode_ipc_stream(&[rb], &stores.trades_schema)?;
            if query_proto::write_response(&mut *stream, query_proto::STATUS_BATCH, &payload).is_err() {
                break;
            }
            buf.clear();
            last_flush = Instant::now();
        }
    }
    Ok(())
}

fn stream_quotes(
    stream: &mut UnixStream,
    stores: &StoreSet,
    hub: &SubscriberHub,
) -> anyhow::Result<()> {
    let rx = hub.subscribe_quotes();
    let mut buf: Vec<QuoteL1> = Vec::with_capacity(STREAM_FLUSH_ROWS);
    let mut last_flush = Instant::now();
    let timeout = Duration::from_millis(STREAM_FLUSH_MS);
    loop {
        match rx.recv_timeout(timeout) {
            Ok(q) => buf.push(q),
            Err(chan::RecvTimeoutError::Timeout) => {}
            Err(chan::RecvTimeoutError::Disconnected) => break,
        }
        while buf.len() < STREAM_FLUSH_ROWS {
            match rx.try_recv() {
                Ok(q) => buf.push(q),
                Err(_) => break,
            }
        }
        let due = !buf.is_empty()
            && (buf.len() >= STREAM_FLUSH_ROWS || last_flush.elapsed() >= timeout);
        if due {
            let rb = quotes_to_batch(&buf, stores)?;
            let payload = encode_ipc_stream(&[rb], &stores.quotes_schema)?;
            if query_proto::write_response(&mut *stream, query_proto::STATUS_BATCH, &payload).is_err() {
                break;
            }
            buf.clear();
            last_flush = Instant::now();
        }
    }
    Ok(())
}

fn trades_to_batch(rows: &[Trade], stores: &StoreSet) -> anyhow::Result<RecordBatch> {
    let mut symbol = Vec::with_capacity(rows.len());
    let mut symbol_id = Vec::with_capacity(rows.len());
    let mut seq = Vec::with_capacity(rows.len());
    let mut ts_exchange_ns = Vec::with_capacity(rows.len());
    let mut ts_local_ns = Vec::with_capacity(rows.len());
    let mut price = Vec::with_capacity(rows.len());
    let mut qty = Vec::with_capacity(rows.len());
    let mut side = Vec::with_capacity(rows.len());
    for t in rows {
        let store = match stores.store_for(t.symbol_id) {
            Some(s) => s,
            None => continue,
        };
        symbol.push(store.symbol.clone());
        symbol_id.push(t.symbol_id);
        seq.push(t.seq);
        ts_exchange_ns.push(t.ts_exchange_ns);
        ts_local_ns.push(t.ts_local_ns);
        price.push(decode_fixed_local(t.price, store.price_scale));
        qty.push(decode_fixed_local(t.qty, store.qty_scale));
        side.push(Side::from_u8(t.side).as_str().to_string());
    }
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(symbol)),
        Arc::new(UInt32Array::from(symbol_id)),
        Arc::new(UInt64Array::from(seq)),
        Arc::new(UInt64Array::from(ts_exchange_ns)),
        Arc::new(UInt64Array::from(ts_local_ns)),
        Arc::new(Float64Array::from(price)),
        Arc::new(Float64Array::from(qty)),
        Arc::new(StringArray::from(side)),
    ];
    Ok(RecordBatch::try_new(stores.trades_schema.clone(), arrays)?)
}

fn stream_book_l2(
    stream: &mut UnixStream,
    stores: &StoreSet,
    hub: &SubscriberHub,
) -> anyhow::Result<()> {
    let rx = hub.subscribe_book_l2();
    let mut buf: Vec<BookL2> = Vec::with_capacity(STREAM_FLUSH_ROWS);
    let mut last_flush = Instant::now();
    let timeout = Duration::from_millis(STREAM_FLUSH_MS);
    loop {
        match rx.recv_timeout(timeout) {
            Ok(b) => buf.push(b),
            Err(chan::RecvTimeoutError::Timeout) => {}
            Err(chan::RecvTimeoutError::Disconnected) => break,
        }
        while buf.len() < STREAM_FLUSH_ROWS {
            match rx.try_recv() {
                Ok(b) => buf.push(b),
                Err(_) => break,
            }
        }
        let due = !buf.is_empty()
            && (buf.len() >= STREAM_FLUSH_ROWS || last_flush.elapsed() >= timeout);
        if due {
            let rb = book_l2_to_batch(&buf, stores)?;
            let payload = encode_ipc_stream(&[rb], &stores.book_l2_schema)?;
            if query_proto::write_response(&mut *stream, query_proto::STATUS_BATCH, &payload).is_err() {
                break;
            }
            buf.clear();
            last_flush = Instant::now();
        }
    }
    Ok(())
}

fn book_l2_to_batch(rows: &[BookL2], stores: &StoreSet) -> anyhow::Result<RecordBatch> {
    use tp_types::BOOK_L2_LEVELS;
    let mut symbol = Vec::with_capacity(rows.len());
    let mut symbol_id = Vec::with_capacity(rows.len());
    let mut seq = Vec::with_capacity(rows.len());
    let mut ts_exchange_ns = Vec::with_capacity(rows.len());
    let mut ts_local_ns = Vec::with_capacity(rows.len());
    let mut bid_prices: Vec<Vec<f64>> = (0..BOOK_L2_LEVELS).map(|_| Vec::new()).collect();
    let mut bid_qtys:   Vec<Vec<f64>> = (0..BOOK_L2_LEVELS).map(|_| Vec::new()).collect();
    let mut ask_prices: Vec<Vec<f64>> = (0..BOOK_L2_LEVELS).map(|_| Vec::new()).collect();
    let mut ask_qtys:   Vec<Vec<f64>> = (0..BOOK_L2_LEVELS).map(|_| Vec::new()).collect();

    for b in rows {
        let store = match stores.store_for(b.symbol_id) {
            Some(s) => s,
            None => continue,
        };
        symbol.push(store.symbol.clone());
        symbol_id.push(b.symbol_id);
        seq.push(b.seq);
        ts_exchange_ns.push(b.ts_exchange_ns);
        ts_local_ns.push(b.ts_local_ns);
        for lvl in 0..BOOK_L2_LEVELS {
            bid_prices[lvl].push(decode_fixed_local(b.bid_prices[lvl], store.price_scale));
            bid_qtys[lvl].push(decode_fixed_local(b.bid_qtys[lvl],     store.qty_scale));
            ask_prices[lvl].push(decode_fixed_local(b.ask_prices[lvl], store.price_scale));
            ask_qtys[lvl].push(decode_fixed_local(b.ask_qtys[lvl],     store.qty_scale));
        }
    }
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(5 + 4 * BOOK_L2_LEVELS);
    arrays.push(Arc::new(StringArray::from(symbol)));
    arrays.push(Arc::new(UInt32Array::from(symbol_id)));
    arrays.push(Arc::new(UInt64Array::from(seq)));
    arrays.push(Arc::new(UInt64Array::from(ts_exchange_ns)));
    arrays.push(Arc::new(UInt64Array::from(ts_local_ns)));
    for lvl in 0..BOOK_L2_LEVELS {
        arrays.push(Arc::new(Float64Array::from(std::mem::take(&mut bid_prices[lvl]))));
        arrays.push(Arc::new(Float64Array::from(std::mem::take(&mut bid_qtys[lvl]))));
    }
    for lvl in 0..BOOK_L2_LEVELS {
        arrays.push(Arc::new(Float64Array::from(std::mem::take(&mut ask_prices[lvl]))));
        arrays.push(Arc::new(Float64Array::from(std::mem::take(&mut ask_qtys[lvl]))));
    }
    Ok(RecordBatch::try_new(stores.book_l2_schema.clone(), arrays)?)
}

fn quotes_to_batch(rows: &[QuoteL1], stores: &StoreSet) -> anyhow::Result<RecordBatch> {
    let mut symbol = Vec::with_capacity(rows.len());
    let mut symbol_id = Vec::with_capacity(rows.len());
    let mut seq = Vec::with_capacity(rows.len());
    let mut ts_exchange_ns = Vec::with_capacity(rows.len());
    let mut ts_local_ns = Vec::with_capacity(rows.len());
    let mut bid_price = Vec::with_capacity(rows.len());
    let mut bid_qty = Vec::with_capacity(rows.len());
    let mut ask_price = Vec::with_capacity(rows.len());
    let mut ask_qty = Vec::with_capacity(rows.len());
    for q in rows {
        let store = match stores.store_for(q.symbol_id) {
            Some(s) => s,
            None => continue,
        };
        symbol.push(store.symbol.clone());
        symbol_id.push(q.symbol_id);
        seq.push(q.seq);
        ts_exchange_ns.push(q.ts_exchange_ns);
        ts_local_ns.push(q.ts_local_ns);
        bid_price.push(decode_fixed_local(q.bid_price, store.price_scale));
        bid_qty.push(decode_fixed_local(q.bid_qty, store.qty_scale));
        ask_price.push(decode_fixed_local(q.ask_price, store.price_scale));
        ask_qty.push(decode_fixed_local(q.ask_qty, store.qty_scale));
    }
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(symbol)),
        Arc::new(UInt32Array::from(symbol_id)),
        Arc::new(UInt64Array::from(seq)),
        Arc::new(UInt64Array::from(ts_exchange_ns)),
        Arc::new(UInt64Array::from(ts_local_ns)),
        Arc::new(Float64Array::from(bid_price)),
        Arc::new(Float64Array::from(bid_qty)),
        Arc::new(Float64Array::from(ask_price)),
        Arc::new(Float64Array::from(ask_qty)),
    ];
    Ok(RecordBatch::try_new(stores.quotes_schema.clone(), arrays)?)
}

fn decode_fixed_local(v: i64, scale: u8) -> f64 {
    let mut p = 1.0f64;
    for _ in 0..scale { p *= 10.0; }
    (v as f64) / p
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

fn build_union_view(name: &str, hdb_dir: Option<&Path>) -> String {
    let live = format!("{name}_live");
    let has_hist = hdb_dir
        .map(|d| has_parquet_files(&d.join(name)))
        .unwrap_or(false);

    if has_hist {
        let glob = hdb_dir.unwrap().join(name).join("**").join("*.parquet");
        let cols = hist_columns(name);
        format!(
            "CREATE TEMP VIEW {name}_hist AS \
               SELECT {cols} \
               FROM read_parquet('{glob}', hive_partitioning=true);\n\
             CREATE TEMP VIEW {name} AS \
               SELECT * FROM {live} UNION ALL SELECT * FROM {name}_hist;",
            glob = glob.display(),
        )
    } else {
        format!("CREATE TEMP VIEW {name} AS SELECT * FROM {live};")
    }
}

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
// WAL replay
// ---------------------------------------------------------------------------

fn replay_wal(
    wal_dir: &Path,
    stores: &StoreSet,
    row_cap: usize,
    since_secs: u64,
    bar_interval_ns: u64,
    bar_row_cap: usize,
) -> anyhow::Result<()> {
    let trade_path   = wal_dir.join("trades.wal");
    let quote_path   = wal_dir.join("quotes.wal");
    let book_l2_path = wal_dir.join("book_l2.wal");
    if !trade_path.exists() && !quote_path.exists() && !book_l2_path.exists() {
        info!(wal_dir = %wal_dir.display(), "no WAL files to replay");
        return Ok(());
    }

    let cutoff_ns: u64 = if since_secs == 0 {
        0
    } else {
        wall_ns().saturating_sub(since_secs.saturating_mul(1_000_000_000))
    };

    let mut trade_replayed = 0u64;
    let mut trade_skipped_old = 0u64;
    let mut trade_unknown_sym = 0u64;
    if trade_path.exists() {
        let trades = wal::read_all::<Trade>(&trade_path)
            .with_context(|| format!("reading {}", trade_path.display()))?;
        for t in &trades {
            if cutoff_ns > 0 && t.ts_local_ns < cutoff_ns {
                trade_skipped_old += 1;
                continue;
            }
            match stores.store_for(t.symbol_id) {
                Some(store) => {
                    store.push_trade(t, row_cap, bar_interval_ns, bar_row_cap);
                    trade_replayed += 1;
                }
                None => trade_unknown_sym += 1,
            }
        }
    }

    let mut quote_replayed = 0u64;
    let mut quote_skipped_old = 0u64;
    let mut quote_unknown_sym = 0u64;
    if quote_path.exists() {
        let quotes = wal::read_all::<QuoteL1>(&quote_path)
            .with_context(|| format!("reading {}", quote_path.display()))?;
        for q in &quotes {
            if cutoff_ns > 0 && q.ts_local_ns < cutoff_ns {
                quote_skipped_old += 1;
                continue;
            }
            match stores.store_for(q.symbol_id) {
                Some(store) => {
                    store.quotes.lock().push(q, row_cap);
                    quote_replayed += 1;
                }
                None => quote_unknown_sym += 1,
            }
        }
    }

    let mut book_l2_replayed = 0u64;
    let mut book_l2_skipped_old = 0u64;
    let mut book_l2_unknown_sym = 0u64;
    if book_l2_path.exists() {
        let books = wal::read_all::<BookL2>(&book_l2_path)
            .with_context(|| format!("reading {}", book_l2_path.display()))?;
        for b in &books {
            if cutoff_ns > 0 && b.ts_local_ns < cutoff_ns {
                book_l2_skipped_old += 1;
                continue;
            }
            match stores.store_for(b.symbol_id) {
                Some(store) => {
                    store.book_l2.lock().push(b, row_cap);
                    book_l2_replayed += 1;
                }
                None => book_l2_unknown_sym += 1,
            }
        }
    }

    info!(
        wal_dir = %wal_dir.display(),
        trade_replayed,
        trade_skipped_old,
        trade_unknown_sym,
        quote_replayed,
        quote_skipped_old,
        quote_unknown_sym,
        book_l2_replayed,
        book_l2_skipped_old,
        book_l2_unknown_sym,
        "WAL replay complete"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Ingest loop
// ---------------------------------------------------------------------------

fn run_ingest_loop(
    args: Args,
    stores: Arc<StoreSet>,
    trade_lat: Arc<LatencyHistogram>,
    quote_lat: Arc<LatencyHistogram>,
    book_l2_lat: Arc<LatencyHistogram>,
    query_lat: Arc<LatencyHistogram>,
    bar_interval_ns: u64,
    bar_row_cap: usize,
    hub: Arc<SubscriberHub>,
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
    let book_l2_svc = node
        .service_builder(&topics::BOOK_L2_AGG.try_into()?)
        .publish_subscribe::<BookL2>()
        .subscriber_max_buffer_size(ipc_cfg::SUBSCRIBER_MAX_BUFFER_SIZE)
        .history_size(ipc_cfg::HISTORY_SIZE)
        .max_publishers(ipc_cfg::MAX_PUBLISHERS)
        .max_subscribers(ipc_cfg::MAX_SUBSCRIBERS)
        .open_or_create()?;

    let trade_sub   = trades_svc.subscriber_builder().create()?;
    let quote_sub   = quotes_svc.subscriber_builder().create()?;
    let book_l2_sub = book_l2_svc.subscriber_builder().create()?;

    let mut last_msg_at = wall_ns();
    let mut last_log_at = wall_ns();
    let log_period_ns = 1_000_000_000u64;
    let idle_sleep = Duration::from_micros(args.idle_sleep_us);
    let idle_exit_ns = args.idle_exit_secs.saturating_mul(1_000_000_000);

    let mut trade_count:   u64 = 0;
    let mut quote_count:   u64 = 0;
    let mut book_l2_count: u64 = 0;

    loop {
        let mut did_work = false;

        while let Some(sample) = trade_sub.receive()? {
            let trade = *sample;
            let now = wall_ns();
            trade_lat.record(now.saturating_sub(trade.ts_local_ns));
            if let Some(store) = stores.store_for(trade.symbol_id) {
                store.push_trade(&trade, row_cap, bar_interval_ns, bar_row_cap);
                trade_count += 1;
                hub.broadcast_trade(&trade);
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
                hub.broadcast_quote(&q);
            } else {
                warn!(symbol_id = q.symbol_id, "quote for unknown symbol_id");
                quote_lat.record_drop();
            }
            did_work = true;
        }

        while let Some(sample) = book_l2_sub.receive()? {
            let b = *sample;
            let now = wall_ns();
            book_l2_lat.record(now.saturating_sub(b.ts_local_ns));
            if let Some(store) = stores.store_for(b.symbol_id) {
                store.book_l2.lock().push(&b, row_cap);
                book_l2_count += 1;
                hub.broadcast_book_l2(&b);
            } else {
                warn!(symbol_id = b.symbol_id, "book_l2 for unknown symbol_id");
                book_l2_lat.record_drop();
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

        if args.limit != 0 && trade_count + quote_count + book_l2_count >= args.limit {
            info!(trade_count, quote_count, book_l2_count, "limit reached; exiting");
            break;
        }

        if now.saturating_sub(last_log_at) > log_period_ns {
            let t = trade_lat.snapshot();
            let q = quote_lat.snapshot();
            let b = book_l2_lat.snapshot();
            let qry = query_lat.snapshot();
            info!(
                trade_count, quote_count, book_l2_count,
                trade_p50_ns   = ?t.p50, trade_p99_ns   = ?t.p99, trade_samples   = t.samples,
                quote_p50_ns   = ?q.p50, quote_p99_ns   = ?q.p99, quote_samples   = q.samples,
                book_l2_p50_ns = ?b.p50, book_l2_p99_ns = ?b.p99, book_l2_samples = b.samples,
                query_p50_ns   = ?qry.p50, query_p99_ns = ?qry.p99, query_samples = qry.samples,
                "rdb stats"
            );
            last_log_at = now;
        }
    }

    let t = trade_lat.snapshot();
    let q = quote_lat.snapshot();
    let b = book_l2_lat.snapshot();
    let qry = query_lat.snapshot();
    info!(
        trade_count, quote_count, book_l2_count,
        trade_p50_ns   = ?t.p50, trade_p99_ns   = ?t.p99,
        quote_p50_ns   = ?q.p50, quote_p99_ns   = ?q.p99,
        book_l2_p50_ns = ?b.p50, book_l2_p99_ns = ?b.p99,
        query_p50_ns   = ?qry.p50, query_p99_ns = ?qry.p99, query_samples = qry.samples,
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

        let batch = match stores.snapshot_for_rollup() {
            Ok(b) => b,
            Err(e) => { error!(error = %e, "rollup: snapshot failed"); continue; }
        };

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

fn sql_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn unix_secs_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

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
