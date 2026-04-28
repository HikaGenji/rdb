//! End-to-end integration test.
//!
//! Drives the consolidated `rdb` binary through three of its subcommands
//! (`rdb serve`, `rdb tickerplant`, `rdb feed-replayer`) over a sample
//! JSONL feed and verifies:
//!
//! 1. The rdb's `trades` table has the expected row count.
//! 2. An asof-join between trades and quotes produces a row count matching
//!    the trade count and binds reasonable bid/ask values.
//!
//! The test owns its own scratch directory under
//! `$TMPDIR/rdb-it-<pid>-<label>-<rand>/` and removes `/tmp/iceoryx2/` at
//! start to clear stale shared-memory segments. iceoryx2 0.7 has no env
//! override for the segment root so concurrent runs of this test on the
//! same host will collide.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use arrow_array::{Float64Array, RecordBatch, StringArray, UInt64Array};
use tp_arrow::decode_ipc_stream;

const REQ_OK: u8 = 0;
const REQ_ERR: u8 = 1;

// Both tests in this binary spawn `rdb serve`, which subscribes to the
// shared iceoryx2 topics under /tmp/iceoryx2/ and is bounded by
// MAX_SUBSCRIBERS. Serialize them so they don't fight for slots.
static SERIAL: Mutex<()> = Mutex::new(());

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root")
        .to_path_buf()
}

fn rdb_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_rdb"))
}

/// Spawn the rdb binary with the given subcommand.
fn rdb_cmd(subcommand: &str) -> Command {
    let mut c = Command::new(rdb_binary());
    c.arg(subcommand);
    c
}

/// Build the rdb binary so the integration test has something to spawn.
fn ensure_binaries_built() {
    let status = Command::new(env!("CARGO"))
        .args(["build", "--bin", "rdb", "--quiet"])
        .current_dir(workspace_root())
        .status()
        .expect("cargo build");
    assert!(status.success(), "cargo build failed");
}

struct ScratchDir {
    path: PathBuf,
}

impl ScratchDir {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "rdb-it-{}-{}-{}",
            std::process::id(),
            label,
            now_nanos() % 1_000_000
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        Self { path }
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn now_nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

fn wait_for_socket(path: &Path, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if path.exists() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    false
}

fn query(socket: &Path, sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
    let mut stream = UnixStream::connect(socket)?;
    let bytes = sql.as_bytes();
    stream.write_all(&(bytes.len() as u32).to_le_bytes())?;
    stream.write_all(bytes)?;
    stream.flush()?;

    let mut s = [0u8; 1];
    stream.read_exact(&mut s)?;
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload)?;

    match s[0] {
        REQ_OK => Ok(decode_ipc_stream(&payload)?),
        REQ_ERR => anyhow::bail!("server error: {}", String::from_utf8_lossy(&payload)),
        other => anyhow::bail!("unknown status {other}"),
    }
}

struct ChildGuard(Option<Child>);

impl ChildGuard {
    fn new(c: Child) -> Self { Self(Some(c)) }
    fn wait(mut self) -> std::io::Result<std::process::ExitStatus> {
        self.0.as_mut().unwrap().wait()
    }
    fn kill(&mut self) {
        if let Some(c) = self.0.as_mut() {
            let _ = c.kill();
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        self.kill();
        if let Some(mut c) = self.0.take() {
            let _ = c.wait();
        }
    }
}

#[test]
fn end_to_end_replay_and_asof() {
    let _g = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    ensure_binaries_built();

    let scratch = ScratchDir::new("e2e");
    let symbols = workspace_root().join("config/symbols.toml");
    let sample  = workspace_root().join("samples/sample.jsonl");
    assert!(symbols.exists(), "config/symbols.toml not found");
    assert!(sample.exists(), "samples/sample.jsonl not found");

    let wal_dir = scratch.path.join("wal");
    let socket  = scratch.path.join("rdb.sock");
    let db      = scratch.path.join("rdb.duckdb");

    // iceoryx2 0.7 has no env-based config: shared-memory segments live
    // under /tmp/iceoryx2/. Clean it before the run to avoid picking up
    // segments from a previous aborted run with mismatched type sizes.
    let _ = std::fs::remove_dir_all("/tmp/iceoryx2");

    // Start the rdb first so it is ready to receive aggregated records.
    let mut rdb = rdb_cmd("serve");
    rdb.arg("--symbols").arg(&symbols)
       .arg("--socket").arg(&socket)
       .arg("--db").arg(&db)
       .arg("--idle-exit-secs").arg("3")
       .env("RUST_LOG", "warn");
    let mut rdb = ChildGuard::new(rdb.spawn().expect("spawn rdb serve"));

    assert!(
        wait_for_socket(&socket, Duration::from_secs(5)),
        "rdb socket did not appear at {}",
        socket.display()
    );

    // Tickerplant.
    let mut tp = rdb_cmd("tickerplant");
    tp.arg("--symbols").arg(&symbols)
      .arg("--wal-dir").arg(&wal_dir)
      .arg("--idle-exit-secs").arg("2")
      .env("RUST_LOG", "warn");
    let tp = ChildGuard::new(tp.spawn().expect("spawn rdb tickerplant"));

    // Give the tickerplant a moment to attach its subscribers before the
    // replayer starts publishing — iceoryx2 subscribers only see samples
    // sent after they connect.
    std::thread::sleep(Duration::from_millis(500));

    // Feed replayer (firehose mode, full sample file).
    let mut fr = rdb_cmd("feed-replayer");
    fr.arg("--symbols").arg(&symbols)
      .arg("--input").arg(&sample)
      .arg("--pace").arg("firehose")
      .env("RUST_LOG", "warn");
    let fr = ChildGuard::new(fr.spawn().expect("spawn rdb feed-replayer"));

    // Wait for replayer to finish, then for tickerplant to drain and exit.
    let fr_status = fr.wait().expect("wait fr");
    assert!(fr_status.success(), "feed-replayer failed: {fr_status:?}");
    let tp_status = tp.wait().expect("wait tp");
    assert!(tp_status.success(), "tickerplant failed: {tp_status:?}");

    // The rdb is still up. Issue queries while it is running.
    let count_batches = query(&socket, "SELECT COUNT(*)::BIGINT AS n FROM trades").unwrap();
    let n = count_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .expect("count column is i64")
        .value(0);
    assert_eq!(n, 6, "expected 6 trades, got {n}");

    let asof_sql = r#"
        SELECT
            t.symbol,
            t.seq,
            t.price,
            q.bid_price,
            q.ask_price
        FROM trades t
        ASOF LEFT JOIN quotes q
          ON t.symbol = q.symbol
         AND q.ts_exchange_ns <= t.ts_exchange_ns
        ORDER BY t.symbol, t.seq
    "#;
    let asof_batches = query(&socket, asof_sql).unwrap();
    let total: usize = asof_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 6, "asof join returned {total} rows, expected 6");

    // The first BTC trade (price 65000.4) should be matched against the
    // first BTC quote (bid=65000.0, ask=65000.5).
    let first = &asof_batches[0];
    let symbol = first.column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0);
    let price  = first.column(2).as_any().downcast_ref::<Float64Array>().unwrap().value(0);
    let bid    = first.column(3).as_any().downcast_ref::<Float64Array>().unwrap().value(0);
    let ask    = first.column(4).as_any().downcast_ref::<Float64Array>().unwrap().value(0);
    assert_eq!(symbol, "BTC-PERP", "first row symbol is {symbol}");
    assert!((price - 65000.4).abs() < 1e-6, "price mismatch {price}");
    assert!(bid <= price && price <= ask, "asof bid<=price<=ask failed: bid={bid} price={price} ask={ask}");

    // Verify the per-symbol seq numbers are present and contiguous within
    // each stream by re-querying.
    let seq_batches = query(&socket, "SELECT seq FROM trades ORDER BY seq").unwrap();
    let seqs: Vec<u64> = seq_batches.iter()
        .flat_map(|b| {
            b.column(0).as_any().downcast_ref::<UInt64Array>().unwrap()
                .iter().map(|v| v.unwrap()).collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(seqs, vec![1, 2, 3, 4, 5, 6], "trade seqs are not 1..=6");

    // Online OHLCV bars are maintained per tick and must reflect the same
    // VWAP as the raw trades, computed end-to-end.
    let bar_batches = query(
        &socket,
        "SELECT symbol, COUNT(*) AS n_bars, SUM(volume) AS total_vol \
         FROM trades_bars GROUP BY symbol ORDER BY symbol",
    )
    .unwrap();
    let total_rows: usize = bar_batches.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows >= 1, "expected at least one bar, got {total_rows}");

    // Cross-check: bar VWAP equals raw-trade VWAP per symbol.
    let cross = query(
        &socket,
        "WITH \
           raw AS (SELECT symbol, SUM(price*qty)/SUM(qty) AS vwap_raw \
                   FROM trades GROUP BY symbol), \
           bar AS (SELECT symbol, SUM(vwap*volume)/SUM(volume) AS vwap_bar \
                   FROM trades_bars GROUP BY symbol) \
         SELECT raw.symbol, raw.vwap_raw, bar.vwap_bar \
         FROM raw JOIN bar USING (symbol) ORDER BY raw.symbol",
    )
    .unwrap();
    for batch in &cross {
        let raws = batch.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        let bars = batch.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
        for i in 0..batch.num_rows() {
            assert!(
                (raws.value(i) - bars.value(i)).abs() < 1e-6,
                "vwap mismatch row {i}: raw={} bar={}",
                raws.value(i),
                bars.value(i)
            );
        }
    }

    // Tear down rdb. Ingest will idle out after 3s; we can also kill it.
    rdb.kill();
}

// ---------------------------------------------------------------------------
// User table CRUD against the persistent DuckDB catalog
// ---------------------------------------------------------------------------

/// Send SQL and return either OK batches or the server's error string. The
/// existing `query` helper bails on REQ_ERR; the guard test needs the text.
fn query_result(socket: &Path, sql: &str) -> std::result::Result<Vec<RecordBatch>, String> {
    let mut stream = UnixStream::connect(socket).map_err(|e| e.to_string())?;
    let bytes = sql.as_bytes();
    stream.write_all(&(bytes.len() as u32).to_le_bytes()).map_err(|e| e.to_string())?;
    stream.write_all(bytes).map_err(|e| e.to_string())?;
    stream.flush().map_err(|e| e.to_string())?;

    let mut s = [0u8; 1];
    stream.read_exact(&mut s).map_err(|e| e.to_string())?;
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).map_err(|e| e.to_string())?;
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).map_err(|e| e.to_string())?;

    match s[0] {
        REQ_OK => decode_ipc_stream(&payload).map_err(|e| e.to_string()),
        REQ_ERR => Err(String::from_utf8_lossy(&payload).into_owned()),
        other => Err(format!("unknown status {other}")),
    }
}

fn spawn_serve(symbols: &Path, socket: &Path, db: &Path) -> ChildGuard {
    let mut cmd = rdb_cmd("serve");
    cmd.arg("--symbols").arg(symbols)
       .arg("--socket").arg(socket)
       .arg("--db").arg(db)
       // Long enough that the test finishes its work before idle-exit kicks
       // in; ChildGuard kills explicitly on drop in any case.
       .arg("--idle-exit-secs").arg("60")
       .env("RUST_LOG", "warn");
    let child = cmd.spawn().expect("spawn rdb serve");
    let g = ChildGuard::new(child);
    assert!(
        wait_for_socket(socket, Duration::from_secs(5)),
        "rdb socket did not appear at {}",
        socket.display()
    );
    g
}

#[test]
fn wal_replay_restores_in_memory_rows_after_restart() {
    // Phase 1: run tickerplant + replayer with `--wal-dir`. No rdb is
    // listening so the */agg samples are simply dropped — only the WAL
    // files matter.
    // Phase 2: start `rdb serve --wal-dir <same dir>` with no live ingest
    // and confirm the in-memory store reflects the replayed records.
    let _g = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    ensure_binaries_built();

    let scratch = ScratchDir::new("wal-replay");
    let symbols = workspace_root().join("config/symbols.toml");
    let sample  = workspace_root().join("samples/sample.jsonl");
    assert!(symbols.exists(), "config/symbols.toml not found");
    assert!(sample.exists(), "samples/sample.jsonl not found");

    let wal_dir = scratch.path.join("wal");
    let socket  = scratch.path.join("rdb.sock");
    let db      = scratch.path.join("rdb.duckdb");

    let _ = std::fs::remove_dir_all("/tmp/iceoryx2");

    // --- Phase 1: populate the WAL ---
    let mut tp = rdb_cmd("tickerplant");
    tp.arg("--symbols").arg(&symbols)
      .arg("--wal-dir").arg(&wal_dir)
      .arg("--idle-exit-secs").arg("3")
      // Force every append to fsync, so even in this short-lived test the
      // WAL is on stable storage by the time the tickerplant exits.
      .arg("--wal-fsync-batch").arg("1")
      .env("RUST_LOG", "warn");
    let tp = ChildGuard::new(tp.spawn().expect("spawn rdb tickerplant"));

    std::thread::sleep(Duration::from_millis(500));

    let mut fr = rdb_cmd("feed-replayer");
    fr.arg("--symbols").arg(&symbols)
      .arg("--input").arg(&sample)
      .arg("--pace").arg("firehose")
      .env("RUST_LOG", "warn");
    let fr = ChildGuard::new(fr.spawn().expect("spawn rdb feed-replayer"));

    let fr_status = fr.wait().expect("wait fr");
    assert!(fr_status.success(), "feed-replayer failed: {fr_status:?}");
    let tp_status = tp.wait().expect("wait tp");
    assert!(tp_status.success(), "tickerplant failed: {tp_status:?}");

    // The WAL files must exist and be non-empty.
    let trades_wal = wal_dir.join("trades.wal");
    let quotes_wal = wal_dir.join("quotes.wal");
    assert!(trades_wal.exists(), "trades.wal not created");
    assert!(quotes_wal.exists(), "quotes.wal not created");

    // Clear the iceoryx2 segments so the new serve starts clean — no
    // leftover */agg subscribers from the prior tickerplant run.
    let _ = std::fs::remove_dir_all("/tmp/iceoryx2");
    std::thread::sleep(Duration::from_millis(200));

    // --- Phase 2: start a fresh rdb pointed at the WAL dir ---
    let mut rdb = rdb_cmd("serve");
    rdb.arg("--symbols").arg(&symbols)
       .arg("--socket").arg(&socket)
       .arg("--db").arg(&db)
       .arg("--wal-dir").arg(&wal_dir)
       .arg("--idle-exit-secs").arg("3")
       .env("RUST_LOG", "warn");
    let mut rdb = ChildGuard::new(rdb.spawn().expect("spawn rdb serve"));

    assert!(
        wait_for_socket(&socket, Duration::from_secs(5)),
        "rdb socket did not appear at {}", socket.display()
    );

    let count = query(&socket, "SELECT COUNT(*)::BIGINT FROM trades").unwrap();
    let n = count[0].column(0).as_any()
        .downcast_ref::<arrow_array::Int64Array>().unwrap().value(0);
    assert_eq!(n, 6, "expected 6 trades replayed from WAL, got {n}");

    let qcount = query(&socket, "SELECT COUNT(*)::BIGINT FROM quotes").unwrap();
    let qn = qcount[0].column(0).as_any()
        .downcast_ref::<arrow_array::Int64Array>().unwrap().value(0);
    assert!(qn > 0, "expected at least one quote replayed, got {qn}");

    rdb.kill();
}

#[test]
fn create_table_persists_and_guard_protects_streaming_views() {
    let _g = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    ensure_binaries_built();

    let scratch = ScratchDir::new("user-tables");
    let symbols = workspace_root().join("config/symbols.toml");
    assert!(symbols.exists(), "config/symbols.toml not found");

    let socket = scratch.path.join("rdb.sock");
    let db     = scratch.path.join("rdb.duckdb");

    // --- Round 1: create + insert + read back ---
    {
        let mut rdb = spawn_serve(&symbols, &socket, &db);

        query_result(&socket, "CREATE TABLE t (a INTEGER, b VARCHAR)")
            .expect("CREATE TABLE should succeed");
        query_result(&socket, "INSERT INTO t VALUES (1, 'x'), (2, 'y')")
            .expect("INSERT should succeed");

        let batches = query_result(&socket, "SELECT a FROM t ORDER BY a").expect("SELECT");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2, "expected 2 rows in t, got {total}");

        // Streaming view still works (no ingest, so count is 0).
        let zero = query_result(&socket, "SELECT COUNT(*)::BIGINT FROM trades")
            .expect("SELECT count(*) FROM trades");
        let n = zero[0].column(0).as_any()
            .downcast_ref::<arrow_array::Int64Array>().unwrap().value(0);
        assert_eq!(n, 0, "trades stream should be empty in this test");

        // Guard: DDL on a reserved name is rejected.
        let err = query_result(&socket, "CREATE TABLE trades (x INT)").unwrap_err();
        assert!(
            err.contains("reserved") || err.contains("trades"),
            "expected reserved-name error, got: {err}"
        );

        // Guard: dropping a reserved view is also rejected.
        let err = query_result(&socket, "DROP TABLE quotes").unwrap_err();
        assert!(
            err.contains("reserved") || err.contains("quotes"),
            "expected reserved-name error, got: {err}"
        );

        rdb.kill();
    }

    // Some time for the OS to release the socket file before reusing it.
    std::thread::sleep(Duration::from_millis(200));
    let _ = std::fs::remove_file(&socket);

    // --- Round 2: restart against the same --db, row should still be there ---
    {
        let mut rdb = spawn_serve(&symbols, &socket, &db);

        let batches = query_result(&socket, "SELECT a, b FROM t ORDER BY a")
            .expect("SELECT after restart should succeed and find table t");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2, "user table did not persist across restart");

        rdb.kill();
    }
}
