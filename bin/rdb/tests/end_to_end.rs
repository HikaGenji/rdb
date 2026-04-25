//! End-to-end integration test.
//!
//! Spawns the four binaries (`feed-replayer`, `tickerplant`, `rdb`,
//! `rdb-query`), drives a sample JSONL through them, and verifies:
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
use std::time::{Duration, Instant};

use arrow_array::{Float64Array, RecordBatch, StringArray, UInt64Array};
use tp_arrow::decode_ipc_stream;

const REQ_OK: u8 = 0;
const REQ_ERR: u8 = 1;

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

fn sibling_binary(name: &str) -> PathBuf {
    rdb_binary().parent().unwrap().join(name)
}

/// Build all workspace binaries so the sibling executables exist.
fn ensure_binaries_built() {
    let status = Command::new(env!("CARGO"))
        .args(["build", "--workspace", "--bins", "--quiet"])
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
    ensure_binaries_built();

    let scratch = ScratchDir::new("e2e");
    let symbols = workspace_root().join("config/symbols.toml");
    let sample  = workspace_root().join("samples/sample.jsonl");
    assert!(symbols.exists(), "config/symbols.toml not found");
    assert!(sample.exists(), "samples/sample.jsonl not found");

    let wal_dir = scratch.path.join("wal");
    let socket  = scratch.path.join("rdb.sock");

    // iceoryx2 0.7 has no env-based config: shared-memory segments live
    // under /tmp/iceoryx2/. Clean it before the run to avoid picking up
    // segments from a previous aborted run with mismatched type sizes.
    let _ = std::fs::remove_dir_all("/tmp/iceoryx2");

    // Start the rdb first so it is ready to receive aggregated records.
    let rdb_path = rdb_binary();
    let mut rdb = Command::new(&rdb_path);
    rdb.arg("--symbols").arg(&symbols)
       .arg("--socket").arg(&socket)
       .arg("--idle-exit-secs").arg("3")
       .env("RUST_LOG", "warn");
    let mut rdb = ChildGuard::new(rdb.spawn().expect("spawn rdb"));

    assert!(
        wait_for_socket(&socket, Duration::from_secs(5)),
        "rdb socket did not appear at {}",
        socket.display()
    );

    // Tickerplant.
    let tp_path = sibling_binary("tickerplant");
    let mut tp = Command::new(&tp_path);
    tp.arg("--symbols").arg(&symbols)
      .arg("--wal-dir").arg(&wal_dir)
      .arg("--idle-exit-secs").arg("2")
      .env("RUST_LOG", "warn");
    let tp = ChildGuard::new(tp.spawn().expect("spawn tickerplant"));

    // Give the tickerplant a moment to attach its subscribers before the
    // replayer starts publishing — iceoryx2 subscribers only see samples
    // sent after they connect.
    std::thread::sleep(Duration::from_millis(500));

    // Feed replayer (firehose mode, full sample file).
    let fr_path = sibling_binary("feed-replayer");
    let mut fr = Command::new(&fr_path);
    fr.arg("--symbols").arg(&symbols)
      .arg("--input").arg(&sample)
      .arg("--pace").arg("firehose")
      .env("RUST_LOG", "warn");
    let fr = ChildGuard::new(fr.spawn().expect("spawn feed-replayer"));

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

    // Tear down rdb. Ingest will idle out after 3s; we can also kill it.
    rdb.kill();
}
