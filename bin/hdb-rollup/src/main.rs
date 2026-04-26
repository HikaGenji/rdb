//! HDB daily rollup.
//!
//! Connects to a running `rdb` process via its Unix socket, fetches the
//! current in-memory `trades_live` and `quotes_live` tables as Arrow IPC,
//! and writes them as Parquet files under the target HDB directory with
//! two-level Hive partitioning:
//!
//! ```text
//! <hdb-dir>/
//!   trades/date=YYYY-MM-DD/symbol=BTC-PERP/data.parquet
//!   trades/date=YYYY-MM-DD/symbol=ETH-PERP/data.parquet
//!   quotes/date=YYYY-MM-DD/symbol=BTC-PERP/data.parquet
//!   quotes/date=YYYY-MM-DD/symbol=ETH-PERP/data.parquet
//!   …
//! ```
//!
//! The two-level Hive layout lets DuckDB skip whole date directories on date
//! predicates **and** skip individual symbol directories on symbol predicates.
//! The `symbol` value is encoded purely in the directory name; it is omitted
//! from the Parquet file itself to avoid a column-name conflict when DuckDB
//! reads with `hive_partitioning=true`. The `rdb` binary mounts history with
//! `read_parquet('…/**/*.parquet', hive_partitioning=true)` and selects
//! columns by name so `trades_hist` stays schema-identical to `trades_live`.
//!
//! Typical usage (run at midnight or end of trading session):
//!
//! ```sh
//! hdb-rollup --hdb-dir ./hdb
//! # or for a specific date:
//! hdb-rollup --hdb-dir ./hdb --date 2024-01-15
//! ```

use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use anyhow::Context;
use arrow::compute::concat_batches;
use arrow_array::RecordBatch;
use clap::Parser;
use duckdb::vtab::arrow::{arrow_recordbatch_to_query_params, ArrowVTab};
use duckdb::Connection;

use tp_arrow::decode_ipc_stream;
use tp_types::query_proto;

#[derive(Parser)]
#[command(name = "hdb-rollup", about = "Snapshot rdb live tables to daily Parquet files")]
struct Args {
    /// rdb Unix socket path.
    #[arg(long, default_value = "/tmp/rdb.sock")]
    socket: PathBuf,

    /// Root HDB directory. Parquet files are written under
    /// `<hdb-dir>/trades/` and `<hdb-dir>/quotes/`.
    #[arg(long, default_value = "./hdb")]
    hdb_dir: PathBuf,

    /// Date label for the output files (YYYY-MM-DD). Defaults to today in
    /// UTC. Supply an explicit value when rolling up a prior session.
    #[arg(long)]
    date: Option<String>,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let date = match args.date {
        Some(d) => d,
        None => today_utc(),
    };

    eprintln!("hdb-rollup: rolling up date={date} from {}", args.socket.display());

    let trades = fetch(&args.socket, "SELECT * FROM trades_live")
        .context("fetching trades_live")?;
    let quotes = fetch(&args.socket, "SELECT * FROM quotes_live")
        .context("fetching quotes_live")?;

    let trades_rows = total_rows(&trades);
    let quotes_rows = total_rows(&quotes);
    eprintln!("  trades_live: {trades_rows} rows");
    eprintln!("  quotes_live: {quotes_rows} rows");

    write_parquet(&trades, &args.hdb_dir.join("trades"), &date)
        .context("writing trades parquet")?;
    write_parquet(&quotes, &args.hdb_dir.join("quotes"), &date)
        .context("writing quotes parquet")?;

    eprintln!("hdb-rollup: done → {}", args.hdb_dir.display());
    Ok(())
}

// ---------------------------------------------------------------------------
// RDB query helper
// ---------------------------------------------------------------------------

fn fetch(socket: &Path, sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
    let mut stream =
        UnixStream::connect(socket).with_context(|| format!("connecting to {}", socket.display()))?;
    query_proto::write_request(&mut stream, sql)?;
    let (status, payload) = query_proto::read_response(&mut stream)?;
    match status {
        query_proto::STATUS_OK => decode_ipc_stream(&payload).context("decode Arrow IPC"),
        query_proto::STATUS_ERR => Err(anyhow::anyhow!("{}", String::from_utf8_lossy(&payload))),
        other => Err(anyhow::anyhow!("unknown rdb status byte {other}")),
    }
}

// ---------------------------------------------------------------------------
// Parquet writer
// ---------------------------------------------------------------------------

/// Write `batches` to per-symbol Parquet files under
/// `<dir>/date=<date>/symbol=<sym>/data.parquet`.
///
/// Each file omits the `symbol` column; it is encoded in the directory name
/// so DuckDB can apply file-skip on both the `date` and `symbol` axes when
/// reading with `hive_partitioning=true`.
fn write_parquet(batches: &[RecordBatch], dir: &Path, date: &str) -> anyhow::Result<()> {
    if batches.is_empty() || total_rows(batches) == 0 {
        eprintln!("  skipping empty table for {}", dir.display());
        return Ok(());
    }

    let schema = batches[0].schema();
    let merged = concat_batches(&schema, batches).context("concat batches")?;

    let conn = Connection::open_in_memory()?;
    conn.register_table_function::<ArrowVTab>("arrow")?;

    let params = arrow_recordbatch_to_query_params(merged);
    let mut stmt = conn.prepare("CREATE TEMP TABLE _data AS SELECT * FROM arrow(?, ?)")?;
    stmt.execute(params)?;

    let mut sym_stmt = conn.prepare("SELECT DISTINCT symbol FROM _data ORDER BY symbol")?;
    let symbols: Vec<String> = sym_stmt
        .query_map([], |row| row.get(0))?
        .collect::<Result<_, _>>()?;

    for sym in &symbols {
        // Hive layout: date=YYYY-MM-DD/symbol=<sym>/data.parquet
        let sym_dir = dir.join(format!("date={date}")).join(format!("symbol={sym}"));
        std::fs::create_dir_all(&sym_dir)
            .with_context(|| format!("creating {}", sym_dir.display()))?;
        let out_path = sym_dir.join("data.parquet");

        // Exclude `symbol` from the file; it is already encoded in the path.
        let copy_sql = format!(
            "COPY (SELECT * EXCLUDE (symbol) FROM _data WHERE symbol = {}) \
             TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD)",
            sql_quote(sym),
            out_path.display()
        );
        conn.execute_batch(&copy_sql)
            .with_context(|| format!("writing {}", out_path.display()))?;
        eprintln!("  wrote {}", out_path.display());
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Wrap `s` in single quotes, escaping any embedded single quotes.
fn sql_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn today_utc() -> String {
    // Use std::time to get today's date without pulling in a chrono dependency.
    // Seconds since Unix epoch → days → YYYY-MM-DD.
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let days = secs / 86400;
    // Tomohiko Sakamoto's algorithm for Gregorian calendar.
    let z = days + 719468;
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
