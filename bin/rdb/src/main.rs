//! Single-binary entry point for the rdb stack.
//!
//! All seven roles are bundled into one executable; pick a role with the
//! first positional argument:
//!
//! ```text
//! rdb serve         --symbols config/symbols.toml
//! rdb tickerplant   --symbols config/symbols.toml --wal-dir /tmp/rdb-wal
//! rdb feed-replayer --symbols config/symbols.toml --input samples/sample.jsonl
//! rdb hdb-rollup    --hdb-dir ./hdb
//! rdb pg-gateway    --listen 0.0.0.0:5432
//! rdb query         --sql "SELECT COUNT(*) FROM trades"
//! rdb zenoh-bridge  --mode outbound
//! ```
//!
//! Sharing a single binary lets the link step deduplicate every dependency
//! (DuckDB, Arrow, tokio, zenoh, …) so the on-disk image is markedly
//! smaller than the sum of the previous prototype binaries.

mod cmd_bridge;
mod cmd_gateway;
mod cmd_query;
mod cmd_replay;
mod cmd_rollup;
mod cmd_serve;
mod cmd_tickerplant;
mod tracing_init;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "rdb",
    version,
    about = "Single-binary rdb market-data stack",
    propagate_version = true
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the in-memory rdb (subscribes to */agg, serves SQL on a Unix socket).
    Serve(cmd_serve::Args),
    /// Run the tickerplant (subscribes to */raw, persists WAL, republishes */agg).
    Tickerplant(cmd_tickerplant::Args),
    /// Replay a JSONL feed to */raw.
    #[command(name = "feed-replayer", alias = "replay")]
    FeedReplayer(cmd_replay::Args),
    /// Snapshot live tables to dated Parquet files.
    #[command(name = "hdb-rollup", alias = "rollup")]
    HdbRollup(cmd_rollup::Args),
    /// PostgreSQL wire-protocol gateway.
    #[command(name = "pg-gateway", alias = "gateway")]
    PgGateway(cmd_gateway::Args),
    /// Send one SQL query and pretty-print the result.
    Query(cmd_query::Args),
    /// Cross-host iceoryx2 ↔ zenoh relay.
    #[command(name = "zenoh-bridge", alias = "bridge")]
    ZenohBridge(cmd_bridge::Args),
}

fn main() -> anyhow::Result<()> {
    tracing_init::init();
    match Cli::parse().cmd {
        Cmd::Serve(a)        => cmd_serve::run(a),
        Cmd::Tickerplant(a)  => cmd_tickerplant::run(a),
        Cmd::FeedReplayer(a) => cmd_replay::run(a),
        Cmd::HdbRollup(a)    => cmd_rollup::run(a),
        Cmd::PgGateway(a)    => cmd_gateway::run(a),
        Cmd::Query(a)        => cmd_query::run(a),
        Cmd::ZenohBridge(a)  => cmd_bridge::run(a),
    }
}
