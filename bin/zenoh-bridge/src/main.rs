//! Cross-host fan-out bridge: iceoryx2 ↔ zenoh.
//!
//! Same-host paths continue using iceoryx2 shared memory unchanged.
//! This binary sits at the host boundary and forwards the `*/agg` topics
//! (produced by the tickerplant, consumed by rdb) over a zenoh session so
//! remote rdb instances can participate.
//!
//! ```text
//! # On the tickerplant host:
//! zenoh-bridge --mode outbound
//!
//! # On each remote rdb host:
//! zenoh-bridge --mode inbound
//! ```

use std::mem::size_of;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use bytemuck::{bytes_of, pod_read_unaligned};
use clap::{Parser, ValueEnum};
use iceoryx2::prelude::*;
use tracing::{info, warn};
use zenoh::sample::Sample;

use tp_types::{ipc_cfg, topics, QuoteL1, Trade};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Mode {
    /// Subscribe to rdb/trades/agg and rdb/quotes/agg on iceoryx2,
    /// republish over zenoh. Run on the tickerplant host.
    Outbound,
    /// Subscribe to rdb/trades/agg and rdb/quotes/agg on zenoh,
    /// inject into iceoryx2. Run on each remote rdb host.
    Inbound,
}

#[derive(Parser, Debug)]
#[command(name = "zenoh-bridge", about = "Cross-host iceoryx2 ↔ zenoh relay")]
struct Args {
    /// Bridge direction.
    #[arg(long)]
    mode: Mode,

    /// Zenoh configuration file (JSON5/YAML).
    /// Without this, zenoh uses multicast scouting — works on a LAN.
    /// Supply a router config for WAN or firewalled deployments.
    #[arg(long)]
    zenoh_config: Option<PathBuf>,

    /// Polling idle sleep in microseconds.
    #[arg(long, default_value_t = 200)]
    idle_sleep_us: u64,
}

fn ze<E: std::fmt::Display>(e: E) -> anyhow::Error {
    anyhow::anyhow!("{e}")
}

fn main() -> anyhow::Result<()> {
    init_tracing();
    let args = Args::parse();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("building tokio runtime")?;
    match args.mode {
        Mode::Outbound => run_outbound(args, &rt),
        Mode::Inbound => run_inbound(args, &rt),
    }
}

// ---------------------------------------------------------------------------
// Outbound: iceoryx2 → zenoh
// ---------------------------------------------------------------------------

fn run_outbound(args: Args, rt: &tokio::runtime::Runtime) -> anyhow::Result<()> {
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

    let trade_iox_sub = trades_svc.subscriber_builder().create()?;
    let quote_iox_sub = quotes_svc.subscriber_builder().create()?;

    let zconfig = build_zenoh_config(&args.zenoh_config)?;
    let session = rt
        .block_on(async { zenoh::open(zconfig).await })
        .map_err(|e| anyhow::anyhow!("opening zenoh session: {e}"))?;
    let trade_zen_pub = rt
        .block_on(async { session.declare_publisher(topics::TRADES_AGG).await })
        .map_err(|e| anyhow::anyhow!("declaring zenoh trades publisher: {e}"))?;
    let quote_zen_pub = rt
        .block_on(async { session.declare_publisher(topics::QUOTES_AGG).await })
        .map_err(|e| anyhow::anyhow!("declaring zenoh quotes publisher: {e}"))?;

    info!("zenoh-bridge outbound: forwarding iceoryx2 agg → zenoh");

    let idle = Duration::from_micros(args.idle_sleep_us);
    loop {
        let mut did_work = false;

        while let Some(sample) = trade_iox_sub.receive()? {
            let trade: Trade = *sample;
            let bytes: Vec<u8> = bytes_of(&trade).to_vec();
            rt.block_on(async { trade_zen_pub.put(bytes).await })
                .map_err(ze)?;
            did_work = true;
        }
        while let Some(sample) = quote_iox_sub.receive()? {
            let quote: QuoteL1 = *sample;
            let bytes: Vec<u8> = bytes_of(&quote).to_vec();
            rt.block_on(async { quote_zen_pub.put(bytes).await })
                .map_err(ze)?;
            did_work = true;
        }

        if !did_work {
            std::thread::sleep(idle);
        }
    }
}

// ---------------------------------------------------------------------------
// Inbound: zenoh → iceoryx2
// ---------------------------------------------------------------------------

fn run_inbound(args: Args, rt: &tokio::runtime::Runtime) -> anyhow::Result<()> {
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

    let iox_trade_pub = trades_svc.publisher_builder().create()?;
    let iox_quote_pub = quotes_svc.publisher_builder().create()?;

    let zconfig = build_zenoh_config(&args.zenoh_config)?;
    let session = rt
        .block_on(async { zenoh::open(zconfig).await })
        .map_err(|e| anyhow::anyhow!("opening zenoh session: {e}"))?;

    // Declare zenoh subscribers with callbacks that push raw bytes onto
    // bounded crossbeam channels. The callbacks fire on zenoh's internal
    // threads; the main loop drains the channels and publishes to iceoryx2.
    let (trade_bytes_tx, trade_bytes_rx) =
        crossbeam_channel::bounded::<Vec<u8>>(ipc_cfg::SUBSCRIBER_MAX_BUFFER_SIZE);
    let (quote_bytes_tx, quote_bytes_rx) =
        crossbeam_channel::bounded::<Vec<u8>>(ipc_cfg::SUBSCRIBER_MAX_BUFFER_SIZE);

    let _trade_zen_sub = rt
        .block_on(async {
            session
                .declare_subscriber(topics::TRADES_AGG)
                .callback({
                    let tx = trade_bytes_tx;
                    move |sample: Sample| {
                        let bytes: Vec<u8> = sample.payload().to_bytes().to_vec();
                        if bytes.len() == size_of::<Trade>() {
                            let _ = tx.try_send(bytes);
                        } else {
                            warn!(len = bytes.len(), "trade: unexpected payload size, dropping");
                        }
                    }
                })
                .await
        })
        .map_err(|e| anyhow::anyhow!("declaring zenoh trades subscriber: {e}"))?;

    let _quote_zen_sub = rt
        .block_on(async {
            session
                .declare_subscriber(topics::QUOTES_AGG)
                .callback({
                    let tx = quote_bytes_tx;
                    move |sample: Sample| {
                        let bytes: Vec<u8> = sample.payload().to_bytes().to_vec();
                        if bytes.len() == size_of::<QuoteL1>() {
                            let _ = tx.try_send(bytes);
                        } else {
                            warn!(len = bytes.len(), "quote: unexpected payload size, dropping");
                        }
                    }
                })
                .await
        })
        .map_err(|e| anyhow::anyhow!("declaring zenoh quotes subscriber: {e}"))?;

    info!("zenoh-bridge inbound: forwarding zenoh → iceoryx2 agg");

    let idle = Duration::from_micros(args.idle_sleep_us);
    loop {
        let mut did_work = false;

        while let Ok(bytes) = trade_bytes_rx.try_recv() {
            let trade: Trade = pod_read_unaligned(&bytes);
            iox_trade_pub.loan_uninit()?.write_payload(trade).send()?;
            did_work = true;
        }
        while let Ok(bytes) = quote_bytes_rx.try_recv() {
            let quote: QuoteL1 = pod_read_unaligned(&bytes);
            iox_quote_pub.loan_uninit()?.write_payload(quote).send()?;
            did_work = true;
        }

        if !did_work {
            std::thread::sleep(idle);
        }
    }
    // _trade_zen_sub and _quote_zen_sub keep subscriptions alive until here.
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn build_zenoh_config(path: &Option<PathBuf>) -> anyhow::Result<zenoh::Config> {
    match path {
        Some(p) => zenoh::Config::from_file(p)
            .map_err(|e| anyhow::anyhow!("loading zenoh config {}: {e}", p.display())),
        None => Ok(zenoh::Config::default()),
    }
}

fn init_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = fmt()
        .json()
        .with_writer(std::io::stderr)
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}
