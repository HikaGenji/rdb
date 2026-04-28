//! `rdb query` — send one SQL query (or SUBSCRIBE) and pretty-print results.

use std::io::Read;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;

use anyhow::Context;
use arrow::util::pretty::pretty_format_batches;

use tp_arrow::decode_ipc_stream;
use tp_types::query_proto;

#[derive(clap::Args, Debug)]
pub struct Args {
    /// Unix socket path of the rdb.
    #[arg(long, default_value = "/tmp/rdb.sock")]
    socket: PathBuf,

    /// SQL to run. Use `--from-stdin` to read from stdin instead. For
    /// streaming, pass `SUBSCRIBE trades` (or `quotes`); the client will
    /// keep printing batches until SIGINT.
    #[arg(long, conflicts_with = "from_stdin")]
    sql: Option<String>,

    /// Read SQL from stdin until EOF.
    #[arg(long, default_value_t = false)]
    from_stdin: bool,
}

pub fn run(args: Args) -> anyhow::Result<()> {
    let sql = match (args.sql, args.from_stdin) {
        (Some(s), false) => s,
        (None, true) => {
            let mut s = String::new();
            std::io::stdin().read_to_string(&mut s)?;
            s
        }
        (None, false) => anyhow::bail!("provide --sql or --from-stdin"),
        (Some(_), true) => unreachable!(),
    };

    let mut stream = UnixStream::connect(&args.socket)
        .with_context(|| format!("connecting to {}", args.socket.display()))?;
    query_proto::write_request(&mut stream, &sql)?;

    if query_proto::looks_like_subscribe(&sql) {
        return run_subscribe_client(&mut stream);
    }

    let (status, payload) = query_proto::read_response(&mut stream)?;
    match status {
        query_proto::STATUS_OK => {
            let batches = decode_ipc_stream(&payload)?;
            if batches.is_empty() {
                println!("(no rows)");
            } else {
                let formatted = pretty_format_batches(&batches)?;
                println!("{formatted}");
            }
            Ok(())
        }
        query_proto::STATUS_ERR => {
            let msg = String::from_utf8_lossy(&payload);
            eprintln!("error: {msg}");
            std::process::exit(1);
        }
        other => anyhow::bail!("unknown response status {other}"),
    }
}

/// Read frames until the server disconnects or sends ERR. The first frame
/// is the OK ack; subsequent frames carry STATUS_BATCH with Arrow IPC.
fn run_subscribe_client(stream: &mut UnixStream) -> anyhow::Result<()> {
    let (status, payload) = query_proto::read_response(&mut *stream)?;
    match status {
        query_proto::STATUS_OK => {
            eprintln!("subscribed; press Ctrl-C to stop");
            if !payload.is_empty() {
                // Defensive: server should send empty payload on ack.
                let _ = decode_ipc_stream(&payload);
            }
        }
        query_proto::STATUS_ERR => {
            let msg = String::from_utf8_lossy(&payload);
            eprintln!("subscribe rejected: {msg}");
            std::process::exit(1);
        }
        other => anyhow::bail!("unexpected subscribe ack status {other}"),
    }

    loop {
        let (status, payload) = match query_proto::read_response(&mut *stream) {
            Ok(v) => v,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        match status {
            query_proto::STATUS_BATCH => {
                let batches = decode_ipc_stream(&payload)?;
                if batches.is_empty() { continue; }
                let formatted = pretty_format_batches(&batches)?;
                println!("{formatted}");
            }
            query_proto::STATUS_ERR => {
                let msg = String::from_utf8_lossy(&payload);
                eprintln!("error: {msg}");
                std::process::exit(1);
            }
            other => anyhow::bail!("unexpected stream status {other}"),
        }
    }
}
