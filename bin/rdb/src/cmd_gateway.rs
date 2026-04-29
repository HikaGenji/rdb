//! `rdb pg-gateway` — PostgreSQL wire-protocol gateway for the rdb Unix socket.

use std::collections::HashMap;
use std::fmt::Debug;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::Context;
use arrow_array::{Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
                  StringArray, UInt32Array, UInt64Array};
use arrow_schema::DataType;
use async_trait::async_trait;
use futures::{stream, Sink};
use pgwire::api::auth::md5pass::{hash_md5_password, Md5PasswordAuthStartupHandler};
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::auth::{
    AuthSource, DefaultServerParameterProvider, LoginInfo, Password,
};
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::{ClientInfo, PgWireHandlerFactory, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::tokio::process_socket;
use tokio::net::TcpListener;

use tp_arrow::decode_ipc_stream;
use tp_types::query_proto;

#[derive(clap::Args, Debug)]
pub struct Args {
    /// rdb Unix socket path.
    #[arg(long, default_value = "/tmp/rdb.sock")]
    socket: PathBuf,

    /// TCP address to listen on.
    #[arg(long, default_value = "0.0.0.0:5432")]
    listen: String,

    /// Optional TOML password file. Format: one `username = "password"` line
    /// per user (plaintext). When set, the gateway requires Postgres-style
    /// MD5 authentication; absent, the gateway accepts any client without
    /// credentials (the prior behaviour). The file is read once at startup.
    #[arg(long)]
    password_file: Option<PathBuf>,
}

pub fn run(args: Args) -> anyhow::Result<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("building tokio runtime")?;
    let users = match &args.password_file {
        Some(p) => {
            let s = std::fs::read_to_string(p)
                .with_context(|| format!("reading {}", p.display()))?;
            let map: HashMap<String, String> = toml::from_str(&s)
                .with_context(|| format!("parsing {}", p.display()))?;
            Some(map)
        }
        None => None,
    };
    rt.block_on(serve(args, users))
}

async fn serve(args: Args, users: Option<HashMap<String, String>>) -> anyhow::Result<()> {
    let listener = TcpListener::bind(&args.listen).await?;
    let auth_label = if users.is_some() { "md5" } else { "none" };
    eprintln!(
        "rdb-pg-gateway: {} → {} (auth: {})",
        args.listen, args.socket.display(), auth_label
    );

    if let Some(users) = users {
        let factory = Arc::new(Md5GatewayFactory {
            handler: Arc::new(RdbHandler::new(args.socket.clone())),
            auth_source: Arc::new(StaticAuthSource { users }),
            param_provider: Arc::new(DefaultServerParameterProvider::default()),
        });
        accept_loop(listener, factory).await
    } else {
        let factory = Arc::new(NoopGatewayFactory {
            handler: Arc::new(RdbHandler::new(args.socket.clone())),
        });
        accept_loop(listener, factory).await
    }
}

async fn accept_loop<F>(listener: TcpListener, factory: Arc<F>) -> anyhow::Result<()>
where
    F: PgWireHandlerFactory + Send + Sync + 'static,
{
    loop {
        let (socket, addr) = listener.accept().await?;
        let factory = factory.clone();
        tokio::spawn(async move {
            if let Err(e) = process_socket(socket, None, factory).await {
                eprintln!("connection {addr}: {e}");
            }
        });
    }
}

struct RdbHandler {
    socket_path: PathBuf,
}

impl RdbHandler {
    fn new(socket_path: PathBuf) -> Self {
        Self { socket_path }
    }
}

impl NoopStartupHandler for RdbHandler {}

/// Maps usernames to plaintext passwords (loaded from `--password-file`).
/// Per-connection, returns the salted MD5 hash that the client's reply
/// must equal. Username miss returns a deliberately wrong hash so the
/// authentication fails without leaking which usernames exist.
struct StaticAuthSource {
    users: HashMap<String, String>,
}

#[async_trait]
impl AuthSource for StaticAuthSource {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        let user = login.user().unwrap_or("");
        let plaintext = self.users.get(user).cloned().unwrap_or_default();
        // 4-byte salt seeded from the wall clock. Not cryptographically
        // strong; sufficient to make MD5 hashes per-connection unique.
        let nanos = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u32)
            .unwrap_or(0);
        let salt = nanos.to_le_bytes().to_vec();
        let hashed = hash_md5_password(user, &plaintext, &salt);
        Ok(Password::new(Some(salt), hashed.into_bytes()))
    }
}

fn arrow_to_pg(dt: &DataType) -> Type {
    match dt {
        DataType::Boolean => Type::BOOL,
        DataType::Int8 | DataType::Int16 => Type::INT2,
        DataType::Int32 => Type::INT4,
        DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Type::INT8,
        DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        _ => Type::TEXT,
    }
}

fn encode_value(encoder: &mut DataRowEncoder, col: &dyn Array, row: usize) -> PgWireResult<()> {
    if col.is_null(row) {
        return encoder.encode_field(&None::<i32>);
    }
    match col.data_type() {
        DataType::Boolean => encoder.encode_field(
            &col.as_any().downcast_ref::<BooleanArray>().unwrap().value(row),
        ),
        DataType::Int32 => encoder.encode_field(
            &col.as_any().downcast_ref::<Int32Array>().unwrap().value(row),
        ),
        DataType::Int64 => encoder.encode_field(
            &col.as_any().downcast_ref::<Int64Array>().unwrap().value(row),
        ),
        DataType::UInt32 => encoder.encode_field(
            &(col.as_any().downcast_ref::<UInt32Array>().unwrap().value(row) as i64),
        ),
        DataType::UInt64 => encoder.encode_field(
            &(col.as_any().downcast_ref::<UInt64Array>().unwrap().value(row) as i64),
        ),
        DataType::Float32 => encoder.encode_field(
            &col.as_any().downcast_ref::<Float32Array>().unwrap().value(row),
        ),
        DataType::Float64 => encoder.encode_field(
            &col.as_any().downcast_ref::<Float64Array>().unwrap().value(row),
        ),
        DataType::Utf8 => encoder.encode_field(
            &col.as_any().downcast_ref::<StringArray>().unwrap().value(row),
        ),
        dt => {
            let s = format!("(unhandled: {dt})");
            encoder.encode_field(&s.as_str())
        }
    }
}

#[async_trait]
impl SimpleQueryHandler for RdbHandler {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = query.trim().trim_end_matches(';').trim();

        if sql.is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }

        let verb = sql.split_ascii_whitespace().next().unwrap_or("").to_ascii_uppercase();
        match verb.as_str() {
            "SET" | "RESET" | "BEGIN" | "COMMIT" | "ROLLBACK" | "DEALLOCATE" => {
                return Ok(vec![Response::Execution(Tag::new(&verb))]);
            }
            _ => {}
        }

        let socket_path = self.socket_path.clone();
        let sql_owned = sql.to_string();

        let result = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            let mut stream = UnixStream::connect(&socket_path).context("connect to rdb socket")?;
            query_proto::write_request(&mut stream, &sql_owned).context("write request")?;
            let (status, payload) = query_proto::read_response(&mut stream).context("read response")?;
            match status {
                query_proto::STATUS_OK => decode_ipc_stream(&payload).context("decode Arrow IPC"),
                query_proto::STATUS_ERR => Err(anyhow::anyhow!("{}", String::from_utf8_lossy(&payload))),
                other => Err(anyhow::anyhow!("unknown rdb status byte {other}")),
            }
        })
        .await
        .map_err(|e| PgWireError::ApiError(e.into()))?;

        let batches = match result {
            Ok(b) => b,
            Err(e) => {
                return Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "XX000".to_owned(),
                    e.to_string(),
                )))]);
            }
        };

        if batches.is_empty() {
            return Ok(vec![Response::Execution(Tag::new("SELECT"))]);
        }

        let schema = batches[0].schema();
        let fields: Arc<Vec<FieldInfo>> = Arc::new(
            schema
                .fields()
                .iter()
                .map(|f| {
                    FieldInfo::new(
                        f.name().clone(),
                        None,
                        None,
                        arrow_to_pg(f.data_type()),
                        FieldFormat::Text,
                    )
                })
                .collect(),
        );

        let mut rows = Vec::new();
        for batch in &batches {
            for row_idx in 0..batch.num_rows() {
                let mut encoder = DataRowEncoder::new(fields.clone());
                for col_idx in 0..batch.num_columns() {
                    encode_value(&mut encoder, batch.column(col_idx).as_ref(), row_idx)?;
                }
                rows.push(encoder.finish()?);
            }
        }

        let response = QueryResponse::new(fields, stream::iter(rows.into_iter().map(Ok)));
        Ok(vec![Response::Query(response)])
    }
}

struct NoopGatewayFactory {
    handler: Arc<RdbHandler>,
}

impl PgWireHandlerFactory for NoopGatewayFactory {
    type StartupHandler = RdbHandler;
    type SimpleQueryHandler = RdbHandler;
    type ExtendedQueryHandler = PlaceholderExtendedQueryHandler;
    type CopyHandler = NoopCopyHandler;

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        self.handler.clone()
    }

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        Arc::new(PlaceholderExtendedQueryHandler)
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }
}

struct Md5GatewayFactory {
    handler: Arc<RdbHandler>,
    auth_source: Arc<StaticAuthSource>,
    param_provider: Arc<DefaultServerParameterProvider>,
}

impl PgWireHandlerFactory for Md5GatewayFactory {
    type StartupHandler =
        Md5PasswordAuthStartupHandler<StaticAuthSource, DefaultServerParameterProvider>;
    type SimpleQueryHandler = RdbHandler;
    type ExtendedQueryHandler = PlaceholderExtendedQueryHandler;
    type CopyHandler = NoopCopyHandler;

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        Arc::new(Md5PasswordAuthStartupHandler::new(
            self.auth_source.clone(),
            self.param_provider.clone(),
        ))
    }

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        Arc::new(PlaceholderExtendedQueryHandler)
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }
}
