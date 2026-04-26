# rdb — Rust market data stack (prototype)

A kdb-style market data platform for crypto perpetuals, in Rust.
The stack covers the full pipeline from raw feed to long-term queryable
history, with a standard SQL interface usable from any BI tool:

Single-host:
```
feed-replayer ─► tickerplant ─► rdb (in-memory) ─►─┐
                     │                               │ pg-gateway (Postgres wire)
                     └─► WAL (mmap)           hdb-rollup ─► Parquet files
```

Multi-host (via `zenoh-bridge`):
```
Host A  feed-replayer ─► tickerplant ─► rdb/*/agg ─► zenoh-bridge --mode outbound
                                                              │
                                                        zenoh (UDP/TCP)
                                                              │
Host B                          zenoh-bridge --mode inbound ─► rdb/*/agg ─► rdb
```

Same-host transport is zero-copy shared memory ([iceoryx2]).
Queries are served by DuckDB, operating on Arrow columnar buffers for
live data and on Parquet files for historical data. The two are
transparently unioned so clients see a single `trades` / `quotes` table
spanning any time range.

This is a prototype to validate the architecture. See the
[Scalability](#scalability) section for an honest account of ceilings
and known bottlenecks.

[iceoryx2]: https://crates.io/crates/iceoryx2

## Layout

The whole stack ships as a **single `rdb` binary** with one subcommand per
role. Sharing one binary lets the linker deduplicate every heavy
dependency (DuckDB, Arrow, tokio, zenoh, …) so the on-disk image is
markedly smaller than the sum of seven separate executables.

```
crates/
  tp-types       Schemas (Trade, QuoteL1), iceoryx2 service tuning,
                 wire protocol for the SQL Unix socket, latency histogram.
  tp-config      Symbol TOML loader + fixed-point encode/decode helpers.
  tp-wal         Append-only mmap-backed WAL writer/reader.
  tp-arrow       Per-symbol record stores and Arrow snapshot/IPC helpers.

bin/rdb/         Single binary. Subcommands:
  rdb serve         Subscribe to */agg, append to per-symbol Arrow buffers,
                    serve SQL on a Unix domain socket.
  rdb tickerplant   Subscribe to */raw, assign sequence numbers, persist
                    to mmap'd WAL, republish on */agg.
  rdb feed-replayer Read JSONL feed events, publish to rdb/trades/raw and
                    rdb/quotes/raw. Firehose or wall-clock pacing.
  rdb hdb-rollup    Snapshot the live tables to dated Parquet files at
                    end of day. Run once per session (e.g. via cron).
  rdb query         Send one SQL query over the Unix socket, print the
                    result as a pretty Arrow table.
  rdb pg-gateway    PostgreSQL wire-protocol proxy for the Unix socket so
                    standard SQL clients (DBeaver, DataGrip, psql, …) can
                    connect without any plugin.
  rdb zenoh-bridge  Cross-host relay. Outbound mode subscribes to */agg
                    on iceoryx2 and republishes over zenoh; inbound mode
                    does the reverse.
```

Run `rdb --help` for the subcommand list, `rdb <subcommand> --help` for
flags. The release profile is tuned for size: fat LTO, single codegen
unit, `panic=abort`, stripped symbols.

## Wire schemas

Both records are `repr(C)` POD with explicit padding so they can be
shared zero-copy across processes:

- `Trade` — 56 bytes: seq, ts_exchange_ns, ts_local_ns, symbol_id,
  price (fixed-point i64), qty (fixed-point i64), side.
- `QuoteL1` — 64 bytes: seq, ts_exchange_ns, ts_local_ns, symbol_id,
  bid_price/qty, ask_price/qty.

Symbol ids are assigned at startup from `config/symbols.toml` (the order
in the file is the id). The `price` and `qty` columns are stored as
fixed-point integers; the per-symbol scale is in the TOML.

## SQL surface

The rdb exposes three table tiers for each dataset:

```text
trades_live   today's in-memory rows (always present)
trades_hist   read_parquet('<hdb-dir>/trades/**/*.parquet',
              hive_partitioning=true) — date=…/symbol=… resolved from path
              (present when --hdb is set and at least one file exists)
trades        UNION ALL of trades_live + trades_hist
              (plain alias for trades_live when --hdb is not set)

quotes_live / quotes_hist / quotes — same pattern
```

Column schemas:

```text
trades(symbol Utf8, symbol_id u32, seq u64,
       ts_exchange_ns u64, ts_local_ns u64,
       price f64, qty f64, side Utf8)

quotes(symbol Utf8, symbol_id u32, seq u64,
       ts_exchange_ns u64, ts_local_ns u64,
       bid_price f64, bid_qty f64, ask_price f64, ask_qty f64)
```

Queries can use the full DuckDB SQL surface, including `ASOF JOIN` and
Parquet predicate-pushdown when filtering on timestamp columns.

## Historical database (HDB)

At the end of each trading session, run `rdb hdb-rollup` to snapshot the
live tables to Parquet:

```bash
# Roll up today (UTC date inferred automatically).
./target/release/rdb hdb-rollup --hdb-dir ./hdb

# Or supply an explicit date for backfills.
./target/release/rdb hdb-rollup --hdb-dir ./hdb --date 2024-01-15
```

This writes two-level Hive-partitioned files:

```
hdb/
  trades/date=2024-01-15/symbol=BTC-PERP/data.parquet
  trades/date=2024-01-15/symbol=ETH-PERP/data.parquet
  trades/date=2024-01-16/symbol=BTC-PERP/data.parquet
  quotes/date=2024-01-15/symbol=BTC-PERP/data.parquet
  …
```

Each file is ZSTD-compressed. The `date=…/symbol=…` directory names are Hive
partition keys; DuckDB uses them to skip whole directories when a query carries
a date or symbol predicate (file-skip on both axes). The `symbol` value is
encoded only in the path — it is omitted from the Parquet file itself to avoid
a column-name conflict with the Hive partition key.

To make the rdb serve both live and historical data, pass `--hdb`:

```bash
./target/release/rdb serve --symbols config/symbols.toml --hdb ./hdb
```

Clients that only ever queried `trades` or `quotes` see no change — they
now transparently get rows from all historical Parquet files plus today's
in-memory data. To query only live data use `trades_live`; to query
only history use `trades_hist`.

Example multi-day query:

```sql
SELECT symbol,
       date_trunc('day', to_timestamp(ts_exchange_ns / 1e9)) AS day,
       COUNT(*)                                               AS n,
       AVG(price)                                            AS avg_px
FROM   trades
GROUP  BY symbol, day
ORDER  BY symbol, day;
```

A typical cron entry for midnight UTC rollup:

```cron
0 0 * * * /path/to/rdb hdb-rollup --hdb-dir /data/hdb >> /var/log/hdb-rollup.log 2>&1
```

## PostgreSQL gateway

Any client that speaks the Postgres protocol can connect directly:

```bash
# Start the gateway (defaults: 0.0.0.0:5432, /tmp/rdb.sock).
./target/release/rdb pg-gateway

# Or with custom endpoints.
./target/release/rdb pg-gateway --listen 127.0.0.1:5433 --socket /tmp/rdb.sock
```

Then connect from any SQL tool — no plugin required:

| Tool | Connection string |
|---|---|
| psql | `psql -h localhost -p 5432 -d any` |
| DBeaver / DataGrip | Host `localhost`, port `5432`, driver PostgreSQL, no password |
| Metabase | PostgreSQL data source, same host/port |
| SQLPad | PostgreSQL, same host/port |

The gateway forwards SQL verbatim to the rdb Unix socket, converts the
Arrow IPC response to Postgres wire-format rows, and maps Arrow types to
their closest Postgres equivalents (Float64 → FLOAT8, UInt64 → INT8,
Utf8 → TEXT, …). Session-level commands sent by tools on connect (`SET`,
`BEGIN`, `RESET`, …) are acknowledged without being forwarded.

## Latency hops

Each record carries `ts_local_ns` (wall-clock ns assigned at the feed
handler). Each downstream stage records `now() - ts_local_ns` into a
sliding-window histogram and emits `p50_ns`, `p99_ns` once a second on
stderr (JSON via `tracing-subscriber`). Drop counters are kept but the
prototype only logs them; backpressure beyond that is out of scope.

## Scalability

### What scales well

| Dimension | Assessment |
|---|---|
| **Ingest throughput** | iceoryx2 shared memory is zero-copy and sub-µs; millions of events/sec is realistic on a single host. The tickerplant and rdb ingest paths are decoupled — queries never stall ingestion beyond a brief per-symbol snapshot lock. |
| **HDB query analytics** | DuckDB on Parquet is a genuine OLAP engine. Multi-year history with billions of rows is queryable in seconds on a single machine, especially with column pruning and timestamp predicate pushdown within row groups. |
| **Operational simplicity** | No external dependencies (no Kafka, no Postgres, no object store). The full pipeline runs in five processes on one machine. |

### Known ceilings and bottlenecks

**Single host — hard limit.**
iceoryx2 uses OS shared memory; it cannot span network boundaries.
Every process in the pipeline must run on the same machine. Horizontal
scaling requires replacing the transport layer with something
network-capable (Aeron, Chronicle, NATS, …).

**Fixed symbol set.**
Symbols are loaded from TOML at startup and assigned fixed integer ids.
Adding a symbol requires restarting the rdb (and losing in-memory data).

**pg-gateway: one round-trip per query.**
The gateway opens a new Unix socket connection for every query and waits
for the full response before returning. There is no pipelining or
connection multiplexing to the rdb. This is fine for interactive BI
tools; it is unsuitable for high-frequency programmatic querying.

### Addressed bottlenecks

**Per-query DuckDB spin-up → connection pool.**
The rdb now maintains a pool of `--query-workers` (default 4) persistent
DuckDB connections. Each connection is initialised once (open +
ArrowVTab registration). Per query, only the live temp tables are
dropped and recreated from a fresh Arrow snapshot; HDB Parquet metadata
stays cached in the connection across queries. Concurrent queries beyond
the pool size queue rather than spin up new connections.

**Unbounded RDB memory → O(1) row-cap eviction.**
`--row-cap N` (default 0 = unlimited) sets a per-symbol row limit.
Column stores use `VecDeque<T>` so evicting the oldest row on each push
that exceeds the cap is O(1). At cap=500 000 rows per symbol, a
3-symbol deployment keeps ≈ 200 MB in RAM regardless of session length.

**HDB glob reads all files → two-level Hive partitioning.**
`hdb-rollup` writes `<hdb>/trades/date=YYYY-MM-DD/symbol=<sym>/data.parquet`.
The rdb mounts history with `read_parquet('…/**/*.parquet',
hive_partitioning=true)`. DuckDB resolves `date` and `symbol` from the
directory names, enabling file-skip on both axes. `trades_hist` selects
columns by name (not `SELECT *`) so the schema is stable regardless of
Parquet column order and stays union-compatible with `trades_live`.

**Row-cap eviction as only flush path → intra-day rollup.**
`--rollup-interval-secs N` (requires `--hdb`) spawns a background thread
that every N seconds snapshots and clears the in-memory stores, writing
per-symbol chunks to
`<hdb>/<table>/date=YYYY-MM-DD/symbol=<sym>/<unix_ts>.parquet`
using the same two-level Hive layout. With a 5-minute interval the stores
never grow beyond one interval's worth of rows; `--row-cap` becomes a
last-resort safety net rather than the primary eviction mechanism.

**Single-host iceoryx2 → cross-host zenoh bridge.**
`zenoh-bridge` forwards the tickerplant's `*/agg` output to remote rdb
instances over zenoh (UDP/TCP multicast or router) without modifying any
existing binary. Same-host paths stay on iceoryx2 shared memory. See
[Multi-host deployment](#multi-host-deployment-zenoh-bridge) below.

### Remaining scaling path

## Running it

```bash
cargo build --release      # produces a single ./target/release/rdb

# Terminal 1 — start the in-memory server (HDB, 4-worker pool, 500k row cap,
# 5-min intra-day rollup).
./target/release/rdb serve --symbols config/symbols.toml --hdb ./hdb \
    --query-workers 4 --row-cap 500000 --rollup-interval-secs 300

# Terminal 2 — start the tickerplant.
./target/release/rdb tickerplant --symbols config/symbols.toml \
    --wal-dir /tmp/rdb-wal

# Terminal 3 — replay the sample feed.
./target/release/rdb feed-replayer --symbols config/symbols.toml \
    --input samples/sample.jsonl

# Terminal 4 — query via CLI.
./target/release/rdb query --sql "
    SELECT symbol, COUNT(*) AS n, AVG(price) AS avg_px
    FROM trades
    GROUP BY symbol
    ORDER BY symbol"

# Terminal 4 (alternative) — start the Postgres gateway and use psql.
./target/release/rdb pg-gateway
psql -h localhost -p 5432 -d rdb -c "SELECT symbol, COUNT(*) FROM trades GROUP BY 1"

# At end of day — snapshot live tables to Parquet.
./target/release/rdb hdb-rollup --hdb-dir ./hdb
```

## Multi-host deployment (zenoh-bridge)

On the **tickerplant host**, start the outbound bridge after the tickerplant:

```bash
# LAN — zenoh uses multicast scouting; no config needed.
./target/release/rdb zenoh-bridge --mode outbound

# WAN / router-based — supply a zenoh config pointing at a router.
./target/release/rdb zenoh-bridge --mode outbound --zenoh-config zenoh-router.json5
```

On each **remote rdb host**, start the inbound bridge before the server:

```bash
./target/release/rdb zenoh-bridge --mode inbound
./target/release/rdb serve --symbols config/symbols.toml
```

The inbound bridge injects arriving `agg` messages into the local iceoryx2
bus; the rdb subscribes to them exactly as if a local tickerplant were running.
The same-host pipeline (feed-replayer, tickerplant, local rdb) is entirely
unaffected.

For zenoh router configuration and cross-subnet deployments, see the
[zenoh documentation](https://zenoh.io/docs/getting-started/deployment/).

## Tests

```bash
cargo test --workspace --lib            # unit tests
cargo test -p rdb --test end_to_end     # drives `rdb serve`,
                                        # `rdb tickerplant`, and
                                        # `rdb feed-replayer` end-to-end.
```

The integration test cleans `/tmp/iceoryx2/` before it runs because
iceoryx2 0.7 has no env-based config root override; concurrent runs of
the integration test on the same machine will collide.

## Out of scope

Real exchange WebSockets, multi-host distribution, auth/TLS, schema
evolution, signal engine, principled backpressure — all deferred.
