# rdb — Rust market data stack (prototype)

A kdb-style market data platform for crypto perpetuals, in Rust.
The stack covers the full pipeline from raw feed to long-term queryable
history, with a standard SQL interface usable from any BI tool:

```
feed-replayer ─► tickerplant ─► rdb (in-memory) ─►─┐
                     │                               │ pg-gateway (Postgres wire)
                     └─► WAL (mmap)           hdb-rollup ─► Parquet files
```

Transport between processes is zero-copy shared memory ([iceoryx2]).
Queries are served by DuckDB, operating on Arrow columnar buffers for
live data and on Parquet files for historical data. The two are
transparently unioned so clients see a single `trades` / `quotes` table
spanning any time range.

This is a prototype to validate the architecture. See the
[Scalability](#scalability) section for an honest account of ceilings
and known bottlenecks.

[iceoryx2]: https://crates.io/crates/iceoryx2

## Layout

```
crates/
  tp-types       Schemas (Trade, QuoteL1), iceoryx2 service tuning,
                 wire protocol for the SQL Unix socket, latency histogram.
  tp-config      Symbol TOML loader + fixed-point encode/decode helpers.
  tp-wal         Append-only mmap-backed WAL writer/reader.
  tp-arrow       Per-symbol record stores and Arrow snapshot/IPC helpers.

bin/
  feed-replayer  Reads JSONL feed events, publishes to `rdb/trades/raw`
                 and `rdb/quotes/raw`. Firehose or wall-clock pacing.
  tickerplant    Subscribes to `*/raw`, assigns sequence numbers, persists
                 to mmap'd WAL, republishes on `*/agg`.
  rdb            Subscribes to `*/agg`, appends to per-symbol Arrow
                 buffers, serves SQL on a Unix domain socket.
  hdb-rollup     Snapshots the live tables to dated Parquet files at
                 end of day. Run once per session (e.g. via cron).
  query-cli      Sends one SQL query over the Unix socket, prints the
                 result as a pretty Arrow table.
  pg-gateway     PostgreSQL wire-protocol proxy for the Unix socket so
                 standard SQL clients (DBeaver, DataGrip, psql, …) can
                 connect without any plugin.
```

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
trades_hist   read_parquet('<hdb-dir>/trades/*.parquet')  (when --hdb is set
              and at least one file exists)
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

At the end of each trading session, run `hdb-rollup` to snapshot the
live tables to Parquet:

```bash
# Roll up today (UTC date inferred automatically).
./target/release/hdb-rollup --hdb-dir ./hdb

# Or supply an explicit date for backfills.
./target/release/hdb-rollup --hdb-dir ./hdb --date 2024-01-15
```

This writes:

```
hdb/
  trades/2024-01-15.parquet
  trades/2024-01-16.parquet
  quotes/2024-01-15.parquet
  quotes/2024-01-16.parquet
  …
```

Each file is ZSTD-compressed and has the same schema as the live tables.

To make the rdb serve both live and historical data, pass `--hdb`:

```bash
./target/release/rdb --symbols config/symbols.toml --hdb ./hdb
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
0 0 * * * /path/to/hdb-rollup --hdb-dir /data/hdb >> /var/log/hdb-rollup.log 2>&1
```

## PostgreSQL gateway

Any client that speaks the Postgres protocol can connect directly:

```bash
# Start the gateway (defaults: 0.0.0.0:5432, /tmp/rdb.sock).
./target/release/rdb-pg-gateway

# Or with custom endpoints.
./target/release/rdb-pg-gateway --listen 127.0.0.1:5433 --socket /tmp/rdb.sock
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

**Per-query DuckDB spin-up.**
`run_query` opens a fresh `Connection::open_in_memory()`, registers the
Arrow virtual table, and materialises two temp tables on every single
query — including trivial ones. This is 1–5 ms of overhead before the
SQL even runs. Under concurrent interactive load (dozens of BI tool
queries at once) this overhead multiplies and each query also takes a
full snapshot lock across all symbols. A persistent DuckDB connection
with pre-registered views would eliminate this overhead.

**Unbounded RDB memory.**
The in-memory store grows without eviction until EOD rollup. At 10 k
trades/min across 3 symbols that is roughly 100 MB/day — manageable.
At 100 symbols or tick-by-tick L2 data it can reach tens of GB before
midnight. There is no per-symbol memory cap or intra-day spill path.

**HDB glob reads all files.**
`read_parquet('trades/*.parquet')` opens every Parquet file in the
directory regardless of date predicates. DuckDB can skip row groups
within a file via statistics, but it cannot skip entire files without
Hive-style directory partitioning
(`trades/date=2024-01-15/data.parquet`). At ~365 files per year this is
fine; at multi-year history the file-open overhead becomes noticeable.
The fix is to switch to Hive partitioning in `hdb-rollup` and use
`read_parquet('trades/**/*.parquet', hive_partitioning=true)` in the
rdb.

**Fixed symbol set.**
Symbols are loaded from TOML at startup and assigned fixed integer ids.
Adding a symbol requires restarting the rdb (and losing in-memory data).

**pg-gateway: one round-trip per query.**
The gateway opens a new Unix socket connection for every query and waits
for the full response before returning. There is no pipelining or
connection multiplexing to the rdb. This is fine for interactive BI
tools; it is unsuitable for high-frequency programmatic querying.

### Scaling path (if this were to grow)

1. Replace iceoryx2 with a network transport (Aeron or Chronicle) to
   allow multi-host fan-out.
2. Shard the rdb by symbol group so each instance fits in RAM.
3. Switch to a persistent DuckDB connection with pre-registered Arrow
   views instead of per-query spin-up.
4. Add Hive partitioning to the HDB (`date=…/symbol=…/data.parquet`) for
   file-skip on both dimensions.
5. Add a rollup trigger or WAL-replay path so intra-day data can be
   partially flushed to Parquet without restarting the rdb.

## Running it

```bash
cargo build --release

# Terminal 1 — start the rdb (with optional HDB).
./target/release/rdb --symbols config/symbols.toml --hdb ./hdb

# Terminal 2 — start the tickerplant.
./target/release/tickerplant --symbols config/symbols.toml \
    --wal-dir /tmp/rdb-wal

# Terminal 3 — replay the sample feed.
./target/release/feed-replayer --symbols config/symbols.toml \
    --input samples/sample.jsonl

# Terminal 4 — query via CLI.
./target/release/rdb-query --sql "
    SELECT symbol, COUNT(*) AS n, AVG(price) AS avg_px
    FROM trades
    GROUP BY symbol
    ORDER BY symbol"

# Terminal 4 (alternative) — start the Postgres gateway and use psql.
./target/release/rdb-pg-gateway
psql -h localhost -p 5432 -d rdb -c "SELECT symbol, COUNT(*) FROM trades GROUP BY 1"

# At end of day — snapshot live tables to Parquet.
./target/release/hdb-rollup --hdb-dir ./hdb
```

## Tests

```bash
cargo test --workspace --lib            # unit tests
cargo test -p rdb --test end_to_end     # spawns the four binaries and
                                        # verifies count + asof join
```

The integration test cleans `/tmp/iceoryx2/` before it runs because
iceoryx2 0.7 has no env-based config root override; concurrent runs of
the integration test on the same machine will collide.

## Out of scope

Real exchange WebSockets, multi-host distribution, auth/TLS, schema
evolution, signal engine, principled backpressure — all deferred.
