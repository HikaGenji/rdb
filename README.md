# rdb — Rust tickerplant prototype

A minimum-viable kdb-style tickerplant for crypto perpetuals, in Rust.
Architecture: feed handler → tickerplant → in-memory RDB queryable via
DuckDB-on-Arrow → optional HDB (daily Parquet files). Single host,
shared-memory transport via [iceoryx2].

This is a prototype to validate the architecture, not a production
system. The hot path is fixed-size POD records over shared memory; the
SQL layer materializes per-symbol Arrow buffers into a fresh DuckDB
in-memory database per query and uses the `arrow(?, ?)` table function
to zero-copy-bind the buffers.

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
all Parquet predicate-pushdown optimisations when filtering on the
timestamp columns.

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
in-memory data. To query only live data, use `trades_live`; to query
only history, use `trades_hist`.

Example multi-day query:

```sql
-- Executes via psql, rdb-query, DBeaver, etc.
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
