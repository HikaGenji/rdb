# rdb — Rust tickerplant prototype

A minimum-viable kdb-style tickerplant for crypto perpetuals, in Rust.
Architecture: feed handler → tickerplant → in-memory RDB queryable via
DuckDB-on-Arrow. Single host, shared-memory transport via [iceoryx2].

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
  query-cli      Sends one SQL query over the Unix socket, prints the
                 result as a pretty Arrow table.
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

The rdb materializes two consolidated tables — `trades` and `quotes` —
joined across all symbols. Columns:

```text
trades(symbol Utf8, symbol_id u32, seq u64,
       ts_exchange_ns u64, ts_local_ns u64,
       price f64, qty f64, side Utf8)

quotes(symbol Utf8, symbol_id u32, seq u64,
       ts_exchange_ns u64, ts_local_ns u64,
       bid_price f64, bid_qty f64, ask_price f64, ask_qty f64)
```

Queries can use the full DuckDB SQL surface, including `ASOF JOIN`.

## Latency hops

Each record carries `ts_local_ns` (wall-clock ns assigned at the feed
handler). Each downstream stage records `now() - ts_local_ns` into a
sliding-window histogram and emits `p50_ns`, `p99_ns` once a second on
stderr (JSON via `tracing-subscriber`). Drop counters are kept but the
prototype only logs them; backpressure beyond that is out of scope.

## Running it

```bash
cargo build --release

# Terminal 1 — start the rdb (listens on /tmp/rdb.sock).
./target/release/rdb --symbols config/symbols.toml

# Terminal 2 — start the tickerplant.
./target/release/tickerplant --symbols config/symbols.toml \
    --wal-dir /tmp/rdb-wal

# Terminal 3 — replay the sample feed.
./target/release/feed-replayer --symbols config/symbols.toml \
    --input samples/sample.jsonl

# Terminal 4 — run a query.
./target/release/rdb-query --sql "
    SELECT symbol, COUNT(*) AS n, AVG(price) AS avg_px
    FROM trades
    GROUP BY symbol
    ORDER BY symbol"
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

Real exchange WebSockets, multi-host distribution, auth/TLS, EOD Parquet
rotation, schema evolution, signal engine, principled backpressure —
all deferred.
