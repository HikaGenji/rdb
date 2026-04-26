# rdb Tutorial

A walk-through of installing `rdb` and exercising every role end-to-end on a
single machine using the bundled sample feed. By the end you will have:

- Built the `rdb` binary from source.
- Started a tickerplant, an in-memory rdb, and a feed replayer.
- Queried the live data over the CLI and over the Postgres wire protocol.
- Rolled the live data into a Parquet HDB and queried history.

The whole tour fits in five terminals and takes about ten minutes.

---

## 1. Prerequisites

| What | Why | How |
|---|---|---|
| Linux x86_64 | iceoryx2 shared-memory transport requires Linux | most modern distros work |
| Rust toolchain (stable, edition 2021) | builds the workspace | `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \| sh` |
| `pkg-config`, `clang`, `cmake`, build essentials | DuckDB and iceoryx2 build deps | Debian/Ubuntu: `sudo apt install build-essential pkg-config clang cmake` |
| `psql` (optional) | exercises the Postgres gateway | Debian/Ubuntu: `sudo apt install postgresql-client` |

Verify Rust:

```bash
rustc --version    # should report 1.75+ or similar
cargo --version
```

---

## 2. Get the source and build

```bash
git clone https://github.com/HikaGenji/rdb.git
cd rdb

# Release build (one fat-LTO binary; first build takes a few minutes).
cargo build --release
```

The result is a single executable at `./target/release/rdb`. Check it:

```bash
./target/release/rdb --help
./target/release/rdb serve --help
```

For convenience, add it to your shell PATH for the rest of the session:

```bash
export PATH="$PWD/target/release:$PATH"
```

(Optional) install it system-wide:

```bash
cargo install --path bin/rdb        # drops `rdb` into ~/.cargo/bin
```

---

## 3. The sample data

The repo ships a minimal feed and symbol table:

- `config/symbols.toml` — three perpetuals (`BTC-PERP`, `ETH-PERP`,
  `SOL-PERP`) with per-symbol fixed-point scales.
- `samples/sample.jsonl` — ten interleaved trade and quote events.

These are what the rest of the tutorial replays.

---

## 4. Start the stack

Open four terminals; in every one, make sure `rdb` is on `PATH` (step 2).

### Terminal 1 — in-memory query server

```bash
mkdir -p ./hdb
rdb serve \
    --symbols config/symbols.toml \
    --hdb ./hdb \
    --query-workers 4 \
    --row-cap 500000 \
    --rollup-interval-secs 0
```

This subscribes to the `*/agg` iceoryx2 topics, builds per-symbol Arrow
columnar buffers, and serves SQL on `/tmp/rdb.sock`. With `--hdb`, any
Parquet files under `./hdb/` are mounted alongside the live tables.

### Terminal 2 — tickerplant

```bash
mkdir -p /tmp/rdb-wal
rdb tickerplant \
    --symbols config/symbols.toml \
    --wal-dir /tmp/rdb-wal
```

The tickerplant subscribes to `*/raw`, assigns a sequence number, appends
to the WAL on `/tmp/rdb-wal`, and republishes on `*/agg` for the rdb to
consume.

### Terminal 3 — feed replayer

```bash
rdb feed-replayer \
    --symbols config/symbols.toml \
    --input samples/sample.jsonl
```

This decodes each JSONL line into the binary `Trade` / `QuoteL1` records
and publishes them to `*/raw`. The default `--pace firehose` blasts every
record at line rate; pass `--pace wall-clock` to honour the inter-event
gaps in the file.

You should see log lines on terminals 1 and 2 indicating that records
were ingested.

---

## 5. Query the live data — CLI

In a fourth terminal:

```bash
rdb query --sql "SELECT symbol, COUNT(*) AS n FROM trades GROUP BY symbol ORDER BY symbol"
```

Expected (counts depend on how many times you replayed the sample):

```
+----------+---+
| symbol   | n |
+----------+---+
| BTC-PERP | 3 |
| ETH-PERP | 2 |
| SOL-PERP | 1 |
+----------+---+
```

A couple more queries to try:

```bash
# Latest quote per symbol (top-of-book snapshot).
rdb query --sql "
  SELECT symbol, bid_price, ask_price, ask_price - bid_price AS spread
  FROM   quotes
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol
                             ORDER BY ts_exchange_ns DESC) = 1
  ORDER BY symbol"

# VWAP per symbol.
rdb query --sql "
  SELECT symbol,
         SUM(price*qty) / SUM(qty) AS vwap,
         SUM(qty)                  AS total_qty
  FROM   trades
  GROUP BY symbol
  ORDER BY symbol"

# ASOF join: each trade tagged with the prevailing top-of-book quote.
rdb query --sql "
  SELECT t.symbol, t.price, t.qty, q.bid_price, q.ask_price
  FROM   trades t ASOF LEFT JOIN quotes q
         ON t.symbol = q.symbol
        AND t.ts_exchange_ns >= q.ts_exchange_ns
  ORDER  BY t.ts_exchange_ns
  LIMIT 10"
```

You can also pipe SQL from stdin:

```bash
echo "SELECT COUNT(*) FROM trades" | rdb query --from-stdin
```

---

## 6. Query from any Postgres client

Start the gateway in a new terminal (the rdb process from step 4 must
still be running):

```bash
rdb pg-gateway --listen 127.0.0.1:5432 --socket /tmp/rdb.sock
```

Now any Postgres-aware client works without a plugin. Examples:

```bash
# psql — the database name is ignored, password is empty.
psql -h 127.0.0.1 -p 5432 -d rdb -c "SELECT symbol, COUNT(*) FROM trades GROUP BY 1 ORDER BY 1"
```

For DBeaver / DataGrip / Metabase / SQLPad: choose the PostgreSQL driver,
host `127.0.0.1`, port `5432`, no credentials. Then point the BI tool at
`trades` or `quotes`.

The gateway opens one connection to `/tmp/rdb.sock` per query — fine for
interactive analytics, not for high-frequency programmatic access.

---

## 7. Roll the live data into Parquet (HDB)

Stop the feed replayer (Ctrl-C in terminal 3) once you have enough rows,
then snapshot the live tables to disk:

```bash
rdb hdb-rollup --hdb-dir ./hdb
```

Inspect the output — Hive-partitioned by date and symbol:

```bash
find ./hdb -name '*.parquet'
# ./hdb/trades/date=2026-04-26/symbol=BTC-PERP/data.parquet
# ./hdb/trades/date=2026-04-26/symbol=ETH-PERP/data.parquet
# ./hdb/quotes/date=2026-04-26/symbol=BTC-PERP/data.parquet
# ...
```

Because the rdb in step 4 was launched with `--hdb ./hdb`, those files
are now visible to it transparently:

```bash
rdb query --sql "
  SELECT symbol,
         COUNT(*)                   AS rows,
         MIN(to_timestamp(ts_exchange_ns/1e9)) AS first_ts,
         MAX(to_timestamp(ts_exchange_ns/1e9)) AS last_ts
  FROM   trades_hist
  GROUP  BY symbol
  ORDER  BY symbol"
```

Three table tiers are exposed for each dataset — query whichever you
need:

| Table | Source |
|---|---|
| `trades_live` / `quotes_live` | today's in-memory rows only |
| `trades_hist` / `quotes_hist` | Parquet files under `--hdb` |
| `trades` / `quotes` | UNION ALL of the two above |

To run an unattended rollup at midnight UTC:

```cron
0 0 * * * /usr/local/bin/rdb hdb-rollup --hdb-dir /data/hdb >> /var/log/hdb-rollup.log 2>&1
```

---

## 8. Cleaning up

Stop the processes (Ctrl-C in each terminal) in any order. To wipe local
state between runs:

```bash
rm -rf /tmp/rdb-wal /tmp/rdb.sock /tmp/iceoryx2 ./hdb
```

`/tmp/iceoryx2/` holds the shared-memory segments; iceoryx2 0.7 has no
config-root override, so concurrent stacks on the same host will collide
unless this is cleared.

---

## 9. Where to go next

- Tweak `config/symbols.toml` to add/remove instruments (a restart is
  required — symbol ids are fixed at startup).
- Replace `samples/sample.jsonl` with a longer capture and replay it
  with `--pace wall-clock` to feel the live-rate behaviour.
- Set `--rollup-interval-secs 300` on `rdb serve` to flush in-memory
  data to Parquet every five minutes instead of waiting for end-of-day.
- Run the integration tests:

  ```bash
  cargo test --workspace --lib
  cargo test -p rdb --test end_to_end
  ```

- For multi-host deployments, see the
  [Multi-host deployment](README.md#multi-host-deployment-zenoh-bridge)
  section of the README — `rdb zenoh-bridge` relays `*/agg` over zenoh
  to remote rdb instances without modifying any other binary.
