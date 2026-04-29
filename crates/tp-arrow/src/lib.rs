//! In-memory per-symbol record stores and Arrow snapshots.
//!
//! The rdb keeps one [`SymbolStore`] per symbol id. The hot append path
//! pushes raw fixed-point integers into `VecDeque<T>`s under a per-symbol
//! mutex; the cold query path takes a single lock per symbol, snapshots the
//! current deques into an Arrow [`RecordBatch`], and releases the lock.
//! This trades a copy on the query side for a lock-free hot path on writes.
//!
//! When `max_rows > 0`, each column store evicts its oldest row on every
//! push that would exceed the cap — O(1) via `VecDeque::pop_front`.
//!
//! All snapshotting routines decode fixed-point ints to `f64` once, so the
//! SQL layer sees natural prices/qtys without per-row Decimal handling.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, Float64Array, RecordBatch, StringArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parking_lot::Mutex;

use tp_config::SymbolTable;
use tp_types::{BookL2, QuoteL1, Side, Trade, BOOK_L2_LEVELS};

#[derive(Default)]
pub struct TradeColumns {
    pub seq: VecDeque<u64>,
    pub ts_exchange_ns: VecDeque<u64>,
    pub ts_local_ns: VecDeque<u64>,
    pub price: VecDeque<i64>,
    pub qty: VecDeque<i64>,
    pub side: VecDeque<u8>,
}

impl TradeColumns {
    pub fn len(&self) -> usize { self.seq.len() }
    pub fn is_empty(&self) -> bool { self.seq.is_empty() }

    /// Append a trade record, evicting the oldest row if `max_rows > 0` and
    /// the store would exceed that limit.
    pub fn push(&mut self, t: &Trade, max_rows: usize) {
        self.seq.push_back(t.seq);
        self.ts_exchange_ns.push_back(t.ts_exchange_ns);
        self.ts_local_ns.push_back(t.ts_local_ns);
        self.price.push_back(t.price);
        self.qty.push_back(t.qty);
        self.side.push_back(t.side);
        if max_rows > 0 && self.seq.len() > max_rows {
            self.pop_oldest();
        }
    }

    fn pop_oldest(&mut self) {
        self.seq.pop_front();
        self.ts_exchange_ns.pop_front();
        self.ts_local_ns.pop_front();
        self.price.pop_front();
        self.qty.pop_front();
        self.side.pop_front();
    }
}

#[derive(Default)]
pub struct QuoteColumns {
    pub seq: VecDeque<u64>,
    pub ts_exchange_ns: VecDeque<u64>,
    pub ts_local_ns: VecDeque<u64>,
    pub bid_price: VecDeque<i64>,
    pub bid_qty: VecDeque<i64>,
    pub ask_price: VecDeque<i64>,
    pub ask_qty: VecDeque<i64>,
}

impl QuoteColumns {
    pub fn len(&self) -> usize { self.seq.len() }
    pub fn is_empty(&self) -> bool { self.seq.is_empty() }

    /// Append a quote record, evicting the oldest row if `max_rows > 0` and
    /// the store would exceed that limit.
    pub fn push(&mut self, q: &QuoteL1, max_rows: usize) {
        self.seq.push_back(q.seq);
        self.ts_exchange_ns.push_back(q.ts_exchange_ns);
        self.ts_local_ns.push_back(q.ts_local_ns);
        self.bid_price.push_back(q.bid_price);
        self.bid_qty.push_back(q.bid_qty);
        self.ask_price.push_back(q.ask_price);
        self.ask_qty.push_back(q.ask_qty);
        if max_rows > 0 && self.seq.len() > max_rows {
            self.pop_oldest();
        }
    }

    fn pop_oldest(&mut self) {
        self.seq.pop_front();
        self.ts_exchange_ns.pop_front();
        self.ts_local_ns.pop_front();
        self.bid_price.pop_front();
        self.bid_qty.pop_front();
        self.ask_price.pop_front();
        self.ask_qty.pop_front();
    }
}

/// L2 order-book column store. Each row carries `BOOK_L2_LEVELS` price+qty
/// pairs per side, ordered best-first (index 0 = best bid / best ask). Empty
/// trailing levels are zero-padded by the publisher.
#[derive(Default)]
pub struct BookColumns {
    pub seq: VecDeque<u64>,
    pub ts_exchange_ns: VecDeque<u64>,
    pub ts_local_ns: VecDeque<u64>,
    /// Levels-major: `bid_prices[level][row]` would be too tall to manage as
    /// a flat Vec, so we keep it as `[VecDeque<i64>; BOOK_L2_LEVELS]`.
    pub bid_prices: [VecDeque<i64>; BOOK_L2_LEVELS],
    pub bid_qtys:   [VecDeque<i64>; BOOK_L2_LEVELS],
    pub ask_prices: [VecDeque<i64>; BOOK_L2_LEVELS],
    pub ask_qtys:   [VecDeque<i64>; BOOK_L2_LEVELS],
}

impl BookColumns {
    pub fn len(&self) -> usize { self.seq.len() }
    pub fn is_empty(&self) -> bool { self.seq.is_empty() }

    /// Append an L2 record, evicting the oldest row if `max_rows > 0` and the
    /// store would exceed that limit.
    pub fn push(&mut self, b: &BookL2, max_rows: usize) {
        self.seq.push_back(b.seq);
        self.ts_exchange_ns.push_back(b.ts_exchange_ns);
        self.ts_local_ns.push_back(b.ts_local_ns);
        for lvl in 0..BOOK_L2_LEVELS {
            self.bid_prices[lvl].push_back(b.bid_prices[lvl]);
            self.bid_qtys[lvl].push_back(b.bid_qtys[lvl]);
            self.ask_prices[lvl].push_back(b.ask_prices[lvl]);
            self.ask_qtys[lvl].push_back(b.ask_qtys[lvl]);
        }
        if max_rows > 0 && self.seq.len() > max_rows {
            self.pop_oldest();
        }
    }

    fn pop_oldest(&mut self) {
        self.seq.pop_front();
        self.ts_exchange_ns.pop_front();
        self.ts_local_ns.pop_front();
        for lvl in 0..BOOK_L2_LEVELS {
            self.bid_prices[lvl].pop_front();
            self.bid_qtys[lvl].pop_front();
            self.ask_prices[lvl].pop_front();
            self.ask_qtys[lvl].pop_front();
        }
    }
}

/// Online OHLCV bars maintained per tick. Bucket boundaries are computed from
/// `ts_exchange_ns / bar_interval_ns`. Prices/quantities are decoded to f64
/// at insert time (bars are at most one row per `bar_interval_ns` per symbol,
/// so the decode cost is negligible vs raw ticks).
#[derive(Default)]
pub struct BarColumns {
    pub ts_bucket_ns: VecDeque<u64>,
    pub open:    VecDeque<f64>,
    pub high:    VecDeque<f64>,
    pub low:     VecDeque<f64>,
    pub close:   VecDeque<f64>,
    /// Sum of price*qty across the trades that fell in this bucket.
    pub vwap_num: VecDeque<f64>,
    /// Sum of qty across the trades in this bucket.
    pub volume:  VecDeque<f64>,
}

impl BarColumns {
    pub fn len(&self) -> usize { self.ts_bucket_ns.len() }
    pub fn is_empty(&self) -> bool { self.ts_bucket_ns.is_empty() }

    /// Update the current bar with a trade or open a new bucket. Returns
    /// `true` if a new bar was opened. `bar_interval_ns == 0` disables bars
    /// entirely. `max_rows` evicts the oldest bar when exceeded (O(1)).
    pub fn update_with_trade(
        &mut self,
        ts_ns: u64,
        price: f64,
        qty: f64,
        bar_interval_ns: u64,
        max_rows: usize,
    ) -> bool {
        if bar_interval_ns == 0 {
            return false;
        }
        let bucket = (ts_ns / bar_interval_ns) * bar_interval_ns;
        match self.ts_bucket_ns.back().copied() {
            Some(last) if last == bucket => {
                let i = self.ts_bucket_ns.len() - 1;
                if price > self.high[i] { self.high[i] = price; }
                if price < self.low[i]  { self.low[i]  = price; }
                self.close[i] = price;
                self.vwap_num[i] += price * qty;
                self.volume[i]   += qty;
                false
            }
            _ => {
                self.ts_bucket_ns.push_back(bucket);
                self.open.push_back(price);
                self.high.push_back(price);
                self.low.push_back(price);
                self.close.push_back(price);
                self.vwap_num.push_back(price * qty);
                self.volume.push_back(qty);
                if max_rows > 0 && self.ts_bucket_ns.len() > max_rows {
                    self.pop_oldest();
                }
                true
            }
        }
    }

    fn pop_oldest(&mut self) {
        self.ts_bucket_ns.pop_front();
        self.open.pop_front();
        self.high.pop_front();
        self.low.pop_front();
        self.close.pop_front();
        self.vwap_num.pop_front();
        self.volume.pop_front();
    }
}

/// Per-symbol mutable buffers. Held inside a `Mutex` because both the ingest
/// thread and snapshot thread access them.
pub struct SymbolStore {
    pub symbol_id: u32,
    pub symbol: String,
    pub price_scale: u8,
    pub qty_scale: u8,
    pub trades: Mutex<TradeColumns>,
    pub quotes: Mutex<QuoteColumns>,
    pub book_l2: Mutex<BookColumns>,
    pub bars:   Mutex<BarColumns>,
}

impl SymbolStore {
    pub fn new(symbol_id: u32, symbol: String, price_scale: u8, qty_scale: u8) -> Self {
        Self {
            symbol_id, symbol, price_scale, qty_scale,
            trades: Mutex::new(TradeColumns::default()),
            quotes: Mutex::new(QuoteColumns::default()),
            book_l2: Mutex::new(BookColumns::default()),
            bars:   Mutex::new(BarColumns::default()),
        }
    }

    /// Append a trade and update the bar in one call. Use this from the
    /// ingest loop and from WAL replay so bars stay consistent with raw
    /// ticks. `bar_interval_ns == 0` disables bar maintenance.
    pub fn push_trade(
        &self,
        t: &Trade,
        max_rows: usize,
        bar_interval_ns: u64,
        bar_max_rows: usize,
    ) {
        self.trades.lock().push(t, max_rows);
        if bar_interval_ns > 0 {
            let price = decode_fixed(t.price, self.price_scale);
            let qty   = decode_fixed(t.qty,   self.qty_scale);
            self.bars.lock().update_with_trade(
                t.ts_exchange_ns,
                price,
                qty,
                bar_interval_ns,
                bar_max_rows,
            );
        }
    }
}

/// Carries the Arrow snapshots and per-symbol row counts for one rollup cycle.
///
/// Pass to [`StoreSet::commit_rollup`] **only after** the Parquet files have
/// been durably written (and atomically renamed into place). If the write
/// fails, drop this value — the rows remain in memory for the next cycle.
pub struct RollupBatch {
    pub trades: RecordBatch,
    pub quotes: RecordBatch,
    /// Number of rows captured per symbol for the `trades` snapshot.
    trades_counts: Vec<usize>,
    /// Number of rows captured per symbol for the `quotes` snapshot.
    quotes_counts: Vec<usize>,
}

/// All per-symbol stores. Owns the Arrow schemas it materializes.
pub struct StoreSet {
    pub stores: Vec<Arc<SymbolStore>>,
    pub trades_schema:  SchemaRef,
    pub quotes_schema:  SchemaRef,
    pub bars_schema:    SchemaRef,
    pub book_l2_schema: SchemaRef,
}

impl StoreSet {
    pub fn from_symbols(table: &SymbolTable) -> Self {
        let stores = table
            .iter()
            .map(|(id, e)| {
                Arc::new(SymbolStore::new(id, e.symbol.clone(), e.price_scale, e.qty_scale))
            })
            .collect();
        Self {
            stores,
            trades_schema:  trades_schema(),
            quotes_schema:  quotes_schema(),
            bars_schema:    bars_schema(),
            book_l2_schema: book_l2_schema(),
        }
    }

    pub fn snapshot_trades(&self) -> anyhow::Result<RecordBatch> {
        let mut symbol = Vec::new();
        let mut symbol_id = Vec::new();
        let mut seq = Vec::new();
        let mut ts_exchange_ns = Vec::new();
        let mut ts_local_ns = Vec::new();
        let mut price = Vec::new();
        let mut qty = Vec::new();
        let mut side = Vec::new();

        for store in &self.stores {
            let cols = store.trades.lock();
            for i in 0..cols.len() {
                symbol.push(store.symbol.clone());
                symbol_id.push(store.symbol_id);
                seq.push(cols.seq[i]);
                ts_exchange_ns.push(cols.ts_exchange_ns[i]);
                ts_local_ns.push(cols.ts_local_ns[i]);
                price.push(decode_fixed(cols.price[i], store.price_scale));
                qty.push(decode_fixed(cols.qty[i], store.qty_scale));
                side.push(Side::from_u8(cols.side[i]).as_str().to_string());
            }
        }
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(symbol)),
            Arc::new(UInt32Array::from(symbol_id)),
            Arc::new(UInt64Array::from(seq)),
            Arc::new(UInt64Array::from(ts_exchange_ns)),
            Arc::new(UInt64Array::from(ts_local_ns)),
            Arc::new(Float64Array::from(price)),
            Arc::new(Float64Array::from(qty)),
            Arc::new(StringArray::from(side)),
        ];
        Ok(RecordBatch::try_new(self.trades_schema.clone(), arrays)?)
    }

    pub fn snapshot_quotes(&self) -> anyhow::Result<RecordBatch> {
        let mut symbol = Vec::new();
        let mut symbol_id = Vec::new();
        let mut seq = Vec::new();
        let mut ts_exchange_ns = Vec::new();
        let mut ts_local_ns = Vec::new();
        let mut bid_price = Vec::new();
        let mut bid_qty = Vec::new();
        let mut ask_price = Vec::new();
        let mut ask_qty = Vec::new();

        for store in &self.stores {
            let cols = store.quotes.lock();
            for i in 0..cols.len() {
                symbol.push(store.symbol.clone());
                symbol_id.push(store.symbol_id);
                seq.push(cols.seq[i]);
                ts_exchange_ns.push(cols.ts_exchange_ns[i]);
                ts_local_ns.push(cols.ts_local_ns[i]);
                bid_price.push(decode_fixed(cols.bid_price[i], store.price_scale));
                bid_qty.push(decode_fixed(cols.bid_qty[i], store.qty_scale));
                ask_price.push(decode_fixed(cols.ask_price[i], store.price_scale));
                ask_qty.push(decode_fixed(cols.ask_qty[i], store.qty_scale));
            }
        }
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(symbol)),
            Arc::new(UInt32Array::from(symbol_id)),
            Arc::new(UInt64Array::from(seq)),
            Arc::new(UInt64Array::from(ts_exchange_ns)),
            Arc::new(UInt64Array::from(ts_local_ns)),
            Arc::new(Float64Array::from(bid_price)),
            Arc::new(Float64Array::from(bid_qty)),
            Arc::new(Float64Array::from(ask_price)),
            Arc::new(Float64Array::from(ask_qty)),
        ];
        Ok(RecordBatch::try_new(self.quotes_schema.clone(), arrays)?)
    }

    /// Snapshot both live tables for a rollup cycle **without clearing them**.
    ///
    /// The returned [`RollupBatch`] carries the Arrow data plus the per-symbol
    /// row counts captured at snapshot time. Pass it to [`commit_rollup`] once
    /// the Parquet files have been durably written; that call trims exactly
    /// those rows from the front of each VecDeque. If writing fails, simply
    /// drop the `RollupBatch` — the rows remain in memory and will be included
    /// in the next cycle.
    pub fn snapshot_for_rollup(&self) -> anyhow::Result<RollupBatch> {
        // --- trades ---
        let mut t_symbol = Vec::new();
        let mut t_symbol_id = Vec::new();
        let mut t_seq = Vec::new();
        let mut t_ts_exchange_ns = Vec::new();
        let mut t_ts_local_ns = Vec::new();
        let mut t_price = Vec::new();
        let mut t_qty = Vec::new();
        let mut t_side = Vec::new();
        let mut trades_counts = Vec::with_capacity(self.stores.len());

        for store in &self.stores {
            let cols = store.trades.lock();
            trades_counts.push(cols.len());
            for i in 0..cols.len() {
                t_symbol.push(store.symbol.clone());
                t_symbol_id.push(store.symbol_id);
                t_seq.push(cols.seq[i]);
                t_ts_exchange_ns.push(cols.ts_exchange_ns[i]);
                t_ts_local_ns.push(cols.ts_local_ns[i]);
                t_price.push(decode_fixed(cols.price[i], store.price_scale));
                t_qty.push(decode_fixed(cols.qty[i], store.qty_scale));
                t_side.push(Side::from_u8(cols.side[i]).as_str().to_string());
            }
        }
        let trades = RecordBatch::try_new(
            self.trades_schema.clone(),
            vec![
                Arc::new(StringArray::from(t_symbol)),
                Arc::new(UInt32Array::from(t_symbol_id)),
                Arc::new(UInt64Array::from(t_seq)),
                Arc::new(UInt64Array::from(t_ts_exchange_ns)),
                Arc::new(UInt64Array::from(t_ts_local_ns)),
                Arc::new(Float64Array::from(t_price)),
                Arc::new(Float64Array::from(t_qty)),
                Arc::new(StringArray::from(t_side)),
            ],
        )?;

        // --- quotes ---
        let mut q_symbol = Vec::new();
        let mut q_symbol_id = Vec::new();
        let mut q_seq = Vec::new();
        let mut q_ts_exchange_ns = Vec::new();
        let mut q_ts_local_ns = Vec::new();
        let mut q_bid_price = Vec::new();
        let mut q_bid_qty = Vec::new();
        let mut q_ask_price = Vec::new();
        let mut q_ask_qty = Vec::new();
        let mut quotes_counts = Vec::with_capacity(self.stores.len());

        for store in &self.stores {
            let cols = store.quotes.lock();
            quotes_counts.push(cols.len());
            for i in 0..cols.len() {
                q_symbol.push(store.symbol.clone());
                q_symbol_id.push(store.symbol_id);
                q_seq.push(cols.seq[i]);
                q_ts_exchange_ns.push(cols.ts_exchange_ns[i]);
                q_ts_local_ns.push(cols.ts_local_ns[i]);
                q_bid_price.push(decode_fixed(cols.bid_price[i], store.price_scale));
                q_bid_qty.push(decode_fixed(cols.bid_qty[i], store.qty_scale));
                q_ask_price.push(decode_fixed(cols.ask_price[i], store.price_scale));
                q_ask_qty.push(decode_fixed(cols.ask_qty[i], store.qty_scale));
            }
        }
        let quotes = RecordBatch::try_new(
            self.quotes_schema.clone(),
            vec![
                Arc::new(StringArray::from(q_symbol)),
                Arc::new(UInt32Array::from(q_symbol_id)),
                Arc::new(UInt64Array::from(q_seq)),
                Arc::new(UInt64Array::from(q_ts_exchange_ns)),
                Arc::new(UInt64Array::from(q_ts_local_ns)),
                Arc::new(Float64Array::from(q_bid_price)),
                Arc::new(Float64Array::from(q_bid_qty)),
                Arc::new(Float64Array::from(q_ask_price)),
                Arc::new(Float64Array::from(q_ask_qty)),
            ],
        )?;

        Ok(RollupBatch { trades, quotes, trades_counts, quotes_counts })
    }

    /// Trim the rows captured by [`snapshot_for_rollup`] from the front of
    /// every symbol's VecDeque. Call this **only after** the Parquet files
    /// have been successfully written and renamed into place.
    ///
    /// Because ingest appends to the *back* of each deque, trimming the first
    /// N rows from the *front* removes exactly the snapshot rows without
    /// touching anything that arrived during the write.
    pub fn commit_rollup(&self, batch: &RollupBatch) {
        for (store, &n) in self.stores.iter().zip(&batch.trades_counts) {
            let mut cols = store.trades.lock();
            for _ in 0..n.min(cols.len()) {
                cols.pop_oldest();
            }
        }
        for (store, &n) in self.stores.iter().zip(&batch.quotes_counts) {
            let mut cols = store.quotes.lock();
            for _ in 0..n.min(cols.len()) {
                cols.pop_oldest();
            }
        }
    }

    pub fn store_for(&self, symbol_id: u32) -> Option<&Arc<SymbolStore>> {
        self.stores.get(symbol_id as usize)
    }

    /// Snapshot the in-memory OHLCV bars across all symbols. VWAP is
    /// materialised here as `vwap_num / volume`; `volume == 0` rows are
    /// skipped (cannot occur in normal operation since bars are opened by
    /// a real trade).
    pub fn snapshot_bars(&self) -> anyhow::Result<RecordBatch> {
        let mut symbol = Vec::new();
        let mut symbol_id = Vec::new();
        let mut ts_bucket_ns = Vec::new();
        let mut open = Vec::new();
        let mut high = Vec::new();
        let mut low  = Vec::new();
        let mut close = Vec::new();
        let mut vwap = Vec::new();
        let mut volume = Vec::new();

        for store in &self.stores {
            let cols = store.bars.lock();
            for i in 0..cols.len() {
                let v = cols.volume[i];
                if v == 0.0 { continue; }
                symbol.push(store.symbol.clone());
                symbol_id.push(store.symbol_id);
                ts_bucket_ns.push(cols.ts_bucket_ns[i]);
                open.push(cols.open[i]);
                high.push(cols.high[i]);
                low.push(cols.low[i]);
                close.push(cols.close[i]);
                vwap.push(cols.vwap_num[i] / v);
                volume.push(v);
            }
        }
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(symbol)),
            Arc::new(UInt32Array::from(symbol_id)),
            Arc::new(UInt64Array::from(ts_bucket_ns)),
            Arc::new(Float64Array::from(open)),
            Arc::new(Float64Array::from(high)),
            Arc::new(Float64Array::from(low)),
            Arc::new(Float64Array::from(close)),
            Arc::new(Float64Array::from(vwap)),
            Arc::new(Float64Array::from(volume)),
        ];
        Ok(RecordBatch::try_new(self.bars_schema.clone(), arrays)?)
    }

    /// Snapshot the in-memory L2 books across all symbols. Each row contains
    /// `BOOK_L2_LEVELS` price+qty pairs per side, decoded to f64.
    pub fn snapshot_book_l2(&self) -> anyhow::Result<RecordBatch> {
        let mut symbol = Vec::new();
        let mut symbol_id = Vec::new();
        let mut seq = Vec::new();
        let mut ts_exchange_ns = Vec::new();
        let mut ts_local_ns = Vec::new();
        let mut bid_prices: Vec<Vec<f64>> = (0..BOOK_L2_LEVELS).map(|_| Vec::new()).collect();
        let mut bid_qtys:   Vec<Vec<f64>> = (0..BOOK_L2_LEVELS).map(|_| Vec::new()).collect();
        let mut ask_prices: Vec<Vec<f64>> = (0..BOOK_L2_LEVELS).map(|_| Vec::new()).collect();
        let mut ask_qtys:   Vec<Vec<f64>> = (0..BOOK_L2_LEVELS).map(|_| Vec::new()).collect();

        for store in &self.stores {
            let cols = store.book_l2.lock();
            for i in 0..cols.len() {
                symbol.push(store.symbol.clone());
                symbol_id.push(store.symbol_id);
                seq.push(cols.seq[i]);
                ts_exchange_ns.push(cols.ts_exchange_ns[i]);
                ts_local_ns.push(cols.ts_local_ns[i]);
                for lvl in 0..BOOK_L2_LEVELS {
                    bid_prices[lvl].push(decode_fixed(cols.bid_prices[lvl][i], store.price_scale));
                    bid_qtys[lvl].push(decode_fixed(cols.bid_qtys[lvl][i],   store.qty_scale));
                    ask_prices[lvl].push(decode_fixed(cols.ask_prices[lvl][i], store.price_scale));
                    ask_qtys[lvl].push(decode_fixed(cols.ask_qtys[lvl][i],   store.qty_scale));
                }
            }
        }
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(5 + 4 * BOOK_L2_LEVELS);
        arrays.push(Arc::new(StringArray::from(symbol)));
        arrays.push(Arc::new(UInt32Array::from(symbol_id)));
        arrays.push(Arc::new(UInt64Array::from(seq)));
        arrays.push(Arc::new(UInt64Array::from(ts_exchange_ns)));
        arrays.push(Arc::new(UInt64Array::from(ts_local_ns)));
        for lvl in 0..BOOK_L2_LEVELS {
            arrays.push(Arc::new(Float64Array::from(std::mem::take(&mut bid_prices[lvl]))));
            arrays.push(Arc::new(Float64Array::from(std::mem::take(&mut bid_qtys[lvl]))));
        }
        for lvl in 0..BOOK_L2_LEVELS {
            arrays.push(Arc::new(Float64Array::from(std::mem::take(&mut ask_prices[lvl]))));
            arrays.push(Arc::new(Float64Array::from(std::mem::take(&mut ask_qtys[lvl]))));
        }
        Ok(RecordBatch::try_new(self.book_l2_schema.clone(), arrays)?)
    }
}

pub fn trades_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("symbol",         DataType::Utf8,    false),
        Field::new("symbol_id",      DataType::UInt32,  false),
        Field::new("seq",            DataType::UInt64,  false),
        Field::new("ts_exchange_ns", DataType::UInt64,  false),
        Field::new("ts_local_ns",    DataType::UInt64,  false),
        Field::new("price",          DataType::Float64, false),
        Field::new("qty",            DataType::Float64, false),
        Field::new("side",           DataType::Utf8,    false),
    ]))
}

pub fn bars_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("symbol",        DataType::Utf8,    false),
        Field::new("symbol_id",     DataType::UInt32,  false),
        Field::new("ts_bucket_ns",  DataType::UInt64,  false),
        Field::new("open",          DataType::Float64, false),
        Field::new("high",          DataType::Float64, false),
        Field::new("low",           DataType::Float64, false),
        Field::new("close",         DataType::Float64, false),
        Field::new("vwap",          DataType::Float64, false),
        Field::new("volume",        DataType::Float64, false),
    ]))
}

pub fn book_l2_schema() -> SchemaRef {
    let mut fields: Vec<Field> = Vec::with_capacity(5 + 4 * BOOK_L2_LEVELS);
    fields.push(Field::new("symbol",         DataType::Utf8,    false));
    fields.push(Field::new("symbol_id",      DataType::UInt32,  false));
    fields.push(Field::new("seq",            DataType::UInt64,  false));
    fields.push(Field::new("ts_exchange_ns", DataType::UInt64,  false));
    fields.push(Field::new("ts_local_ns",    DataType::UInt64,  false));
    for lvl in 0..BOOK_L2_LEVELS {
        fields.push(Field::new(format!("bid_price_{lvl}"), DataType::Float64, false));
        fields.push(Field::new(format!("bid_qty_{lvl}"),   DataType::Float64, false));
    }
    for lvl in 0..BOOK_L2_LEVELS {
        fields.push(Field::new(format!("ask_price_{lvl}"), DataType::Float64, false));
        fields.push(Field::new(format!("ask_qty_{lvl}"),   DataType::Float64, false));
    }
    Arc::new(Schema::new(fields))
}

pub fn quotes_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("symbol",         DataType::Utf8,    false),
        Field::new("symbol_id",      DataType::UInt32,  false),
        Field::new("seq",            DataType::UInt64,  false),
        Field::new("ts_exchange_ns", DataType::UInt64,  false),
        Field::new("ts_local_ns",    DataType::UInt64,  false),
        Field::new("bid_price",      DataType::Float64, false),
        Field::new("bid_qty",        DataType::Float64, false),
        Field::new("ask_price",      DataType::Float64, false),
        Field::new("ask_qty",        DataType::Float64, false),
    ]))
}

fn decode_fixed(v: i64, scale: u8) -> f64 {
    let mut p = 1.0f64;
    for _ in 0..scale { p *= 10.0; }
    (v as f64) / p
}

/// Encode a `RecordBatch` as Arrow IPC stream bytes.
pub fn encode_ipc_stream(batches: &[RecordBatch], schema: &SchemaRef) -> anyhow::Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(8 * 1024);
    {
        let mut w = arrow_ipc::writer::StreamWriter::try_new(&mut buf, schema)?;
        for b in batches {
            w.write(b)?;
        }
        w.finish()?;
    }
    Ok(buf)
}

/// Decode Arrow IPC stream bytes into a vector of `RecordBatch`.
pub fn decode_ipc_stream(bytes: &[u8]) -> anyhow::Result<Vec<RecordBatch>> {
    let r = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
    let mut out = Vec::new();
    for b in r {
        out.push(b?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn st() -> SymbolTable {
        SymbolTable::from_toml_str(r#"
            [[symbol]]
            symbol = "BTC-PERP"
            price_scale = 1
            qty_scale = 4
        "#).unwrap()
    }

    fn make_trade(seq: u64) -> Trade {
        Trade {
            seq,
            ts_exchange_ns: 100,
            ts_local_ns: 200,
            symbol_id: 0,
            _pad0: 0,
            price: 650005,
            qty: 10000,
            side: 0,
            _pad1: [0; 7],
        }
    }

    #[test]
    fn snapshot_one_trade() {
        let set = StoreSet::from_symbols(&st());
        set.stores[0].trades.lock().push(&make_trade(1), 0);
        let rb = set.snapshot_trades().unwrap();
        assert_eq!(rb.num_rows(), 1);
    }

    #[test]
    fn row_cap_evicts_oldest() {
        let set = StoreSet::from_symbols(&st());
        let store = &set.stores[0];
        for seq in 1..=5 {
            store.trades.lock().push(&make_trade(seq), 3);
        }
        // cap=3: seqs 1,2 should be evicted; 3,4,5 remain
        let cols = store.trades.lock();
        assert_eq!(cols.len(), 3);
        assert_eq!(cols.seq[0], 3);
        assert_eq!(cols.seq[2], 5);
    }

    #[test]
    fn bars_aggregate_within_bucket_and_rotate_across() {
        let set = StoreSet::from_symbols(&st());
        let store = &set.stores[0];

        // 1-second buckets. price_scale=1 → encoded fixed-point = price*10.
        let bar_ns: u64 = 1_000_000_000;

        // Bucket 0: three trades at 0ns, 100ns, 999_999_999ns.
        let mut t = make_trade(1); t.ts_exchange_ns = 0;             t.price = 100_000; t.qty = 5; // 10000.0 px, 0.0005 qty
        store.push_trade(&t, 0, bar_ns, 0);
        let mut t = make_trade(2); t.ts_exchange_ns = 100;           t.price = 110_000; t.qty = 3; // 11000.0
        store.push_trade(&t, 0, bar_ns, 0);
        let mut t = make_trade(3); t.ts_exchange_ns = 999_999_999;   t.price = 90_000;  t.qty = 2; //  9000.0
        store.push_trade(&t, 0, bar_ns, 0);
        // Bucket 1: one trade at 1_500_000_000ns.
        let mut t = make_trade(4); t.ts_exchange_ns = 1_500_000_000; t.price = 120_000; t.qty = 4; // 12000.0
        store.push_trade(&t, 0, bar_ns, 0);

        let bars = store.bars.lock();
        assert_eq!(bars.len(), 2, "two buckets expected, got {}", bars.len());

        // Bucket 0: open=10000, high=11000, low=9000, close=9000.
        assert_eq!(bars.ts_bucket_ns[0], 0);
        assert_eq!(bars.open[0],  10000.0);
        assert_eq!(bars.high[0],  11000.0);
        assert_eq!(bars.low[0],   9000.0);
        assert_eq!(bars.close[0], 9000.0);

        // Bucket 1: open=close=12000.
        assert_eq!(bars.ts_bucket_ns[1], 1_000_000_000);
        assert_eq!(bars.open[1],  12000.0);
        assert_eq!(bars.close[1], 12000.0);
    }

    #[test]
    fn snapshot_bars_emits_vwap() {
        let set = StoreSet::from_symbols(&st());
        let bar_ns: u64 = 1_000_000_000;
        let store = &set.stores[0];

        // Two trades in the same bucket: (10000, 0.0005), (11000, 0.0003).
        let mut t = make_trade(1); t.ts_exchange_ns = 0;   t.price = 100_000; t.qty = 5;
        store.push_trade(&t, 0, bar_ns, 0);
        let mut t = make_trade(2); t.ts_exchange_ns = 100; t.price = 110_000; t.qty = 3;
        store.push_trade(&t, 0, bar_ns, 0);

        let rb = set.snapshot_bars().unwrap();
        assert_eq!(rb.num_rows(), 1);
        let vwap_col = rb.column(7).as_any().downcast_ref::<Float64Array>().unwrap();
        // VWAP = (10000*0.0005 + 11000*0.0003) / (0.0005 + 0.0003) = 8.3 / 0.0008 = 10375.
        assert!((vwap_col.value(0) - 10375.0).abs() < 1e-6, "vwap = {}", vwap_col.value(0));
    }

    #[test]
    fn ipc_round_trip() {
        let set = StoreSet::from_symbols(&st());
        let rb = set.snapshot_trades().unwrap();
        let bytes = encode_ipc_stream(&[rb.clone()], &set.trades_schema).unwrap();
        let back = decode_ipc_stream(&bytes).unwrap();
        assert_eq!(back.len(), 1);
        assert_eq!(back[0].schema(), rb.schema());
    }
}
