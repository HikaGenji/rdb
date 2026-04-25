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
use tp_types::{QuoteL1, Side, Trade};

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

/// Per-symbol mutable buffers. Held inside a `Mutex` because both the ingest
/// thread and snapshot thread access them.
pub struct SymbolStore {
    pub symbol_id: u32,
    pub symbol: String,
    pub price_scale: u8,
    pub qty_scale: u8,
    pub trades: Mutex<TradeColumns>,
    pub quotes: Mutex<QuoteColumns>,
}

impl SymbolStore {
    pub fn new(symbol_id: u32, symbol: String, price_scale: u8, qty_scale: u8) -> Self {
        Self {
            symbol_id, symbol, price_scale, qty_scale,
            trades: Mutex::new(TradeColumns::default()),
            quotes: Mutex::new(QuoteColumns::default()),
        }
    }
}

/// All per-symbol stores. Owns the Arrow schemas it materializes.
pub struct StoreSet {
    pub stores: Vec<Arc<SymbolStore>>,
    pub trades_schema: SchemaRef,
    pub quotes_schema: SchemaRef,
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
            trades_schema: trades_schema(),
            quotes_schema: quotes_schema(),
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

    pub fn store_for(&self, symbol_id: u32) -> Option<&Arc<SymbolStore>> {
        self.stores.get(symbol_id as usize)
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
    fn ipc_round_trip() {
        let set = StoreSet::from_symbols(&st());
        let rb = set.snapshot_trades().unwrap();
        let bytes = encode_ipc_stream(&[rb.clone()], &set.trades_schema).unwrap();
        let back = decode_ipc_stream(&bytes).unwrap();
        assert_eq!(back.len(), 1);
        assert_eq!(back[0].schema(), rb.schema());
    }
}
