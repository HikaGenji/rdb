//! Symbol configuration and string interning.
//!
//! The tickerplant assigns each configured symbol a stable `u32` id at
//! startup. Wire records identify symbols by id; only the rdb attaches the
//! human-readable string when materializing query results.

use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolEntry {
    pub symbol: String,
    /// Number of decimal places encoded into the fixed-point integer price.
    /// e.g. `price_scale = 4` means the wire price `12345` represents `1.2345`.
    pub price_scale: u8,
    /// Number of decimal places encoded into the fixed-point integer qty.
    pub qty_scale: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolFile {
    pub symbol: Vec<SymbolEntry>,
}

#[derive(Debug, Clone)]
pub struct SymbolTable {
    by_name: HashMap<String, u32>,
    entries: Vec<SymbolEntry>,
}

impl SymbolTable {
    pub fn from_toml_str(s: &str) -> anyhow::Result<Self> {
        let parsed: SymbolFile = toml::from_str(s)?;
        let mut by_name = HashMap::with_capacity(parsed.symbol.len());
        for (idx, e) in parsed.symbol.iter().enumerate() {
            if by_name.insert(e.symbol.clone(), idx as u32).is_some() {
                anyhow::bail!("duplicate symbol in config: {}", e.symbol);
            }
        }
        Ok(Self { by_name, entries: parsed.symbol })
    }

    pub fn from_path(p: impl AsRef<Path>) -> anyhow::Result<Self> {
        let bytes = std::fs::read_to_string(p.as_ref())?;
        Self::from_toml_str(&bytes)
    }

    pub fn id_of(&self, name: &str) -> Option<u32> {
        self.by_name.get(name).copied()
    }

    pub fn entry(&self, id: u32) -> Option<&SymbolEntry> {
        self.entries.get(id as usize)
    }

    pub fn name(&self, id: u32) -> Option<&str> {
        self.entry(id).map(|e| e.symbol.as_str())
    }

    pub fn price_scale(&self, id: u32) -> u8 {
        self.entry(id).map(|e| e.price_scale).unwrap_or(0)
    }

    pub fn qty_scale(&self, id: u32) -> u8 {
        self.entry(id).map(|e| e.qty_scale).unwrap_or(0)
    }

    pub fn len(&self) -> usize { self.entries.len() }
    pub fn is_empty(&self) -> bool { self.entries.is_empty() }

    pub fn iter(&self) -> impl Iterator<Item = (u32, &SymbolEntry)> {
        self.entries.iter().enumerate().map(|(i, e)| (i as u32, e))
    }

    /// Encode a floating-point price into the symbol's fixed-point integer form.
    pub fn encode_price(&self, id: u32, price: f64) -> i64 {
        encode_fixed(price, self.price_scale(id))
    }
    pub fn encode_qty(&self, id: u32, qty: f64) -> i64 {
        encode_fixed(qty, self.qty_scale(id))
    }
    pub fn decode_price(&self, id: u32, raw: i64) -> f64 {
        decode_fixed(raw, self.price_scale(id))
    }
    pub fn decode_qty(&self, id: u32, raw: i64) -> f64 {
        decode_fixed(raw, self.qty_scale(id))
    }
}

fn pow10(n: u8) -> f64 {
    let mut x = 1.0;
    for _ in 0..n { x *= 10.0; }
    x
}

pub fn encode_fixed(v: f64, scale: u8) -> i64 {
    (v * pow10(scale)).round() as i64
}

pub fn decode_fixed(v: i64, scale: u8) -> f64 {
    (v as f64) / pow10(scale)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_two_symbols_and_round_trips() {
        let toml = r#"
            [[symbol]]
            symbol = "BTC-PERP"
            price_scale = 1
            qty_scale = 4

            [[symbol]]
            symbol = "ETH-PERP"
            price_scale = 2
            qty_scale = 4
        "#;
        let st = SymbolTable::from_toml_str(toml).unwrap();
        assert_eq!(st.len(), 2);
        let id = st.id_of("BTC-PERP").unwrap();
        assert_eq!(st.encode_price(id, 65000.5), 650005);
        assert!((st.decode_price(id, 650005) - 65000.5).abs() < 1e-9);
    }

    #[test]
    fn rejects_duplicates() {
        let toml = r#"
            [[symbol]]
            symbol = "X"
            price_scale = 0
            qty_scale = 0
            [[symbol]]
            symbol = "X"
            price_scale = 0
            qty_scale = 0
        "#;
        assert!(SymbolTable::from_toml_str(toml).is_err());
    }
}
