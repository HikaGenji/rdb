//! Lightweight SQL pre-flight check.
//!
//! User SQL still executes inside DuckDB — this guard only blocks DDL that
//! would shadow or destroy the streaming `trades`/`quotes` fixtures the
//! query path mounts as temp views before every query.

use anyhow::{anyhow, Result};
use sqlparser::ast::{ObjectName, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

const RESERVED: &[&str] = &[
    "trades",
    "quotes",
    "trades_live",
    "quotes_live",
    "trades_hist",
    "quotes_hist",
];

pub fn check(sql: &str) -> Result<()> {
    // If the parser can't handle the SQL (DuckDB-specific syntax,
    // extensions, etc.) let DuckDB return its own error rather than
    // pre-empting it here.
    let stmts = match Parser::parse_sql(&GenericDialect {}, sql) {
        Ok(s) => s,
        Err(_) => return Ok(()),
    };

    for stmt in &stmts {
        for name in ddl_targets(stmt) {
            if is_reserved(&name) {
                return Err(anyhow!(
                    "`{}` is a reserved streaming table; DDL on it is not allowed",
                    name
                ));
            }
        }
    }
    Ok(())
}

fn ddl_targets(stmt: &Statement) -> Vec<String> {
    match stmt {
        Statement::CreateTable(c) => vec![leaf(&c.name)],
        Statement::CreateView(c) => vec![leaf(&c.name)],
        Statement::AlterTable(a) => vec![leaf(&a.name)],
        Statement::Drop { names, .. } => names.iter().map(leaf).collect(),
        Statement::Truncate(t) => t.table_names.iter().map(|x| leaf(&x.name)).collect(),
        _ => Vec::new(),
    }
}

fn leaf(name: &ObjectName) -> String {
    // sqlparser models qualified names as a path of parts; only the last
    // identifier matters for our reserved-name check.
    name.0
        .last()
        .and_then(|p| p.as_ident())
        .map(|i| i.value.to_ascii_lowercase())
        .unwrap_or_default()
}

fn is_reserved(name: &str) -> bool {
    RESERVED.iter().any(|r| r.eq_ignore_ascii_case(name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_user_create_table() {
        assert!(check("CREATE TABLE foo (a INT, b VARCHAR)").is_ok());
    }

    #[test]
    fn allows_select_on_reserved() {
        assert!(check("SELECT count(*) FROM trades").is_ok());
        assert!(check("SELECT * FROM quotes WHERE symbol = 'BTC-PERP'").is_ok());
    }

    #[test]
    fn blocks_create_table_on_reserved() {
        assert!(check("CREATE TABLE trades (x INT)").is_err());
        assert!(check("CREATE TABLE Quotes (x INT)").is_err());
        assert!(check("CREATE TABLE main.trades_live (x INT)").is_err());
    }

    #[test]
    fn blocks_drop_on_reserved() {
        assert!(check("DROP TABLE trades").is_err());
        assert!(check("DROP VIEW IF EXISTS trades_hist").is_err());
    }

    #[test]
    fn blocks_alter_on_reserved() {
        assert!(check("ALTER TABLE quotes RENAME TO q2").is_err());
    }

    #[test]
    fn parse_failure_passes_through() {
        // Garbage / DuckDB-specific syntax we can't parse — pass through to
        // DuckDB instead of producing a misleading guard error.
        assert!(check("THIS IS NOT SQL").is_ok());
    }
}
