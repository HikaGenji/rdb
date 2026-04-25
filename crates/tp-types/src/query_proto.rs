//! Wire format for the rdb query Unix socket.
//!
//! ```text
//! request:  u32 LE length | utf-8 SQL bytes
//! response: u8 status     | u32 LE length | payload
//!     status = 0 -> payload is Arrow IPC stream bytes
//!     status = 1 -> payload is utf-8 error message
//! ```
//!
//! Both helpers are blocking and intentionally minimal; the prototype uses
//! one query per connection.

use std::io::{self, Read, Write};

pub const STATUS_OK: u8 = 0;
pub const STATUS_ERR: u8 = 1;
pub const MAX_PAYLOAD: u32 = 256 * 1024 * 1024;

pub fn write_request<W: Write>(mut w: W, sql: &str) -> io::Result<()> {
    let bytes = sql.as_bytes();
    let len = bytes.len() as u32;
    w.write_all(&len.to_le_bytes())?;
    w.write_all(bytes)?;
    w.flush()
}

pub fn read_request<R: Read>(mut r: R) -> io::Result<String> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf);
    if len > MAX_PAYLOAD {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "request too large"));
    }
    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf)?;
    String::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

pub fn write_response<W: Write>(mut w: W, status: u8, payload: &[u8]) -> io::Result<()> {
    w.write_all(&[status])?;
    let len = payload.len() as u32;
    w.write_all(&len.to_le_bytes())?;
    w.write_all(payload)?;
    w.flush()
}

pub fn read_response<R: Read>(mut r: R) -> io::Result<(u8, Vec<u8>)> {
    let mut s = [0u8; 1];
    r.read_exact(&mut s)?;
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf);
    if len > MAX_PAYLOAD {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "response too large"));
    }
    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf)?;
    Ok((s[0], buf))
}
