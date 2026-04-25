//! Append-only mmap-backed WAL.
//!
//! A WAL file is a flat sequence of fixed-size records preceded by a tiny
//! header (`magic`, `record_size`, `count`). The writer grows the underlying
//! file in chunks (`grow_chunk_bytes`), remaps, and appends. We keep this
//! single-process / single-writer for the prototype.
//!
//! On a clean shutdown the writer trims the file to `header_size + count *
//! record_size`. After a crash the file may end with up to one chunk of
//! zeroed slack; readers should stop at `count` records.

use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use bytemuck::{Pod, Zeroable};
use memmap2::{MmapMut, MmapOptions};

const MAGIC: u64 = 0x52444257_414c5630; // "RDBWALV0"
const HEADER_SIZE: usize = std::mem::size_of::<WalHeader>();
const DEFAULT_GROW_CHUNK: usize = 4 * 1024 * 1024;

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct WalHeader {
    magic: u64,
    record_size: u64,
    count: u64,
}

pub struct WalWriter<T: Pod + Zeroable> {
    path: PathBuf,
    file: File,
    mmap: MmapMut,
    capacity_records: usize,
    grow_chunk_bytes: usize,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Pod + Zeroable> WalWriter<T> {
    pub fn create(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Self::create_with_chunk(path, DEFAULT_GROW_CHUNK)
    }

    pub fn create_with_chunk(path: impl AsRef<Path>, grow_chunk_bytes: usize) -> anyhow::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&path)?;
        let initial = HEADER_SIZE as u64 + grow_chunk_bytes as u64;
        file.set_len(initial)?;
        let mut mmap = unsafe { MmapOptions::new().len(initial as usize).map_mut(&file)? };

        let hdr = WalHeader {
            magic: MAGIC,
            record_size: std::mem::size_of::<T>() as u64,
            count: 0,
        };
        mmap[..HEADER_SIZE].copy_from_slice(bytemuck::bytes_of(&hdr));

        Ok(Self {
            path,
            file,
            mmap,
            capacity_records: grow_chunk_bytes / std::mem::size_of::<T>(),
            grow_chunk_bytes,
            _phantom: std::marker::PhantomData,
        })
    }

    fn header(&self) -> &WalHeader {
        bytemuck::from_bytes(&self.mmap[..HEADER_SIZE])
    }

    fn header_mut(&mut self) -> &mut WalHeader {
        bytemuck::from_bytes_mut(&mut self.mmap[..HEADER_SIZE])
    }

    pub fn count(&self) -> u64 {
        self.header().count
    }

    pub fn append(&mut self, record: &T) -> anyhow::Result<u64> {
        let count = self.header().count as usize;
        if count >= self.capacity_records {
            self.grow()?;
        }
        let offset = HEADER_SIZE + count * std::mem::size_of::<T>();
        self.mmap[offset..offset + std::mem::size_of::<T>()]
            .copy_from_slice(bytemuck::bytes_of(record));
        self.header_mut().count = (count as u64) + 1;
        Ok(count as u64 + 1)
    }

    fn grow(&mut self) -> anyhow::Result<()> {
        let new_len = self.mmap.len() + self.grow_chunk_bytes;
        // Save count, drop mmap, resize file, remap.
        let count = self.header().count;
        let _ = self.mmap.flush();
        // Replace mmap with a placeholder so we can drop the old one before
        // resizing the underlying file.
        let placeholder = MmapMut::map_anon(1)?;
        let _old = std::mem::replace(&mut self.mmap, placeholder);
        drop(_old);
        self.file.set_len(new_len as u64)?;
        self.mmap = unsafe { MmapOptions::new().len(new_len).map_mut(&self.file)? };
        // Re-stamp the header (count etc. are still in the mmap'd bytes).
        let hdr = self.header_mut();
        hdr.magic = MAGIC;
        hdr.record_size = std::mem::size_of::<T>() as u64;
        hdr.count = count;
        self.capacity_records = (new_len - HEADER_SIZE) / std::mem::size_of::<T>();
        Ok(())
    }

    pub fn flush(&mut self) -> anyhow::Result<()> {
        self.mmap.flush()?;
        Ok(())
    }

    pub fn path(&self) -> &Path { &self.path }

    /// Trim the file to the minimum size required to hold the current records.
    pub fn finalize(mut self) -> anyhow::Result<()> {
        self.mmap.flush()?;
        let count = self.header().count as usize;
        let final_len = HEADER_SIZE + count * std::mem::size_of::<T>();
        // Drop mmap before truncating.
        self.mmap = MmapMut::map_anon(1)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.file.set_len(final_len as u64)?;
        self.file.flush()?;
        Ok(())
    }
}

/// Read all complete records from a WAL file written by [`WalWriter`].
pub fn read_all<T: Pod + Zeroable>(path: impl AsRef<Path>) -> anyhow::Result<Vec<T>> {
    let bytes = std::fs::read(path.as_ref())?;
    if bytes.len() < HEADER_SIZE {
        anyhow::bail!("wal file too short");
    }
    let hdr: &WalHeader = bytemuck::from_bytes(&bytes[..HEADER_SIZE]);
    if hdr.magic != MAGIC {
        anyhow::bail!("wal magic mismatch");
    }
    if hdr.record_size as usize != std::mem::size_of::<T>() {
        anyhow::bail!(
            "wal record size {} does not match T size {}",
            hdr.record_size,
            std::mem::size_of::<T>()
        );
    }
    let count = hdr.count as usize;
    let needed = HEADER_SIZE + count * std::mem::size_of::<T>();
    if bytes.len() < needed {
        anyhow::bail!("wal file truncated: header says {} records but file is only {} bytes",
            count, bytes.len());
    }
    let slice = &bytes[HEADER_SIZE..needed];
    let recs: &[T] = bytemuck::cast_slice(slice);
    Ok(recs.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[repr(C)]
    #[derive(Clone, Copy, Debug, PartialEq, Pod, Zeroable)]
    struct R { a: u64, b: u64 }

    #[test]
    fn round_trip_through_grow() {
        let dir = tempdir();
        let p = dir.join("test.wal");
        let mut w: WalWriter<R> = WalWriter::create_with_chunk(&p, 64).unwrap();
        for i in 0..200u64 {
            w.append(&R { a: i, b: i * 3 }).unwrap();
        }
        w.finalize().unwrap();
        let recs: Vec<R> = read_all(&p).unwrap();
        assert_eq!(recs.len(), 200);
        for (i, r) in recs.iter().enumerate() {
            assert_eq!(r.a, i as u64);
            assert_eq!(r.b, (i as u64) * 3);
        }
    }

    fn tempdir() -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!("tp-wal-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }
}
