//! Demonstrates the Parquet [`PageStore`] API with a **spilling** backend that
//! keeps completed Parquet pages in temp files instead of buffering them on the
//! heap.
//!
//! # Why
//!
//! Parquet requires every column chunk to be contiguous in the file, but Arrow
//! record batches arrive with all columns interleaved. So while a row group is
//! being written, [`ArrowWriter`] must buffer every column's completed pages
//! until the row group is flushed — peak write memory therefore grows with the
//! row group size. That is painful for wide schemas with large, skewed columns
//! (a few `id` columns next to a pile of fat string columns).
//!
//! A [`PageStore`] lets that page buffer live somewhere other than the heap.
//! This example plugs in a [`TempFilePageStore`] (one temp file per column
//! chunk) and reports the writer's peak heap memory, so you can compare the
//! default in-memory buffering against spilling.
//!
//! # Running
//!
//! ```sh
//! # Baseline: default in-memory page buffering. Peak writer memory grows with
//! # the row group.
//! cargo run --release
//!
//! # Spill completed pages to temp files: peak writer memory stays bounded.
//! cargo run --release -- --spill
//!
//! # Make the schema wider / the skew worse:
//! cargo run --release -- --spill --large-string-columns 40
//! ```
//!
//! [`ArrowWriter`]: parquet::arrow::ArrowWriter
//! [`PageStore`]: parquet::arrow::arrow_writer::PageStore

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use clap::Parser;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_writer::{
    ArrowWriterOptions, PageKey, PageStore, PageStoreArgs, PageStoreFactory,
};
use parquet::basic::Compression;
use parquet::errors::Result;
use parquet::file::properties::WriterProperties;

/// Write a wide, skewed Parquet file and report the writer's peak heap memory,
/// with or without a spilling `PageStore`.
#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// Number of large (~8 KiB) string columns — the fat columns that make the
    /// in-memory page buffer blow up.
    #[arg(long, default_value_t = 10)]
    large_string_columns: usize,

    /// Number of small (~20 byte) string columns.
    #[arg(long, default_value_t = 5)]
    small_string_columns: usize,

    /// Number of `Int64` columns.
    #[arg(long, default_value_t = 3)]
    int_columns: usize,

    /// Total number of rows, all written into a single row group.
    #[arg(long, default_value_t = 2048)]
    rows: usize,

    /// Rows per input batch fed to the writer.
    #[arg(long, default_value_t = 256)]
    batch_size: usize,

    /// Spill completed pages to temp files instead of buffering them on the heap.
    #[arg(long)]
    spill: bool,

    /// Optional path to write the Parquet file to. Defaults to a sink so the
    /// produced file bytes never live on the heap and the reported memory
    /// reflects only the writer's page buffering.
    #[arg(long)]
    output: Option<PathBuf>,
}

/// Average length, in bytes, of values in a "large" string column.
const LARGE_AVG_LEN: usize = 8 * 1024;
/// Average length, in bytes, of values in a "small" string column.
const SMALL_AVG_LEN: usize = 20;

// ---------------------------------------------------------------------------
// The spilling page store.
//
// A `PageStore` is intentionally "dumb": it maps an opaque, store-allocated
// `PageKey` to a blob of bytes and knows nothing about pages, dictionaries, or
// ordering. The caller keeps the handles and decides what they mean. That is all
// a backend has to implement to move the page buffer off the heap.
// ---------------------------------------------------------------------------

/// Running totals of what was spilled, shared across the per-column stores.
#[derive(Debug, Default)]
struct SpillStats {
    pages: AtomicUsize,
    bytes: AtomicU64,
}

/// A spilling [`PageStore`]: one temp file per column chunk. `put` appends the
/// page blob and records its `(offset, len)`; `take` seeks and reads it back.
/// The file is unlinked on creation (via [`tempfile::tempfile`]) so the OS
/// reclaims it when the store is dropped.
struct TempFilePageStore {
    file: File,
    /// Logical end of the file — where the next `put` appends.
    end: u64,
    /// `(offset, len)` for each stored blob, indexed by the `PageKey` we minted.
    locs: Vec<(u64, usize)>,
    stats: Arc<SpillStats>,
}

impl TempFilePageStore {
    fn new(stats: Arc<SpillStats>) -> Result<Self> {
        Ok(Self {
            file: tempfile::tempfile()?,
            end: 0,
            locs: Vec::new(),
            stats,
        })
    }
}

impl PageStore for TempFilePageStore {
    fn put(&mut self, value: Bytes) -> Result<PageKey> {
        // Always append at the logical end (a prior `take` may have moved the
        // OS file cursor).
        self.file.seek(SeekFrom::Start(self.end))?;
        self.file.write_all(&value)?;
        self.stats.pages.fetch_add(1, Ordering::Relaxed);
        self.stats.bytes.fetch_add(value.len() as u64, Ordering::Relaxed);
        let key = PageKey::new(self.locs.len() as u64);
        self.locs.push((self.end, value.len()));
        self.end += value.len() as u64;
        Ok(key)
    }

    fn take(&mut self, key: PageKey) -> Result<Bytes> {
        let (offset, len) = self.locs[key.get() as usize];
        let mut buf = vec![0u8; len];
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(&mut buf)?;
        Ok(Bytes::from(buf))
    }

    // `memory_size` keeps its default of 0: once a blob is handed to `put` it
    // lives in the temp file, not on the heap. That zero is what makes
    // `ArrowWriter::memory_size()` drop once pages are spilled.
}

/// Creates a fresh [`TempFilePageStore`] for each column chunk the writer opens.
#[derive(Debug)]
struct TempFilePageStoreFactory {
    stats: Arc<SpillStats>,
}

impl PageStoreFactory for TempFilePageStoreFactory {
    fn create(&self, _args: &PageStoreArgs<'_>) -> Result<Box<dyn PageStore>> {
        Ok(Box::new(TempFilePageStore::new(self.stats.clone())?))
    }
}

// ---------------------------------------------------------------------------
// Schema + deterministic data generation.
// ---------------------------------------------------------------------------

/// Build the wide, skewed schema: a few integer columns, then small string
/// columns, then the fat large string columns.
fn build_schema(args: &Args) -> SchemaRef {
    let mut fields = Vec::new();
    for i in 0..args.int_columns {
        fields.push(Field::new(format!("int_{i}"), DataType::Int64, false));
    }
    for i in 0..args.small_string_columns {
        fields.push(Field::new(format!("small_str_{i}"), DataType::Utf8, false));
    }
    for i in 0..args.large_string_columns {
        fields.push(Field::new(format!("large_str_{i}"), DataType::Utf8, false));
    }
    Arc::new(Schema::new(fields))
}

/// Fill `buf` with a deterministic value of exactly `len` bytes derived from the
/// counter `n`. The 20-digit zero-padded counter makes every value distinct, so
/// the fat columns stay plain-encoded (high cardinality) rather than
/// dictionary-encoding away; the remainder is padded with a fixed `a`–`z` cycle.
fn fill_value(buf: &mut String, n: u64, len: usize) {
    use std::fmt::Write;
    buf.clear();
    let _ = write!(buf, "{n:020}");
    while buf.len() < len {
        buf.push((b'a' + (buf.len() % 26) as u8) as char);
    }
    buf.truncate(len); // all bytes are ASCII, so this is a clean char boundary
}

/// Build a string column of `rows` values, each exactly `len` bytes, keyed by
/// the global row index and a per-column `salt` so values are distinct.
fn make_string_array(rows: usize, row_offset: u64, salt: u64, len: usize) -> ArrayRef {
    let mut builder = StringBuilder::with_capacity(rows, rows * len);
    let mut value = String::new();
    for r in 0..rows {
        let n = (row_offset + r as u64).wrapping_mul(101).wrapping_add(salt);
        fill_value(&mut value, n, len);
        builder.append_value(&value);
    }
    Arc::new(builder.finish())
}

/// Build the record batch covering rows `[row_offset, row_offset + rows)`.
fn make_batch(schema: &SchemaRef, args: &Args, row_offset: u64, rows: usize) -> RecordBatch {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    let mut salt = 0u64; // distinguishes columns so they don't all hold equal values
    for _ in 0..args.int_columns {
        let s = salt;
        salt += 1;
        let vals: Vec<i64> = (0..rows).map(|r| (row_offset + r as u64 + s) as i64).collect();
        columns.push(Arc::new(Int64Array::from(vals)));
    }
    for _ in 0..args.small_string_columns {
        columns.push(make_string_array(rows, row_offset, salt, SMALL_AVG_LEN));
        salt += 1;
    }
    for _ in 0..args.large_string_columns {
        columns.push(make_string_array(rows, row_offset, salt, LARGE_AVG_LEN));
        salt += 1;
    }
    RecordBatch::try_new(schema.clone(), columns).unwrap()
}

fn mib(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn main() -> Result<()> {
    let start = Instant::now();
    let args = Args::parse();
    let schema = build_schema(&args);

    // One uncompressed row group for the whole dataset, so the page buffer (the
    // thing a PageStore governs) is the only thing that grows.
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_max_row_group_row_count(Some(args.rows * 2))
        .build();

    let stats = Arc::new(SpillStats::default());
    let mut options = ArrowWriterOptions::new().with_properties(props);
    if args.spill {
        options = options.with_page_store_factory(Arc::new(TempFilePageStoreFactory {
            stats: stats.clone(),
        }));
    }

    // Total logical payload across the large columns — the part that dominates.
    let large_payload = args.large_string_columns * LARGE_AVG_LEN * args.rows;
    println!(
        "Writing {} rows × {} columns ({} int, {} small-string ~{}B, {} large-string ~{}B)",
        args.rows,
        args.int_columns + args.small_string_columns + args.large_string_columns,
        args.int_columns,
        args.small_string_columns,
        SMALL_AVG_LEN,
        args.large_string_columns,
        LARGE_AVG_LEN,
    );
    println!(
        "Page buffering: {}  (large-column payload ≈ {:.1} MiB)",
        if args.spill {
            "TempFilePageStore (spilling to temp files)"
        } else {
            "InMemoryPageStore (default, on the heap)"
        },
        mib(large_payload),
    );

    // The output sink. A sink discards the file bytes so they never inflate the
    // heap — the measured peak then reflects only the writer's page buffering.
    let sink: Box<dyn Write + Send> = match &args.output {
        Some(path) => Box::new(File::create(path)?),
        None => Box::new(std::io::sink()),
    };
    let mut writer = ArrowWriter::try_new_with_options(sink, schema.clone(), options)?;

    let mut peak_memory = 0usize;
    let mut written = 0usize;
    while written < args.rows {
        let n = args.batch_size.min(args.rows - written);
        let batch = make_batch(&schema, &args, written as u64, n);
        writer.write(&batch)?;
        written += n;
        // `memory_size()` reports the bytes the writer holds resident on the
        // heap: with the in-memory store this climbs toward the whole row group;
        // with the spilling store it stays flat.
        peak_memory = peak_memory.max(writer.memory_size());
    }
    peak_memory = peak_memory.max(writer.memory_size());
    writer.close()?;
    let elapsed = start.elapsed();

    println!();
    println!("Done. Wrote {written} rows.");
    println!(
        "Peak ArrowWriter::memory_size(): {:>8.1} MiB   <- bytes the writer held on the heap",
        mib(peak_memory),
    );
    println!(
        "Total elapsed time             : {:>8.3} s",
        elapsed.as_secs_f64(),
    );
    if args.spill {
        println!(
            "Spilled {} pages ({:.1} MiB) to temp files.",
            stats.pages.load(Ordering::Relaxed),
            mib(stats.bytes.load(Ordering::Relaxed) as usize),
        );
        println!();
        println!(
            "With spilling, peak writer memory is bounded by the in-flight encoder \n\
             buffers, not the {:.1} MiB row group payload.",
            mib(large_payload),
        );
    } else {
        println!();
        println!(
            "Re-run with --spill to keep those pages off the heap and watch peak \n\
             writer memory drop well below the {:.1} MiB row group payload.",
            mib(large_payload),
        );
    }

    Ok(())
}
