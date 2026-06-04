//! Demonstrates the Parquet [`PageStore`] API with a **spilling** backend that
//! keeps completed Parquet pages in temp files instead of buffering them on the
//! heap.
//!
//! See [README.md] for more details

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringViewBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use clap::Parser;
use parquet::arrow::arrow_writer::{
    ArrowWriterOptions, PageKey, PageStore, PageStoreArgs, PageStoreFactory,
};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::errors::Result;
use parquet::file::properties::WriterProperties;

/// Write a wide, skewed Parquet file and report the writer's peak heap memory,
/// with or without a spilling `PageStore`.
#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// Number of large (~16 KiB) string columns.
    #[arg(long, default_value_t = 10)]
    large_string_columns: usize,

    /// Number of small (~20 byte) string columns.
    #[arg(long, default_value_t = 5)]
    small_string_columns: usize,

    /// Number of `Int64` columns.
    #[arg(long, default_value_t = 3)]
    int_columns: usize,

    /// Total number of rows, all written into a single row group.
    #[arg(long, default_value_t = 8192)]
    rows: usize,

    /// Spill completed pages to temp files instead of buffering them.
    #[arg(long)]
    spill: bool,
}

/// Length, in bytes, of values in a "large" string column (16 KiB per row).
const LARGE_AVG_LEN: usize = 16 * 1024;
/// Length, in bytes, of values in a "small" string column.
const SMALL_AVG_LEN: usize = 20;
/// Rows per input batch fed to the writer
const BATCH_SIZE: usize = 4096;

/// Running totals of what was spilled, shared across the per-column stores.
#[derive(Debug, Default)]
struct SpillStats {
    pages: AtomicUsize,
    bytes: AtomicU64,
}

/// A spilling [`PageStore`]: one temp file per column chunk.
///
/// `put` appends the bytes to the file and records its `(offset, len)`; `take`
/// seeks and reads it back. The file is unlinked on creation (via
/// [`tempfile::tempfile`]) so the OS reclaims it when the store is dropped.
struct TempFilePageStore {
    file: File,
    /// Logical end of the file — where the next `put` appends.
    end: u64,
    /// `(offset, len)` for each stored blob, indexed by the `PageKey`
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
        // Always append at the logical end
        self.file.seek(SeekFrom::Start(self.end))?;
        self.file.write_all(&value)?;
        self.stats.pages.fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes
            .fetch_add(value.len() as u64, Ordering::Relaxed);
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

fn main() -> Result<()> {
    let start = Instant::now();
    let args = Args::parse();
    let schema = build_schema(&args);

    // One uncompressed row group for the whole dataset
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
        "Writing {} rows × {} columns ({} int, {} small-string ~{}B, {} large-string ~{} KiB)",
        args.rows,
        args.int_columns + args.small_string_columns + args.large_string_columns,
        args.int_columns,
        args.small_string_columns,
        SMALL_AVG_LEN,
        args.large_string_columns,
        LARGE_AVG_LEN / 1024,
    );
    println!(
        "{:<31}: {}",
        "Page buffering",
        if args.spill {
            "TempFilePageStore (spilling to temp files)"
        } else {
            "InMemoryPageStore (default, on the heap)"
        },
    );

    // Throw away all output since we're just measuring memory, not the file
    // size or contents.
    let sink = std::io::sink();
    let mut writer = ArrowWriter::try_new_with_options(sink, schema.clone(), options)?;

    let mut peak_memory = 0usize;
    let mut written = 0usize;
    while written < args.rows {
        let n = BATCH_SIZE.min(args.rows - written);
        let batch = make_batch(&schema, &args, written as u64, n);
        writer.write(&batch)?;
        written += n;
        // `memory_size()` reports the bytes the writer holds on the heap
        peak_memory = peak_memory.max(writer.memory_size());
    }
    peak_memory = peak_memory.max(writer.memory_size());
    writer.close()?;
    let elapsed = start.elapsed();

    println!("{:<31}: {written} rows", "Rows written");
    println!(
        "{:<31}: {:.1} MiB   <- bytes the writer held on the heap",
        "Peak ArrowWriter::memory_size()",
        mib(peak_memory),
    );
    println!(
        "{:<31}: {:.3} s",
        "Total elapsed time",
        elapsed.as_secs_f64(),
    );
    if args.spill {
        println!(
            "{:<31}: {} pages ({:.1} MiB)",
            "Spilled to temp file",
            stats.pages.load(Ordering::Relaxed),
            mib(stats.bytes.load(Ordering::Relaxed) as usize),
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
        fields.push(Field::new(
            format!("small_str_{i}"),
            DataType::Utf8View,
            false,
        ));
    }
    for i in 0..args.large_string_columns {
        fields.push(Field::new(
            format!("large_str_{i}"),
            DataType::Utf8View,
            false,
        ));
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
    let mut builder = StringViewBuilder::with_capacity(rows);
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
        let vals: Vec<i64> = (0..rows)
            .map(|r| (row_offset + r as u64 + s) as i64)
            .collect();
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
