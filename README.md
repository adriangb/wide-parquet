# wide-parquet

Write Parquet with Many Heterogeneous columns efficiently, with Rust.

This example shows how to use the Rust [`parquet crate`] to write wide tables
(1000s of columns) and large strings (1MB each row) with limited memory. 

The example reports the peak memory buffered by the underlying ArrowWriter. You
can also run using a `--spill` argument which will write buffered pages to
temporary files instead.


## Background

The nature of Parquet is that data pages for a particular column chunk (the rows
for a column within a row group) must be contiguous, meaning that the row group
encoding must complete before the final bytes can be written

By default, the [arrow-rs] Parquet writer, like many other parqet writer
implementations, buffers the entire (compressed) row group in RAM before writing
out to storage. While efficient, this can buffer very large amounts of data for
wide columns or columns with large (e.g. string / image) values.


## PageStore

To avoid buffering the entire row group in memory, the Parquet writer can be
configured to use a [`PageStore`] for buffering the encoded pages. This example
writes encoded pages to temp files before writing the final Parquet file, but a
`PageStore` could also be used to buffer pages in memory, write to a remote
store, or dynamic spilling to disk when a memory threshold is exceeded, and
more.

Nothing comes for free, of course, and using a PageStore results in writing the
bytes one extra time -- both to and from the page store, though the bytes
are efficiently encoded Parquet data pages, not the original input data.

## Running

```sh
# Baseline: default in-memory page buffering. Peak writer memory grows with the
# row group.
cargo run --release

# Spill completed pages to temp files: peak writer memory stays bounded.
cargo run --release -- --spill

# Make the schema wider / the skew worse:
cargo run --release -- --spill --large-string-columns 40
```

Flags: `--large-string-columns`, `--small-string-columns`, `--int-columns`,
`--rows`, `--spill`.

### Example output

```text
$ cargo run --release
Writing 64 rows × 18 columns (3 int, 5 small-string ~20B, 10 large-string ~1 MiB)
Page buffering: InMemoryPageStore (default, on the heap)  (large-column payload ≈ 640.0 MiB)

Done. Wrote 64 rows.
Peak ArrowWriter::memory_size():    640.3 MiB   <- bytes the writer held on the heap
...

$ cargo run --release -- --spill
Page buffering: TempFilePageStore (spilling to temp files)  (large-column payload ≈ 640.0 MiB)

Done. Wrote 64 rows.
Peak ArrowWriter::memory_size():     10.2 MiB   <- bytes the writer held on the heap
Spilled 1296 pages (630.0 MiB) to temp files.
```

i.e. spilling cuts peak writer memory from ~640 MiB (the whole row group buffered
on the heap) to ~10 MiB (just the in-flight encoder buffers).

## Dependency pinning

The `PageStore` API is not yet in a published `parquet` release, so `Cargo.toml`
pins `parquet` (and the matching `arrow` crates) to the
[apache/arrow-rs](https://github.com/apache/arrow-rs) commit that merged it
([#10020](https://github.com/apache/arrow-rs/pull/10020)). Once it ships in a
crates.io release, switch the git dependencies to a version requirement.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).

[`parquet crate`]: https://docs.rs/parquet
[arrow-rs]: https://github.com/apache/arrow-rs
[`PageStore`]: https://github.com/apache/arrow-rs/blob/main/parquet/src/column/page_store.rs
