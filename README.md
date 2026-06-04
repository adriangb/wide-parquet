# wide-parquet

Write Parquet with many heterogeneous columns efficiently, with Rust.

A small example of the Parquet [`PageStore`] API: a **spilling** page store that
keeps completed Parquet pages in temp files instead of buffering them on the
heap, so peak write memory stays bounded even for wide, skewed schemas.

## Background

Parquet requires every column chunk to be contiguous in the file, but Arrow
record batches arrive with all columns interleaved. So while a row group is
being written, the `ArrowWriter` must buffer every column's completed pages
until the row group is flushed — peak write memory therefore grows with the row
group size. That hurts most on **wide, skewed** schemas (a few `id` columns next
to a pile of fat string columns).

A `PageStore` lets that page buffer live somewhere other than the heap. This
example implements a `TempFilePageStore` (one temp file per column chunk) and
reports the writer's peak heap memory so you can compare the default in-memory
buffering against spilling.

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
`--rows`, `--batch-size`, `--spill`, `--output <path>`.

### Example output

```text
$ cargo run --release
Writing 2048 rows × 18 columns (3 int, 5 small-string ~20B, 10 large-string ~8192B)
Page buffering: InMemoryPageStore (default, on the heap)  (large-column payload ≈ 160.0 MiB)

Done. Wrote 2048 rows.
Peak ArrowWriter::memory_size():    161.0 MiB   <- bytes the writer held on the heap
...

$ cargo run --release -- --spill
Page buffering: TempFilePageStore (spilling to temp files)  (large-column payload ≈ 160.0 MiB)

Done. Wrote 2048 rows.
Peak ArrowWriter::memory_size():     11.0 MiB   <- bytes the writer held on the heap
Spilled 336 pages (150.1 MiB) to temp files.
```

i.e. spilling cuts peak writer memory from ~161 MiB (the whole row group buffered
on the heap) to ~11 MiB (just the in-flight encoder buffers), and the output file
is byte-identical to the in-memory path.

## Dependency pinning

The `PageStore` API is not yet in a published `parquet` release, so `Cargo.toml`
pins `parquet` (and the matching `arrow` crates) to the
[apache/arrow-rs](https://github.com/apache/arrow-rs) commit that merged it
([#10020](https://github.com/apache/arrow-rs/pull/10020)). Once it ships in a
crates.io release, switch the git dependencies to a version requirement.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).

[`PageStore`]: https://github.com/apache/arrow-rs/blob/main/parquet/src/column/page_store.rs
