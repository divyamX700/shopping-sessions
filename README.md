# Shopping Sessions

A hand-written Rust pipeline for the Builders Buildathon Shopping Sessions track
(`rules.md`, scorer version 5): messy click-stream CSVs in `/data` to two exact
Parquet tables in `/output`. IDs are treated as opaque variable-width strings.

## How it works

1. **Parse** (all threads, memory-mapped inputs in newline-aligned chunks).
   `src/scan.rs` classifies every 64-byte block once (AVX-512 compares when the
   CPU has them, AVX2 otherwise) and splits each line from a 128-bit window of
   newline, comma and quote bits; quote pairing for a whole line is checked
   with one carry-propagating add. Timestamps use a day table for the years
   around the window (`src/tsp.rs`). IDs become exact 64-bit keys
   (`src/keys.rs`): short IDs packed raw, 32-hex IDs by value, anything else
   interned. Events are scattered by a hash of `event_id`; links are joined
   with a union-find on their own thread meanwhile, and clicks are routed by
   person. Parsed chunks give their pages back right away.
2. **Dedup** per `event_id` partition: a bitmap pass finds possible duplicates,
   only those go through a small hash table (smallest `ingest_ts` wins, before
   validation), and the survivors are routed by person.
3. **Sessions** per person partition: counting sort by person, sort by time,
   sessionize, credit and assist with a sliding 7-day click window, aggregate.
4. **Output** (`src/out.rs`): a small Parquet writer. `user_daily` uses
   DELTA_BYTE_ARRAY names, dictionary-encoded small integers and categories,
   and DELTA_BINARY_PACKED revenue: 4x smaller than plain pages, which matters
   because the output's dirty pages are written back when the container exits.

## Checks

`.github/workflows/ci.yml`: unit tests (including scanner fuzzing against the
reference splitter), the 36 fixtures, exact diffs against an independent build
and the official dev answer on generated 1M, 5M and 20M sets, three Parquet
readers (polars, pyarrow, DuckDB), and container timing with the scorer's flags.
