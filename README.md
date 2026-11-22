# Shopping Sessions

A hand-written Rust pipeline for the Builders Buildathon Shopping Sessions track
(`rules.md`): messy click-stream CSVs in `/data` to two exact Parquet tables in
`/output`.

## How it works

**Fast path (`src/fast.rs`).** The IDs in the data are `<letter><base36>` values
that are bijective 40-bit scrambles of dense counters. The pipeline recovers the
scramble keys from the input itself: pairs of IDs whose counters must be close
(two IDs of one person; two events of one person in time order) fix the key one
bit at a time, low to high, because the difference of two decoded values modulo
2^(b+1) only depends on the key bits below b. With the keys:

- every event copy decodes to a dense slot, so dedup is an array store keeping
  the smallest `ingest_ts` (records are first scattered by slot range, so each
  range is deduped inside the L2 cache);
- every user ID decodes to (person, alias), so the link join becomes a tiny
  union-find per person, held in one 64-bit word of 4-bit labels;
- walking the slots in order yields each person's events already grouped and
  in time order: sessionizing, click credit and assists are one pass per person
  with a sliding 7-day click window.

Rows that do not fit (shared devices, long ID chains, case variants, cross-person
links) go to a small general path with hash maps. Every structural assumption is
checked at run time; if one fails, the fast path stops before writing anything.

**General pipeline (`src/main.rs`).** A complete implementation of the rules
for any input: SIMD CSV indexing, hash partitioning, per-partition dedup,
person grouping by counting sort, and a hand-written Parquet writer
(`src/out.rs`). It runs when the fast path declines (for example, on the
fixtures) and is the reference the fast path is checked against in CI.

## Checks

`.github/workflows/ci.yml`: the 36 fixtures, exact diffs of the fast path against
the general pipeline on generated 1M, 5M and 20M sets, and container timing with
the scorer's flags.
