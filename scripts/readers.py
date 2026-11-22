"""Read an output directory with polars, pyarrow and DuckDB; all must agree."""
import sys

import duckdb
import polars as pl
import pyarrow.parquet as pq

out = sys.argv[1]
for t in ("user_daily", "campaign_daily"):
    a = pl.read_parquet(f"{out}/{t}/*.parquet")
    b = pl.from_arrow(pq.read_table(f"{out}/{t}"))
    c = pl.from_arrow(duckdb.sql(f"select * from read_parquet('{out}/{t}/*.parquet')").arrow())
    key = [x for x in ("user_id", "campaign_id", "day") if x in a.columns]

    def norm(df):
        df = df.with_columns([pl.col(x).cast(pl.Int64) for x in df.columns if df[x].dtype.is_integer()])
        return df.select(sorted(df.columns)).sort(key)

    a, b, c = norm(a), norm(b), norm(c)
    ok = a.equals(b) and a.equals(c)
    print(t, a.height, "rows;", "readers agree" if ok else "READERS DIFFER", dict(a.schema))
    if not ok:
        sys.exit(1)
