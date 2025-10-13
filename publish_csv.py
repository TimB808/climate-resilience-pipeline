import os
import glob
import argparse
import re
import pandas as pd
import numpy as np

from config import (
    PROC_ANNUAL_TEMP_DIR,
    PUBLISH_CSV,
    PUBLISH_SUCCESS_MARK,
    PARQUET_ENGINE,
)


REQUIRED_COLUMNS = {"country", "year", "temp_c"}


def ensure_dir(path):
    directory = path if os.path.splitext(path)[1] == "" else os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)


def atomic_write_csv(df: pd.DataFrame, dst_path: str) -> None:
    ensure_dir(dst_path)
    tmp_path = f"{dst_path}.tmp"
    df.to_csv(tmp_path, index=False)
    os.replace(tmp_path, dst_path)


def discover_partitions(base_dir: str) -> list:
    pattern = os.path.join(base_dir, "year=*", "part.parquet")
    return sorted(glob.glob(pattern))


def extract_year_from_path(path: str) -> int | None:
    # expects .../year=YYYY/part.parquet
    m = re.search(r"year=(\d{4})", path)
    return int(m.group(1)) if m else None


def validate_parquet_schema(parquet_path: str) -> None:
    try:
        import pyarrow.parquet as pq
    except Exception:
        # Fall back: read a small portion with pandas
        df = pd.read_parquet(parquet_path, engine=PARQUET_ENGINE)
        cols = set(df.columns)
    else:
        pf = pq.ParquetFile(parquet_path)
        cols = set(pf.schema.names)

    missing = REQUIRED_COLUMNS - cols
    unexpected = cols - REQUIRED_COLUMNS
    if missing or unexpected:
        raise ValueError(
            f"Schema mismatch in {parquet_path}: missing={sorted(missing)} unexpected={sorted(unexpected)}"
        )


def load_partitions(paths: list, min_year: int | None, max_year: int | None) -> pd.DataFrame:
    frames = []
    for p in paths:
        year = extract_year_from_path(p)
        if year is None:
            continue
        if min_year is not None and year < min_year:
            continue
        if max_year is not None and year > max_year:
            continue
        validate_parquet_schema(p)
        df = pd.read_parquet(p, engine=PARQUET_ENGINE, columns=["country", "year", "temp_c"])  # type: ignore[arg-type]
        # enforce dtypes
        df["country"] = df["country"].astype(str)
        df["year"] = df["year"].astype(int)
        df["temp_c"] = df["temp_c"].astype(float)
        frames.append(df)

    if not frames:
        return pd.DataFrame(columns=["country", "year", "temp_c"])  # empty

    df_all = pd.concat(frames, ignore_index=True)
    return df_all


def publish(min_year: int | None, max_year: int | None, write_success_marker: bool) -> tuple[int, int, int, int | None, int | None]:
    paths = discover_partitions(PROC_ANNUAL_TEMP_DIR)
    if not paths:
        raise FileNotFoundError(f"No partitions found under {PROC_ANNUAL_TEMP_DIR}")

    df = load_partitions(paths, min_year, max_year)
    if df.empty:
        raise ValueError("No rows to publish after filtering.")

    # sort, drop duplicates
    df = df.sort_values(["country", "year"]).drop_duplicates(subset=["country", "year"], keep="first")

    # write CSV atomically
    atomic_write_csv(df, PUBLISH_CSV)

    # optional success marker
    if write_success_marker:
        ensure_dir(PUBLISH_SUCCESS_MARK)
        # Remove if it's a directory (from previous runs)
        if os.path.isdir(PUBLISH_SUCCESS_MARK):
            import shutil
            shutil.rmtree(PUBLISH_SUCCESS_MARK)
        with open(PUBLISH_SUCCESS_MARK, "w") as f:
            f.write("ok\n")

    n_rows = len(df)
    n_countries = df["country"].nunique()
    y_min = int(df["year"].min()) if n_rows > 0 else None
    y_max = int(df["year"].max()) if n_rows > 0 else None

    # number of partitions included
    included_years = sorted(set(df["year"].tolist()))
    n_partitions = len(included_years)
    return n_partitions, n_countries, n_rows, y_min, y_max


def main():
    parser = argparse.ArgumentParser(description="Publish annual temperature CSV from Parquet partitions")
    parser.add_argument("--min-year", type=int, default=None, help="Minimum year to include")
    parser.add_argument("--max-year", type=int, default=None, help="Maximum year to include")
    parser.add_argument("--no-success-marker", action="store_true", help="Do not write _SUCCESS marker")
    args = parser.parse_args()

    n_parts, n_countries, n_rows, y_min, y_max = publish(
        min_year=args.min_year,
        max_year=args.max_year,
        write_success_marker=not args.no_success_marker,
    )

    print(
        f"Published CSV to {PUBLISH_CSV} | Years: {y_min}..{y_max} | Countries: {n_countries} | Rows: {n_rows} | Partitions: {n_parts}"
    )


if __name__ == "__main__":
    main()
