"""Data format converters for StatsKita."""

import time
from pathlib import Path
from typing import Optional, Union


def dbf_to_parquet(
    dbf_path: Union[str, Path],
    parquet_path: Optional[Union[str, Path]] = None,
    force_rebuild: bool = False,
) -> Path:
    """Convert DBF file to Parquet format for faster loading.

    Uses dbfrs library to preserve all data types including decimal fields.
    Provides ~100x speedup for subsequent loads (0.5s vs 36s).

    Args:
        dbf_path: Path to input DBF file
        parquet_path: Optional output path (defaults to same name with .parquet)
        force_rebuild: Force conversion even if parquet exists and is newer

    Returns:
        Path to the created Parquet file

    Example:
        >>> pq_file = dbf_to_parquet("data.dbf")
        >>> df = pl.read_parquet(pq_file)
    """
    import dbfrs
    import polars as pl

    dbf_path = Path(dbf_path)
    if not dbf_path.exists():
        raise FileNotFoundError(f"DBF file not found: {dbf_path}")

    # default output path
    if parquet_path is None:
        parquet_path = dbf_path.with_suffix(".parquet")
    else:
        parquet_path = Path(parquet_path)

    # check if conversion needed
    need_conversion = (
        force_rebuild
        or not parquet_path.exists()
        or parquet_path.stat().st_mtime < dbf_path.stat().st_mtime
    )

    if need_conversion:
        print(f"Converting {dbf_path.name}...")
        start = time.time()

        try:
            # try dbfrs first (preserves all data types)
            fields = dbfrs.get_dbf_fields(str(dbf_path))
            field_names = [f.name for f in fields]
            data = dbfrs.load_dbf(str(dbf_path))

            # convert to polars
            df = pl.DataFrame(data, schema=field_names, orient="row")
            method = "dbfrs"
        except Exception:
            # fallback to sakernas loader (handles edge cases)
            from ..loaders import load_sakernas

            df = load_sakernas(dbf_path)
            method = "loader"

        # save as parquet
        df.write_parquet(parquet_path, compression="snappy")

        elapsed = time.time() - start
        size_reduction = (
            (dbf_path.stat().st_size - parquet_path.stat().st_size) / dbf_path.stat().st_size * 100
        )

        print(f"Converted in {elapsed:.1f}s ({size_reduction:.0f}% smaller, {method})")
    else:
        print(f"Using cached: {parquet_path.name}")

    return parquet_path


def dta_to_parquet(
    dta_path: Union[str, Path],
    parquet_path: Optional[Union[str, Path]] = None,
    force_rebuild: bool = False,
) -> Path:
    """Convert Stata DTA file to Parquet format for faster loading.

    This provides ~400x speedup for subsequent loads.

    Args:
        dta_path: Path to input DTA file
        parquet_path: Optional output path (defaults to same name with .parquet)
        force_rebuild: Force conversion even if parquet exists and is newer

    Returns:
        Path to the created Parquet file

    Example:
        >>> pq_file = dta_to_parquet("data.dta")
        >>> df = pl.read_parquet(pq_file)
    """
    import polars as pl
    import pyreadstat

    dta_path = Path(dta_path)
    if not dta_path.exists():
        raise FileNotFoundError(f"DTA file not found: {dta_path}")

    # default output path
    if parquet_path is None:
        parquet_path = dta_path.with_suffix(".parquet")
    else:
        parquet_path = Path(parquet_path)

    # check if conversion needed
    need_conversion = (
        force_rebuild
        or not parquet_path.exists()
        or parquet_path.stat().st_mtime < dta_path.stat().st_mtime
    )

    if need_conversion:
        print(f"Converting {dta_path.name}...")
        start = time.time()

        # load stata file
        df_pd, meta = pyreadstat.read_dta(str(dta_path))
        df = pl.from_pandas(df_pd)

        # save as parquet
        df.write_parquet(parquet_path, compression="snappy")

        elapsed = time.time() - start
        size_reduction = (
            (dta_path.stat().st_size - parquet_path.stat().st_size) / dta_path.stat().st_size * 100
        )

        original_mb = dta_path.stat().st_size / (1024 * 1024)
        parquet_mb = parquet_path.stat().st_size / (1024 * 1024)
        print(
            f"Converted in {elapsed:.1f}s ({size_reduction:.0f}% smaller, {original_mb:.1f} MB to {parquet_mb:.1f} MB)"
        )
    else:
        print(f"Using cached: {parquet_path.name}")

    return parquet_path
