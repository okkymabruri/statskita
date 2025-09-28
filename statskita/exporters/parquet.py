"""Export to Parquet format for high-performance analytics."""

from pathlib import Path
from typing import Union

import polars as pl


def export_parquet(
    df: pl.DataFrame, file_path: Union[str, Path], compression: str = "snappy", **kwargs
) -> None:
    """Export polars DataFrame to Parquet format.

    Args:
        df: Polars DataFrame to export
        file_path: Output file path
        compression: Compression algorithm ("snappy", "gzip", "brotli", "lz4", "zstd")
        **kwargs: Additional arguments for polars write_parquet
    """
    try:
        df.write_parquet(file=str(file_path), compression=compression, **kwargs)
        print(f"Data exported to Parquet: {file_path}")

    except Exception as e:
        raise RuntimeError(f"Parquet export failed: {e}")


def export_parquet_partitioned(
    df: pl.DataFrame,
    output_dir: Union[str, Path],
    partition_cols: list[str],
    compression: str = "snappy",
    **kwargs,
) -> None:
    """Export DataFrame as partitioned Parquet files.

    Args:
        df: Polars DataFrame to export
        output_dir: Output directory for partitioned files
        partition_cols: Columns to partition by
        compression: Compression algorithm
        **kwargs: Additional arguments
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    try:
        # group by partition columns and save separate files
        for partition_values, group in df.group_by(partition_cols):
            if isinstance(partition_values, tuple):
                partition_str = "_".join(
                    f"{col}={val}" for col, val in zip(partition_cols, partition_values)
                )
            else:
                partition_str = f"{partition_cols[0]}={partition_values}"

            file_path = output_path / f"{partition_str}.parquet"
            group.write_parquet(file=str(file_path), compression=compression, **kwargs)

        print(f"Partitioned data exported to: {output_dir}")

    except Exception as e:
        raise RuntimeError(f"Partitioned Parquet export failed: {e}")
