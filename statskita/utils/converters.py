"""Data format converters for StatsKita."""

import subprocess
import time
from pathlib import Path
from typing import Optional, Union


def dbf_to_parquet(
    dbf_path: Union[str, Path],
    parquet_path: Optional[Union[str, Path]] = None,
    force_rebuild: bool = False,
) -> Path:
    """Convert DBF file to Parquet format for faster loading.

    This provides ~100x speedup for subsequent loads (0.5s vs 36s).

    Args:
        dbf_path: Path to input DBF file
        parquet_path: Optional output path (defaults to same name with .parquet)
        force_rebuild: Force conversion even if parquet exists and is newer

    Returns:
        Path to the created Parquet file

    Example:
        >>> # Convert once (takes 36 seconds)
        >>> pq_file = dbf_to_parquet("sak202502_15+_p1.dbf")
        >>>
        >>> # Load instantly thereafter (0.5 seconds)
        >>> df = pl.read_parquet(pq_file)
    """
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
        print(f"Converting {dbf_path.name} to Parquet format...")
        print("This is a one-time operation that will speed up future loads by ~100x")

        # try gdal first (fastest)
        try:
            subprocess.run(
                ["ogr2ogr", "-f", "Parquet", str(parquet_path), str(dbf_path)],
                check=True,
                capture_output=True,
                timeout=60,
            )
            print("Converted using GDAL (fastest method)")
            return parquet_path
        except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
            # gdal not available, use python
            pass

        # fallback to python conversion
        from ..loaders import load_sakernas

        start = time.time()
        print("Converting using Python (this may take 30-60 seconds)...")

        # load dbf
        df = load_sakernas(dbf_path)

        # save as parquet
        df.write_parquet(parquet_path, compression="snappy")

        elapsed = time.time() - start
        size_reduction = (
            (dbf_path.stat().st_size - parquet_path.stat().st_size) / dbf_path.stat().st_size * 100
        )

        print(f"Converted in {elapsed:.1f}s")
        print(f"File size reduced by {size_reduction:.0f}%")
    else:
        print(f"Using existing Parquet file: {parquet_path.name}")

    return parquet_path


def batch_convert_dbf_to_parquet(directory: Union[str, Path], pattern: str = "*.dbf") -> list[Path]:
    """Convert all DBF files in a directory to Parquet.

    Args:
        directory: Directory containing DBF files
        pattern: Glob pattern for DBF files (default: "*.dbf")

    Returns:
        List of created Parquet file paths
    """
    directory = Path(directory)
    parquet_files = []

    for dbf_file in directory.glob(pattern):
        try:
            pq_file = dbf_to_parquet(dbf_file)
            parquet_files.append(pq_file)
        except Exception as e:
            print(f"⚠️ Failed to convert {dbf_file.name}: {e}")

    return parquet_files
