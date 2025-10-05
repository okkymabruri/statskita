#!/usr/bin/env python3
"""
Convert BPS survey data files (.dta, .dbf, .sav) to Parquet format for faster loading.

Usage:
    python scripts/convert_to_parquet.py sakernas
    python scripts/convert_to_parquet.py susenas
    python scripts/convert_to_parquet.py all

Requires .env file with paths:
    SAKERNAS_DATA_DIR=/path/to/bps-sakernas
    SAKERNAS_PARQUET_DIR=/path/to/bps-sakernas-pq
    SUSENAS_DATA_DIR=/path/to/bps-susenas
    SUSENAS_PARQUET_DIR=/path/to/bps-susenas-pq
"""

import os
import sys
import time
from pathlib import Path
from typing import Optional

import polars as pl
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))

from statskita.utils.converters import dbf_to_parquet, dta_to_parquet


def convert_file(input_path: Path, output_path: Path, force_rebuild: bool = False) -> Path:
    """Convert data file to parquet format."""
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    suffix = input_path.suffix.lower()

    if suffix == ".dbf":
        return dbf_to_parquet(input_path, output_path, force_rebuild)
    elif suffix == ".dta":
        return dta_to_parquet(input_path, output_path, force_rebuild)
    elif suffix == ".sav":
        import pyreadstat

        need_conversion = (
            force_rebuild
            or not output_path.exists()
            or output_path.stat().st_mtime < input_path.stat().st_mtime
        )

        if need_conversion:
            print(f"Converting {input_path.name}...")
            start = time.time()

            df_pd, meta = pyreadstat.read_sav(str(input_path))
            df = pl.from_pandas(df_pd)
            df.write_parquet(output_path, compression="snappy")

            elapsed = time.time() - start
            size_reduction = (
                (input_path.stat().st_size - output_path.stat().st_size)
                / input_path.stat().st_size
                * 100
            )
            print(f"Converted in {elapsed:.1f}s ({size_reduction:.0f}% smaller)")
        else:
            print(f"Using cached: {output_path.name}")

        return output_path
    elif suffix == ".parquet":
        return input_path
    else:
        raise ValueError(f"Unsupported file format: {suffix}")



# combine multi-part files
def combine_parts(
    data_dir: Path, year_month: str, output_dir: Path, dataset: str
) -> Optional[Path]:
    """Combine multi-part files into single parquet."""
    year_month_clean = year_month.replace("-", "")

    patterns = [
        f"{dataset[:3]}{year_month_clean}*p1.*",
        f"{dataset[:3]}{year_month_clean}*p2.*",
        f"{dataset}_{year_month_clean}*p1.*",
        f"{dataset}_{year_month_clean}*p2.*",
    ]

    part_files = []
    for pattern in patterns:
        part_files.extend(data_dir.glob(pattern))

    if not part_files:
        return None

    print(f"\nCombining {dataset.upper()} {year_month} parts ({len(part_files)} files)")

    temp_parts = []
    for part_file in sorted(part_files):
        if part_file.suffix != ".parquet":
            pq_path = output_dir / part_file.with_suffix(".parquet").name
            convert_file(part_file, pq_path)
            temp_parts.append(pq_path)
        else:
            temp_parts.append(part_file)

    dfs = [pl.read_parquet(pq) for pq in sorted(set(temp_parts))]

    if len(dfs) == 2:
        df1 = dfs[0].with_row_index("_row_id")
        df2 = dfs[1].with_row_index("_row_id")
        combined_df = df1.join(df2, on="_row_id", how="inner").drop("_row_id")
    else:
        combined_df = pl.concat(dfs, how="vertical_relaxed")

    output_path = output_dir / f"{dataset}_{year_month.replace('_', '-')}.parquet"
    combined_df.write_parquet(output_path, compression="snappy")

    # cleanup temp part files
    for temp_file in temp_parts:
        if temp_file.parent == output_dir and "p1" in temp_file.name or "p2" in temp_file.name:
            temp_file.unlink()

    file_size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"Saved: {output_path.name} ({file_size_mb:.1f} MB)")

    return output_path


# main dataset converter
def convert_dataset(dataset: str) -> int:
    """Convert all files for a dataset."""
    load_dotenv()

    data_dir_key = f"{dataset.upper()}_DATA_DIR"
    parquet_dir_key = f"{dataset.upper()}_PARQUET_DIR"

    data_dir = os.environ.get(data_dir_key)
    parquet_dir = os.environ.get(parquet_dir_key)

    if not data_dir or not parquet_dir:
        print("ERROR: Missing environment variables")
        print(f"Set {data_dir_key} and {parquet_dir_key} in .env file")
        return 1

    data_dir = Path(data_dir)
    parquet_dir = Path(parquet_dir)

    if not data_dir.exists():
        print(f"ERROR: Directory not found: {data_dir}")
        return 1

    parquet_dir.mkdir(parents=True, exist_ok=True)

    print(f"\nConverting {dataset.upper()} data to Parquet")
    print(f"Source: {data_dir}")
    print(f"Output: {parquet_dir}")

    # check for multi-part files
    for pattern in data_dir.glob(f"{dataset[:3]}*p[12].*"):
        year_month = pattern.stem.split("p")[0].replace(dataset[:3], "")
        if len(year_month) == 6:
            year_month = f"{year_month[:4]}-{year_month[4:]}"
            combine_parts(data_dir, year_month, parquet_dir, dataset)

    # convert standalone files
    data_files = []
    for pattern in ["*.dta", "*.dbf", "*.sav"]:
        files = [
            f
            for f in data_dir.glob(pattern)
            if not any(x in f.name for x in ["p1", "p2", "part1", "part2"])
        ]
        data_files.extend(files)

    if data_files:
        print(f"\nConverting {len(data_files)} standalone files...")
        successful = 0
        for data_file in sorted(data_files):
            try:
                output_path = parquet_dir / data_file.with_suffix(".parquet").name
                convert_file(data_file, output_path)
                successful += 1
            except Exception as e:
                print(f"  Failed: {e}")

        print(f"\nConverted: {successful}/{len(data_files)} files")

    print(f"\nAll parquet files saved to: {parquet_dir}")
    return 0


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: python scripts/convert_to_parquet.py [sakernas|susenas|all]")
        return 1

    dataset = sys.argv[1].lower()

    if dataset == "all":
        for ds in ["sakernas", "susenas"]:
            convert_dataset(ds)
        return 0
    elif dataset in ["sakernas", "susenas"]:
        return convert_dataset(dataset)
    else:
        print(f"Unknown dataset: {dataset}")
        print("Use: sakernas, susenas, or all")
        return 1


if __name__ == "__main__":
    sys.exit(main())
