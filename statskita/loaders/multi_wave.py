"""
Multi-wave loading functionality for SAKERNAS and other surveys.
Enables loading and combining multiple survey waves efficiently.
"""

from pathlib import Path
from typing import Dict, List, Optional, Union

import polars as pl


def load_sakernas_multi(
    path_or_pattern: Union[str, Path, List[Union[str, Path]]],
    waves: Optional[List[str]] = None,
    combine: bool = False,
    add_wave_column: bool = True,
) -> Union[pl.DataFrame, Dict[str, pl.DataFrame]]:
    """
    Load multiple SAKERNAS survey waves at once.

    Args:
        path_or_pattern: Can be:
            - Directory path containing sakernas_*.parquet files
            - Glob pattern like "sakernas_202*.parquet"
            - List of specific file paths
        waves: Optional list of specific waves to load (e.g., ["2023-02", "2024-08"])
        combine: If True, concatenate all waves into single DataFrame
        add_wave_column: If True and combine=True, add 'wave' column to identify source

    Returns:
        If combine=False: Dict mapping wave to DataFrame
        If combine=True: Single concatenated DataFrame

    Examples:
        # Load all waves from directory
        >>> data = load_sakernas_multi("/path/to/parquet/")

        # Load specific waves
        >>> data = load_sakernas_multi("/path/to/parquet/", waves=["2023-02", "2024-08"])

        # Load and combine with wave identifier
        >>> df = load_sakernas_multi("/path/to/parquet/", combine=True)
    """
    from .sakernas import SakernasLoader

    loader = SakernasLoader()
    datasets = {}

    # determine files to load
    if isinstance(path_or_pattern, (str, Path)):
        path = Path(path_or_pattern)

        if path.is_dir():
            # directory: find all sakernas_*.parquet files
            files = sorted(path.glob("sakernas_*.parquet"))
        elif "*" in str(path):
            # glob pattern
            parent = path.parent
            pattern = path.name
            files = sorted(parent.glob(pattern))
        else:
            # single file
            files = [path] if path.exists() else []
    else:
        # list of paths
        files = [Path(p) for p in path_or_pattern]

    # filter by requested waves if specified
    if waves:
        wave_set = set(waves)
        filtered_files = []
        for f in files:
            # extract wave from filename (assumes sakernas_YYYY-MM.parquet format)
            wave = f.stem.replace("sakernas_", "")
            if wave in wave_set:
                filtered_files.append(f)
        files = filtered_files
    else:
        # exclude 2024-08 by default (under investigation)
        filtered_files = []
        for f in files:
            wave = f.stem.replace("sakernas_", "")
            if wave != "2024-08":
                filtered_files.append(f)
            else:
                print(f"Excluding {wave} (under investigation - specify explicitly to load)")
        files = filtered_files

    # load each file
    for file_path in files:
        if not file_path.exists():
            print(f"Warning: File not found: {file_path}")
            continue

        # extract wave from filename
        wave = file_path.stem.replace("sakernas_", "")

        try:
            df = loader.load(file_path, wave=wave)

            if add_wave_column:
                df = df.with_columns(pl.lit(wave).alias("survey_wave"))

            datasets[wave] = df
            print(f"Loaded {wave}: {len(df):,} observations")

        except Exception as e:
            print(f"Error loading {wave}: {e}")

    # combine if requested
    if combine and datasets:
        # align schemas before concatenating
        all_columns = set()
        for df in datasets.values():
            all_columns.update(df.columns)

        # add missing columns as nulls
        aligned_dfs = []
        for wave, df in datasets.items():
            missing_cols = all_columns - set(df.columns)
            if missing_cols:
                # add missing columns with null values
                for col in missing_cols:
                    df = df.with_columns(pl.lit(None).alias(col))

            # reorder columns for consistency
            df = df.select(sorted(all_columns))
            aligned_dfs.append(df)

        # concatenate all dataframes
        combined = pl.concat(aligned_dfs, how="vertical")
        print(f"\nCombined dataset: {len(combined):,} total observations")
        return combined

    return datasets


def load_and_harmonize_multi(
    path_or_pattern: Union[str, Path, List[Union[str, Path]]],
    waves: Optional[List[str]] = None,
    combine: bool = False,
    harmonize: bool = True,
) -> Union[pl.DataFrame, Dict[str, pl.DataFrame]]:
    """
    Load and harmonize multiple SAKERNAS waves.

    Args:
        path_or_pattern: Path pattern for files to load
        waves: Specific waves to load
        combine: Whether to combine into single DataFrame
        harmonize: Whether to apply harmonization

    Returns:
        Harmonized data as dict or combined DataFrame

    Example:
        >>> df = load_and_harmonize_multi(
        ...     "/path/to/parquet/",
        ...     waves=["2023-02", "2024-08", "2025-02"],
        ...     combine=True
        ... )
    """
    from ..core.wrangler import DataWrangler

    # load raw data
    datasets = load_sakernas_multi(
        path_or_pattern, waves=waves, combine=False, add_wave_column=False
    )

    if not harmonize:
        if combine:
            # combine without harmonization
            return pl.concat(list(datasets.values()))
        return datasets

    # harmonize each wave
    wrangler = DataWrangler()
    harmonized = {}

    for wave, df in datasets.items():
        try:
            df_harmonized = wrangler.wrangle(df, harmonize=True, source_wave=wave)
            # add wave identifier
            df_harmonized = df_harmonized.with_columns(pl.lit(wave).alias("survey_wave"))
            harmonized[wave] = df_harmonized
            print(f"Harmonized {wave}")

        except Exception as e:
            print(f"Error harmonizing {wave}: {e}")

    # combine if requested
    if combine and harmonized:
        # concatenate with diagonal_relaxed to handle schema differences
        combined = pl.concat(list(harmonized.values()), how="diagonal_relaxed")
        print(f"\nCombined harmonized data: {len(combined):,} observations")
        return combined

    return harmonized


def calculate_indicators_multi(
    datasets: Union[pl.DataFrame, Dict[str, pl.DataFrame]],
    indicators: Union[str, List[str]] = "all",
    by: Optional[List[str]] = None,
    as_wide: bool = False,
    use_strata: bool = False,
) -> pl.DataFrame:
    """
    Calculate indicators for multiple waves and return in wide or long format.

    Args:
        datasets: Either combined DataFrame with 'survey_wave' column or dict of DataFrames
        indicators: Indicators to calculate
        by: Grouping variables
        as_wide: If True, return wide format with waves as columns

    Returns:
        DataFrame with indicators for all waves

    Example:
        >>> results = calculate_indicators_multi(
        ...     datasets,
        ...     indicators=["unemployment_rate", "lfpr"],
        ...     as_wide=True
        ... )
    """
    from ..core.indicators import calculate_indicators
    from ..core.survey import declare_survey

    results = []

    # handle combined dataframe
    if isinstance(datasets, pl.DataFrame):
        if "survey_wave" not in datasets.columns:
            raise ValueError("Combined DataFrame must have 'survey_wave' column")

        # split by wave
        waves = datasets["survey_wave"].unique().sort()
        datasets_dict = {}
        for wave in waves:
            datasets_dict[wave] = datasets.filter(pl.col("survey_wave") == wave)
        datasets = datasets_dict

    # calculate indicators for each wave
    for wave, df in datasets.items():
        try:
            # auto-detect weight column (harmonized data uses 'survey_weight')
            weight_col = None
            for col in ["survey_weight", "WEIGHT", "WEIGHTR", "weight"]:
                if col in df.columns:
                    weight_col = col
                    break

            # auto-detect psu column
            psu_col = None
            for col in ["psu", "PSU", "KLUSTER"]:
                if col in df.columns:
                    psu_col = col
                    break

            # auto-detect strata column (only if use_strata=True)
            strata_col = None
            if use_strata:
                for col in ["strata", "STRATA"]:
                    if col in df.columns:
                        strata_col = col
                        break

            # declare survey design (strata=None by default to avoid singleton PSU)
            design = declare_survey(
                df,
                weight=weight_col,
                strata=strata_col,
                psu=psu_col,
            )

            # calculate indicators
            wave_results = calculate_indicators(design, indicators=indicators, by=by, as_table=True)

            # add wave column
            wave_results = wave_results.with_columns(pl.lit(wave).alias("wave"))

            results.append(wave_results)

        except Exception as e:
            print(f"Error calculating indicators for {wave}: {e}")

    if not results:
        return pl.DataFrame()

    # combine results
    combined_results = pl.concat(results)

    # convert to wide format if requested
    if as_wide:
        # determine index columns based on what's actually in the results
        if "domain" in combined_results.columns:
            # with grouping: pivot includes domain
            pivot_on = ["wave"]
            index_cols = ["indicator", "unit", "domain"]
        else:
            # simple pivot
            pivot_on = "wave"
            index_cols = ["indicator", "unit"] if "unit" in combined_results.columns else "indicator"

        wide_results = combined_results.pivot(
            values="estimate", index=index_cols, on=pivot_on, aggregate_function="first"
        )

        # add SE columns if available
        if "se" in combined_results.columns:
            se_wide = combined_results.pivot(
                values="se", index=index_cols, on=pivot_on, aggregate_function="first"
            )
            # rename SE columns
            se_cols = [c for c in se_wide.columns if c not in index_cols]
            for col in se_cols:
                se_wide = se_wide.rename({col: f"{col}_se"})

            # join with estimates
            if isinstance(index_cols, list):
                wide_results = wide_results.join(
                    se_wide.select([*index_cols] + [f"{c}_se" for c in se_cols]), on=index_cols
                )
            else:
                wide_results = wide_results.join(
                    se_wide.select([index_cols] + [f"{c}_se" for c in se_cols]), on=index_cols
                )

        return wide_results

    return combined_results


def compare_waves(
    datasets: Union[pl.DataFrame, Dict[str, pl.DataFrame]],
    indicators: Union[str, List[str]] = "all",
    by: Optional[List[str]] = None,
    wave_order: Optional[List[str]] = None,
) -> pl.DataFrame:
    """
    Compare indicators across waves in wide format with change calculation.

    Best for comparing 2 waves side-by-side. For 2 waves, automatically
    calculates the change (wave2 - wave1). For 3+ waves, shows all waves
    without change calculation.

    Args:
        datasets: Either combined DataFrame with 'survey_wave' column or dict of DataFrames
        indicators: Indicators to calculate
        by: Grouping variables
        wave_order: Optional list to specify wave ordering (e.g., ["2024-08", "2025-02"])

    Returns:
        DataFrame with columns: indicator, wave1, wave2, [change] (if 2 waves)

    Example:
        >>> # Compare two waves
        >>> comparison = compare_waves(
        ...     datasets,
        ...     indicators=["unemployment_rate", "lfpr"],
        ...     wave_order=["2024-08", "2025-02"]
        ... )
        >>> print(comparison)
        # indicator                        2024-08  2025-02  change
        # unemployment_rate                   4.91     4.76   -0.15
        # labor_force_participation_rate     69.50    70.10   +0.60
    """
    # get wide format results
    wide_df = calculate_indicators_multi(datasets, indicators=indicators, by=by, as_wide=True)

    if len(wide_df) == 0:
        return wide_df

    # identify wave columns (index cols are either ["indicator"] or ["indicator", "domain"])
    if "domain" in wide_df.columns:
        index_cols = ["indicator", "domain"]
    else:
        index_cols = ["indicator"]

    wave_cols = [col for col in wide_df.columns if col not in index_cols and not col.endswith("_se")]

    # sort wave columns
    if wave_order:
        # use specified order
        wave_cols = [w for w in wave_order if w in wave_cols]
    else:
        # natural sort
        wave_cols = sorted(wave_cols)

    # if exactly 2 waves, calculate change
    if len(wave_cols) == 2:
        wave1, wave2 = wave_cols[0], wave_cols[1]

        # calculate change
        result = wide_df.with_columns(
            [(pl.col(wave2) - pl.col(wave1)).alias("change")]
        )

        # select and order columns
        output_cols = index_cols + [wave1, wave2, "change"]
        result = result.select([col for col in output_cols if col in result.columns])

    else:
        # more than 2 waves - just show all waves ordered
        result = wide_df.select(index_cols + wave_cols)

    return result
