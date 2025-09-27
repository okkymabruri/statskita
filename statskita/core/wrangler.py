"""Data wrangling for survey data."""
# TODO: modularize when supporting multiple surveys

from typing import Any, Dict, List, Optional

import polars as pl

from .harmonizer import SurveyHarmonizer


class DataWrangler:
    """Data wrangling and cleaning for survey data."""

    def __init__(self, dataset_type: str = "sakernas"):
        self.dataset_type = dataset_type
        self.harmonizer = SurveyHarmonizer(dataset_type)

    def wrangle(
        self,
        df: pl.DataFrame,
        harmonize: bool = True,
        source_wave: Optional[str] = None,
        fix_types: bool = True,
        validate_weights: bool = True,
        create_indicators: bool = True,
        min_working_age: int = 15,
    ) -> pl.DataFrame:
        """Wrangle survey data into analysis-ready format.

        Args:
            df: Raw survey data
            harmonize: Standardize variable names across waves
            source_wave: Which survey wave (e.g. "2024-02")
            fix_types: Convert string codes to proper types
            validate_weights: Check for invalid survey weights
            create_indicators: Generate labor force indicators
        """
        result_df = df.clone()

        # harmonize if needed
        if harmonize and source_wave:
            result_df, mapping_log = self.harmonizer.harmonize(result_df, source_wave)
            # harmonizer creates labor force indicators after harmonization
            result_df = self.harmonizer.create_labor_force_indicators(result_df)

        if fix_types:
            result_df = self._fix_data_types(result_df)

        if validate_weights:
            result_df = self._validate_weights(result_df)

        # only create if not already there
        if create_indicators and "in_labor_force" not in result_df.columns:
            result_df = self._create_labor_indicators(result_df, min_working_age)

        result_df = self._final_cleaning(result_df)

        return result_df

    def _fix_data_types(self, df: pl.DataFrame) -> pl.DataFrame:
        # fix types - should probably use schema validation instead
        result_df = df.clone()

        for col in result_df.columns:
            if result_df[col].dtype == pl.String:
                try:
                    # check if all numeric
                    if result_df[col].str.match(r"^\d+$").all():
                        result_df = result_df.with_columns(pl.col(col).cast(pl.Int64))
                except Exception:
                    pass  # keep as string

        # weights must be numeric
        weight_cols = ["survey_weight", "WEIGHT", "weight"]
        for col in weight_cols:
            if col in result_df.columns:
                result_df = result_df.with_columns(pl.col(col).cast(pl.Float64))

        age_cols = ["age", "AGE", "B4K5"]
        for col in age_cols:
            if col in result_df.columns:
                result_df = result_df.with_columns(pl.col(col).cast(pl.Int64))

        return result_df

    def _validate_weights(self, df: pl.DataFrame) -> pl.DataFrame:
        # validate weights
        result_df = df.clone()

        # find weight col
        weight_col = None
        for col in ["survey_weight", "WEIGHT", "weight"]:
            if col in result_df.columns:
                weight_col = col
                break

        if not weight_col:
            return result_df

        result_df = result_df.filter(pl.col(weight_col) > 0)  # drop invalid

        # check for extreme values
        median_weight = result_df[weight_col].median()
        if median_weight:
            extreme_threshold = median_weight * 100  # arbitrary threshold
            n_extreme = len(result_df.filter(pl.col(weight_col) > extreme_threshold))
            if n_extreme > 0:
                print(
                    f"Warning: {n_extreme} observations with extreme weights (>{extreme_threshold:.1f})"
                )

        return result_df

    def _create_labor_indicators(self, df: pl.DataFrame, min_working_age: int = 15) -> pl.DataFrame:
        # print(f"Creating labor indicators...")  # debug
        result_df = df.clone()

        # WAP
        age_col = self._find_column(result_df, ["age", "AGE", "B4K5"])
        if age_col:
            result_df = result_df.with_columns(
                (pl.col(age_col) >= min_working_age).alias("working_age_population")
            )

        # employment
        work_status_col = self._find_column(result_df, ["work_status", "B5R1"])
        if work_status_col:
            result_df = result_df.with_columns(
                [
                    (pl.col(work_status_col) == "Working").alias("employed"),
                    (pl.col(work_status_col) == "Not Working").alias("not_working"),
                ]
            )
        else:
            # defaults
            result_df = result_df.with_columns(
                [pl.lit(False).alias("employed"), pl.lit(False).alias("not_working")]
            )

        # LF
        if "employed" in result_df.columns:
            result_df = result_df.with_columns(
                pl.col("employed").alias("in_labor_force")  # simplified
            )

        # unemployed
        result_df = result_df.with_columns(
            (~pl.col("employed") & pl.col("in_labor_force")).alias("unemployed")
        )

        # underemployment
        hours_col = self._find_column(result_df, ["hours_worked", "B5R28"])
        if hours_col:
            result_df = result_df.with_columns(
                (pl.col("employed") & (pl.col(hours_col) < 35)).alias("underemployed")
            )

        return result_df

    def _find_column(self, df: pl.DataFrame, possible_names: List[str]) -> Optional[str]:
        for name in possible_names:
            if name in df.columns:
                return name
        return None

    def _final_cleaning(self, df: pl.DataFrame) -> pl.DataFrame:
        result_df = df.clone()

        key_vars = (
            ["age", "gender"]
            if "age" in result_df.columns and "gender" in result_df.columns
            else []
        )
        if key_vars:
            for var in key_vars:
                result_df = result_df.filter(pl.col(var).is_not_null())

        # sort
        id_cols = [col for col in ["province_code", "age", "gender"] if col in result_df.columns]
        if id_cols:
            result_df = result_df.sort(id_cols)

        return result_df

    def get_data_summary(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Get data summary."""
        summary = {
            "shape": df.shape,
            "columns": list(df.columns),
            "missing_data": {},
            "data_types": {col: str(df[col].dtype) for col in df.columns},
        }

        for col in df.columns:
            missing_count = df[col].null_count()
            if missing_count > 0:
                summary["missing_data"][col] = {
                    "count": missing_count,
                    "percentage": round(missing_count / len(df) * 100, 2),
                }

        labor_indicators = ["employed", "unemployed", "in_labor_force", "working_age_population"]
        available_indicators = [ind for ind in labor_indicators if ind in df.columns]
        summary["labor_indicators"] = available_indicators

        design_vars = ["survey_weight", "province_code", "urban_rural"]
        available_design = [var for var in design_vars if var in df.columns]
        summary["survey_design_vars"] = available_design

        return summary


def wrangle(
    df: pl.DataFrame,
    harmonize: bool = True,
    source_wave: Optional[str] = None,
    dataset_type: str = "sakernas",
    **kwargs,
) -> pl.DataFrame:
    """Clean and harmonize survey data.

    Example:
        >>> df = sk.load_sakernas("sakernas_2024.sav")
        >>> clean = sk.wrangle(df, source_wave="2024-02")
    """
    wrangler = DataWrangler(dataset_type=dataset_type)
    return wrangler.wrangle(df, harmonize=harmonize, source_wave=source_wave, **kwargs)
