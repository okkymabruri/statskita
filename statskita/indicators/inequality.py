"""
Inequality indicators for SUSENAS data.

Includes Gini coefficient, Theil index, and percentile ratios.
"""

from typing import Tuple

import numpy as np
import polars as pl


def calculate_gini(
    df: pl.DataFrame,
    weight_col: str = "survey_weight",
    value_col: str = "per_capita_expenditure",
    exclude_single_person: bool = False,
) -> float:
    """Calculate Gini coefficient using the Lorenz curve method.

    Returns value between 0 (perfect equality) and 1 (perfect inequality).
    Set exclude_single_person=True to match BPS methodology.
    """
    # apply single-person exclusion if requested
    if exclude_single_person and "R301" in df.columns:
        df = df.filter(pl.col("R301") != 1)

    # ensure numeric types
    if weight_col not in df.columns:
        weight_col = "WEIND" if "WEIND" in df.columns else "weight"
    if value_col not in df.columns:
        value_col = "KAPITA" if "KAPITA" in df.columns else "per_capita_expenditure"

    # convert to numpy for calculation
    values = df[value_col].to_numpy()
    weights = df[weight_col].cast(pl.Float64).to_numpy()

    # sort by values
    sorted_indices = np.argsort(values)
    sorted_values = values[sorted_indices]
    sorted_weights = weights[sorted_indices]

    # calculate weighted cumulative
    cumsum_weights = np.cumsum(sorted_weights)
    total_weight = cumsum_weights[-1]

    # normalized cumulative weights
    cumsum_norm = cumsum_weights / total_weight

    # calculate weighted cumulative income
    weighted_values = sorted_values * sorted_weights
    cumsum_values = np.cumsum(weighted_values)
    total_value = cumsum_values[-1]

    # normalized cumulative values (lorenz curve)
    lorenz = cumsum_values / total_value

    # calculate gini using trapezoidal rule
    x = np.concatenate([[0], cumsum_norm])
    y = np.concatenate([[0], lorenz])

    # trapezoidal integration
    area = np.trapezoid(y, x)
    gini = 1 - 2 * area

    return gini


def calculate_gini_bootstrap(
    df: pl.DataFrame,
    n_bootstrap: int = 100,
    confidence: float = 0.95,
    weight_col: str = "survey_weight",
    value_col: str = "per_capita_expenditure",
    exclude_single_person: bool = False,
) -> Tuple[float, float, float]:
    """Calculate Gini coefficient with bootstrap confidence intervals."""
    # apply exclusion once
    if exclude_single_person and "R301" in df.columns:
        df = df.filter(pl.col("R301") != 1)

    gini_values = []
    n_households = len(df)

    for _ in range(n_bootstrap):
        # bootstrap sample
        indices = np.random.choice(n_households, n_households, replace=True)
        df_boot = df[indices]
        gini = calculate_gini(df_boot, weight_col, value_col, False)
        gini_values.append(gini)

    # calculate statistics
    mean_gini = np.mean(gini_values)
    alpha = 1 - confidence
    ci_lower = np.percentile(gini_values, alpha / 2 * 100)
    ci_upper = np.percentile(gini_values, (1 - alpha / 2) * 100)

    return mean_gini, ci_lower, ci_upper


def calculate_theil_index(
    df: pl.DataFrame,
    weight_col: str = "survey_weight",
    value_col: str = "per_capita_expenditure",
    exclude_single_person: bool = False,
) -> float:
    """Calculate Theil index (GE(1)) inequality measure."""
    # apply single-person exclusion if requested
    if exclude_single_person and "R301" in df.columns:
        df = df.filter(pl.col("R301") != 1)

    # ensure columns exist
    if weight_col not in df.columns:
        weight_col = "WEIND" if "WEIND" in df.columns else "weight"
    if value_col not in df.columns:
        value_col = "KAPITA" if "KAPITA" in df.columns else "per_capita_expenditure"

    # calculate weighted mean
    weights = df[weight_col].cast(pl.Float64)
    values = df[value_col].cast(pl.Float64)

    total_weight = weights.sum()
    mean_value = (values * weights).sum() / total_weight

    # calculate theil components
    df = df.with_columns(
        [
            (pl.col(value_col) / mean_value).alias("ratio"),
            pl.col(weight_col).cast(pl.Float64).alias("w"),
        ]
    )

    df = df.with_columns(
        [(pl.col("ratio") * pl.col("ratio").log() * pl.col("w")).alias("theil_component")]
    )

    theil = df["theil_component"].sum() / total_weight

    return float(theil)


def calculate_percentile_ratios(
    df: pl.DataFrame,
    weight_col: str = "survey_weight",
    value_col: str = "per_capita_expenditure",
    exclude_single_person: bool = False,
) -> dict:
    """
    Calculate inequality ratios (P90/P10, P80/P20, etc.).

    Args:
        df: DataFrame with expenditure data
        weight_col: Column name for survey weights
        value_col: Column name for per capita values
        exclude_single_person: Whether to exclude single-person households

    Returns:
        Dictionary with various percentile ratios
    """
    # apply single-person exclusion if requested
    if exclude_single_person and "R301" in df.columns:
        df = df.filter(pl.col("R301") != 1)

    # ensure columns exist
    if value_col not in df.columns:
        value_col = "KAPITA" if "KAPITA" in df.columns else "per_capita_expenditure"

    # calculate percentiles
    p10 = df[value_col].quantile(0.10)
    p20 = df[value_col].quantile(0.20)
    p50 = df[value_col].quantile(0.50)  # median
    p80 = df[value_col].quantile(0.80)
    p90 = df[value_col].quantile(0.90)

    # calculate ratios
    ratios = {
        "p90_p10": float(p90 / p10) if p10 > 0 else None,
        "p80_p20": float(p80 / p20) if p20 > 0 else None,
        "p90_p50": float(p90 / p50) if p50 > 0 else None,
        "p50_p10": float(p50 / p10) if p10 > 0 else None,
        "median": float(p50),
        "p10": float(p10),
        "p90": float(p90),
    }

    return ratios


def calculate_atkinson_index(
    df: pl.DataFrame,
    epsilon: float = 0.5,
    weight_col: str = "survey_weight",
    value_col: str = "per_capita_expenditure",
    exclude_single_person: bool = False,
) -> float:
    """
    Calculate Atkinson inequality index.

    Args:
        df: DataFrame with expenditure data
        epsilon: Inequality aversion parameter (0.5, 1.0, or 2.0 common)
        weight_col: Column name for survey weights
        value_col: Column name for per capita values
        exclude_single_person: Whether to exclude single-person households

    Returns:
        Atkinson index value
    """
    # apply single-person exclusion if requested
    if exclude_single_person and "R301" in df.columns:
        df = df.filter(pl.col("R301") != 1)

    # ensure columns exist
    if weight_col not in df.columns:
        weight_col = "WEIND" if "WEIND" in df.columns else "weight"
    if value_col not in df.columns:
        value_col = "KAPITA" if "KAPITA" in df.columns else "per_capita_expenditure"

    # calculate weighted mean
    weights = df[weight_col].cast(pl.Float64).to_numpy()
    values = df[value_col].cast(pl.Float64).to_numpy()

    weights_norm = weights / weights.sum()
    mean_value = np.sum(values * weights_norm)

    if epsilon == 1:
        # special case: epsilon = 1
        log_values = np.log(values / mean_value)
        atkinson = 1 - np.exp(np.sum(weights_norm * log_values))
    else:
        # general case
        term = np.sum(weights_norm * (values / mean_value) ** (1 - epsilon))
        atkinson = 1 - term ** (1 / (1 - epsilon))

    return float(atkinson)
