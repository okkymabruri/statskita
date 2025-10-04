"""
Inequality indicators for SUSENAS data.

BPS official: Gini coefficient (0.379 for March 2024)
International standards: Percentile ratios (P90/P10, P80/P20)
"""

import numpy as np
import polars as pl


def calculate_gini(
    df: pl.DataFrame,
    weight_col: str = "survey_weight",
    value_col: str = "per_capita_expenditure",
    exclude_single_person: bool = False,
) -> float:
    """Calculate Gini coefficient using the Lorenz curve method.

    BPS official inequality indicator. Returns value between 0 (perfect equality)
    and 1 (perfect inequality). Set exclude_single_person=True to match BPS methodology.

    Args:
        df: DataFrame with expenditure data
        weight_col: Column name for survey weights
        value_col: Column name for per capita expenditure
        exclude_single_person: Whether to exclude single-person households

    Returns:
        Gini coefficient (0.0 to 1.0)

    Example:
        >>> gini = calculate_gini(df, exclude_single_person=True)
        >>> print(f"Gini coefficient: {gini:.4f}")  # BPS March 2024: 0.379
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


def calculate_percentile_ratios(
    df: pl.DataFrame,
    weight_col: str = "survey_weight",
    value_col: str = "per_capita_expenditure",
    exclude_single_person: bool = False,
) -> dict:
    """
    Calculate inequality ratios (P90/P10, P80/P20, etc.).

    International standard inequality indicators used by UN and World Bank.
    Complements Gini coefficient with interpretable ratio measures.

    Args:
        df: DataFrame with expenditure data
        weight_col: Column name for survey weights
        value_col: Column name for per capita values
        exclude_single_person: Whether to exclude single-person households

    Returns:
        Dictionary with various percentile ratios:
        - p90_p10: Top 10% to bottom 10% ratio
        - p80_p20: Top 20% to bottom 20% ratio
        - p90_p50: Top 10% to median ratio
        - p50_p10: Median to bottom 10% ratio
        - median: Median value
        - p10: 10th percentile value
        - p90: 90th percentile value

    Example:
        >>> ratios = calculate_percentile_ratios(df, exclude_single_person=True)
        >>> print(f"P90/P10 ratio: {ratios['p90_p10']:.2f}")
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
