"""
Multi-level grouped analysis for survey indicators.
Enables hierarchical analysis like unemployment by province and gender.
"""

from typing import Dict, List, Union

import polars as pl


def calculate_indicators_by_groups(
    design,
    indicators: Union[str, List[str]] = "all",
    by: List[str] = None,
    confidence_level: float = 0.95,
    include_ci: bool = False,
) -> pl.DataFrame:
    """
    Calculate indicators with multi-level grouping for survey data.

    This is a convenience wrapper around calculate_indicators() that makes
    multi-level analysis clearer. Use calculate_indicators() directly for
    most cases.

    Args:
        design: SurveyDesign object
        indicators: Indicators to calculate
        by: Grouping variables (e.g., ["province_code", "gender"])
        confidence_level: Confidence level for intervals
        include_ci: Include confidence intervals and standard errors

    Returns:
        DataFrame with multi-level results

    Example:
        >>> # unemployment by province and gender
        >>> results = calculate_indicators_by_groups(
        ...     design,
        ...     indicators=["unemployment_rate"],
        ...     by=["province_code", "gender"]
        ... )
    """
    from .indicators import calculate_indicators

    results = calculate_indicators(
        design,
        indicators=indicators,
        by=by,
        confidence_level=confidence_level,
        as_table=True,
        include_ci=include_ci,
    )

    return results


def compare_across_groups(
    design,
    indicator: str,
    grouping_vars: List[str],
    top_n: int = 10,
    include_ci: bool = False,
) -> Dict[str, pl.DataFrame]:
    """
    Compare a single indicator across multiple grouping dimensions.

    Args:
        design: SurveyDesign object
        indicator: Single indicator to compare
        grouping_vars: List of grouping variables to compare across
        top_n: Show top N groups by estimate value
        include_ci: Include confidence intervals

    Returns:
        Dictionary mapping grouping variable to top results

    Example:
        >>> # compare unemployment across province, gender, education
        >>> comparison = compare_across_groups(
        ...     design,
        ...     "unemployment_rate",
        ...     ["province_code", "gender", "education_level"],
        ...     top_n=5
        ... )
    """
    from .indicators import calculate_indicators

    comparisons = {}

    for group_var in grouping_vars:
        # check if variable exists
        if group_var not in design.data.columns:
            print(f"Warning: {group_var} not found in data")
            continue

        # calculate for this grouping
        results = calculate_indicators(
            design,
            indicators=[indicator],
            by=[group_var],
            as_table=True,
            include_ci=include_ci,
        )

        # add sample size info
        counts = design.data.group_by(group_var).agg(
            [
                pl.len().alias("n_unweighted"),
                pl.col(design.weight_col).sum().alias("n_weighted"),
            ]
        )

        results = results.join(counts, left_on="domain", right_on=group_var)

        # get top N by estimate
        results = results.sort("estimate", descending=True).head(top_n)

        comparisons[group_var] = results

    return comparisons


def analyze_subgroups(
    design,
    indicator: str,
    dimensions: List[str],
    min_sample_size: int = 30,
    include_ci: bool = False,
) -> Dict[str, pl.DataFrame]:
    """
    Analyze indicator across multiple dimensions with sample size checks.

    Args:
        design: SurveyDesign object
        indicator: Indicator to analyze
        dimensions: List of grouping variables to analyze
        min_sample_size: Minimum sample size for reliable estimates
        include_ci: Include confidence intervals

    Returns:
        Dictionary mapping dimension to results

    Example:
        >>> # analyze unemployment across multiple dimensions
        >>> results = analyze_subgroups(
        ...     design,
        ...     "unemployment_rate",
        ...     ["province_code", "gender", "age_group", "education_level"]
        ... )
    """
    from .indicators import calculate_indicators

    results = {}

    for dim in dimensions:
        # check if dimension exists
        if dim not in design.data.columns:
            print(f"Warning: {dim} not found in data")
            continue

        # calculate for this dimension
        dim_results = calculate_indicators(
            design, indicators=[indicator], by=[dim], as_table=True, include_ci=include_ci
        )

        # add sample size
        counts = design.data.group_by(dim).agg(
            [
                pl.len().alias("n_unweighted"),
                pl.col(design.weight_col).sum().alias("n_weighted"),
            ]
        )

        dim_results = dim_results.join(counts, left_on="domain", right_on=dim)

        # flag small samples
        dim_results = dim_results.with_columns(
            (pl.col("n_unweighted") < min_sample_size).alias("small_sample")
        )

        results[dim] = dim_results.sort("estimate", descending=True)

    return results


def create_crosstab(
    design,
    indicator: str,
    row_var: str,
    col_var: str,
    include_totals: bool = True,
) -> pl.DataFrame:
    """
    Create crosstabulation of indicator by two variables.

    Args:
        design: SurveyDesign object
        indicator: Indicator to calculate
        row_var: Variable for rows
        col_var: Variable for columns
        include_totals: Include row and column totals

    Returns:
        Crosstab dataframe in wide format

    Example:
        >>> # unemployment by province and gender crosstab
        >>> crosstab = create_crosstab(
        ...     design,
        ...     "unemployment_rate",
        ...     row_var="province_code",
        ...     col_var="gender"
        ... )
    """
    from .indicators import calculate_indicators

    # calculate for all combinations
    results = calculate_indicators(
        design, indicators=[indicator], by=[row_var, col_var], as_table=True, include_ci=False
    )

    # extract row and col values from domain
    # domain format is "row_value_col_value"
    results = results.with_columns(
        [
            pl.col("domain").str.split("_").list.get(0).alias(row_var),
            pl.col("domain").str.split("_").list.get(1).alias(col_var),
        ]
    )

    # pivot to crosstab format
    crosstab = results.pivot(
        values="estimate", index=row_var, on=col_var, aggregate_function="first"
    )

    if include_totals:
        # add row totals
        row_totals = calculate_indicators(
            design, indicators=[indicator], by=[row_var], as_table=True, include_ci=False
        ).rename({"estimate": "Total", "domain": row_var})

        crosstab = crosstab.join(row_totals.select([row_var, "Total"]), on=row_var, how="left")

    return crosstab


def pivot_indicators_wide(
    design,
    indicators: List[str],
    by: List[str],
    include_ci: bool = False,
) -> pl.DataFrame:
    """
    Calculate multiple indicators and pivot to wide format.

    Each indicator becomes a column, with optional CI columns.

    Args:
        design: SurveyDesign object
        indicators: List of indicators to calculate
        by: Grouping variables
        include_ci: Include confidence interval columns

    Returns:
        Wide format dataframe with indicator columns

    Example:
        >>> # get unemployment and LFPR by province in wide format
        >>> wide = pivot_indicators_wide(
        ...     design,
        ...     ["unemployment_rate", "labor_force_participation_rate"],
        ...     by=["province_code"]
        ... )
    """
    from .indicators import calculate_indicators

    # calculate all indicators
    results = calculate_indicators(
        design, indicators=indicators, by=by, as_table=True, include_ci=include_ci
    )

    # pivot estimates to wide
    wide = results.pivot(
        values="estimate", index=["domain"], on="indicator", aggregate_function="first"
    )

    if include_ci and "ci_lower" in results.columns:
        # pivot CI columns
        ci_low = results.pivot(
            values="ci_lower", index=["domain"], on="indicator", aggregate_function="first"
        )

        ci_high = results.pivot(
            values="ci_upper", index=["domain"], on="indicator", aggregate_function="first"
        )

        # rename CI columns
        for col in ci_low.columns:
            if col != "domain":
                ci_low = ci_low.rename({col: f"{col}_ci_low"})
                ci_high = ci_high.rename({col: f"{col}_ci_high"})

        # join all together
        wide = wide.join(ci_low, on="domain", how="left")
        wide = wide.join(ci_high, on="domain", how="left")

    return wide


def create_hierarchical_breakdown(
    design,
    indicator: str,
    level1: str,
    level2: str,
    top_level1: int = 10,
) -> Dict[str, pl.DataFrame]:
    """
    Create hierarchical breakdown showing level1 totals and level2 details.

    Args:
        design: SurveyDesign object
        indicator: Indicator to analyze
        level1: Primary grouping (e.g., "province_code")
        level2: Secondary grouping (e.g., "gender")
        top_level1: Show top N level1 groups

    Returns:
        Dictionary with "summary" (level1 only) and "details" (level1 x level2)

    Example:
        >>> # unemployment by province with gender breakdown
        >>> breakdown = create_hierarchical_breakdown(
        ...     design,
        ...     "unemployment_rate",
        ...     level1="province_code",
        ...     level2="gender",
        ...     top_level1=5
        ... )
        >>> print(breakdown["summary"])  # province totals
        >>> print(breakdown["details"])   # province x gender
    """
    from .indicators import calculate_indicators
    from .survey import SurveyDesign

    # level1 summary
    summary = calculate_indicators(
        design, indicators=[indicator], by=[level1], as_table=True, include_ci=True
    )

    summary = summary.sort("estimate", descending=True).head(top_level1)

    # get top level1 values
    top_values = summary.select("domain").to_series().to_list()

    # filter data to top level1 groups
    filtered_data = design.data.filter(pl.col(level1).is_in(top_values))

    # create new survey design with filtered data
    filtered_design = SurveyDesign(
        data=filtered_data,
        weight_col=design.weight_col,
        strata_col=design.strata_col,
        psu_col=design.psu_col,
        ssu_col=design.ssu_col,
        fpc=design.fpc,
    )

    # calculate level1 x level2 for top groups
    details = calculate_indicators(
        filtered_design,
        indicators=[indicator],
        by=[level1, level2],
        as_table=True,
        include_ci=True,
    )

    return {"summary": summary, "details": details}


def format_hierarchical_table(
    summary: pl.DataFrame, details: pl.DataFrame, level1_col: str, level2_col: str
) -> str:
    """
    Format hierarchical results as readable table.

    Args:
        summary: Summary results (level1 only)
        details: Detailed results (level1 x level2)
        level1_col: Name of level1 grouping
        level2_col: Name of level2 grouping

    Returns:
        Formatted string table
    """
    output = []

    # header
    output.append(f"\n{level1_col.upper()} Breakdown by {level2_col.upper()}")
    output.append("=" * 70)

    # extract level1 values from domain in details
    # domain format: "level1value_level2value"
    details_parsed = details.with_columns(
        [
            pl.col("domain").str.split("_").list.get(0).alias(level1_col),
            pl.col("domain").str.split("_").list.get(1).alias(level2_col),
        ]
    )

    for row in summary.iter_rows(named=True):
        level1_val = row["domain"]
        est = row["estimate"]
        se = row.get("std_error", 0)

        output.append(f"\n{level1_val}: {est:.2f}% (±{se:.2f}%)")

        # get level2 breakdown for this level1
        level2_data = details_parsed.filter(pl.col(level1_col) == level1_val)

        for detail_row in level2_data.iter_rows(named=True):
            level2_val = detail_row[level2_col]
            detail_est = detail_row["estimate"]
            detail_se = detail_row.get("std_error", 0)
            output.append(f"  {level2_val}: {detail_est:.2f}% (±{detail_se:.2f}%)")

    output.append("=" * 70)

    return "\n".join(output)
