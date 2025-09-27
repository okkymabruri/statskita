"""Labor force and demographic indicator calculations with survey design awareness."""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import polars as pl

from .survey import SurveyDesign, SurveyEstimate


@dataclass
class IndicatorResult:
    """Result container for calculated indicators."""

    indicator_name: str
    estimate: SurveyEstimate
    domain: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class IndicatorCalculator:
    """Calculate labor force indicators with proper survey methodology."""

    def __init__(self, survey_design: SurveyDesign):
        """Initialize with survey design object."""
        self.design = survey_design
        self.data = survey_design.data

    def calculate_labor_force_participation_rate(
        self,
        by: Optional[List[str]] = None,
        min_working_age: int = 15,
        confidence_level: float = 0.95,
    ) -> Dict[str, IndicatorResult]:
        """Calculate Labor Force Participation Rate (TPAK).

        TPAK = (Labor Force / Working Age Population) * 100

        Args:
            by: Grouping variables
            min_working_age: Minimum working age
            confidence_level: Confidence level for intervals

        Returns:
            Dictionary of TPAK estimates by domain
        """
        # create working age population indicator
        df_working = self.data.with_columns(
            [
                (pl.col("age") >= min_working_age).alias("working_age"),
                pl.when(pl.col("in_labor_force").is_not_null())
                .then(pl.col("in_labor_force"))
                .otherwise(False)
                .alias("in_lf"),
            ]
        )

        # filter to working age population
        df_working_age = df_working.filter(pl.col("working_age"))

        if len(df_working_age) == 0:
            return {}

        # create temporary survey design for working age population
        working_design = SurveyDesign(
            data=df_working_age,
            weight_col=self.design.weight_col,
            strata_col=self.design.strata_col,
            psu_col=self.design.psu_col,
            fpc=self.design.fpc,
        )

        # calculate labor force participation rate
        lfpr_estimates = working_design.estimate_proportion(
            variable="in_lf", by=by, confidence_level=confidence_level
        )

        # wrap results
        results = {}
        for domain, estimate in lfpr_estimates.items():
            # keep as float*100 – rounding loses precision in CI calc
            est_pct = estimate.as_pct()
            results[domain] = IndicatorResult(
                indicator_name="Labor Force Participation Rate (TPAK)",
                estimate=est_pct,
                domain=domain if domain != "overall" else None,
                metadata={
                    "min_working_age": min_working_age,
                    "sample_size": len(df_working_age),
                    "denominator": "Working age population",
                },
            )

        return results

    def calculate_unemployment_rate(
        self, by: Optional[List[str]] = None, confidence_level: float = 0.95
    ) -> Dict[str, IndicatorResult]:
        """Calculate Unemployment Rate (TPT).

        TPT = (Unemployed / Labor Force) * 100

        Args:
            by: Grouping variables
            confidence_level: Confidence level for intervals

        Returns:
            Dictionary of TPT estimates by domain
        """
        # filter to labor force only
        df_labor_force = self.data.filter(pl.col("in_labor_force"))

        if len(df_labor_force) == 0:
            return {}

        # create survey design for labor force
        lf_design = SurveyDesign(
            data=df_labor_force,
            weight_col=self.design.weight_col,
            strata_col=self.design.strata_col,
            psu_col=self.design.psu_col,
            fpc=self.design.fpc,
        )

        # calculate unemployment rate
        unemployment_estimates = lf_design.estimate_proportion(
            variable="unemployed", by=by, confidence_level=confidence_level
        )

        # wrap results
        results = {}
        for domain, estimate in unemployment_estimates.items():
            est_pct = estimate.as_pct()
            results[domain] = IndicatorResult(
                indicator_name="Unemployment Rate (TPT)",
                estimate=est_pct,
                domain=domain if domain != "overall" else None,
                metadata={"sample_size": len(df_labor_force), "denominator": "Labor force"},
            )

        return results

    def calculate_employment_rate(
        self,
        by: Optional[List[str]] = None,
        min_working_age: int = 15,
        confidence_level: float = 0.95,
    ) -> Dict[str, IndicatorResult]:
        """Calculate Employment Rate.

        Employment Rate = (Employed / Working Age Population) * 100

        Args:
            by: Grouping variables
            min_working_age: Minimum working age
            confidence_level: Confidence level for intervals

        Returns:
            Dictionary of employment rate estimates by domain
        """
        # create working age population indicator
        df_working = self.data.with_columns(
            [(pl.col("age") >= min_working_age).alias("working_age")]
        )

        # filter to working age population
        df_working_age = df_working.filter(pl.col("working_age"))

        if len(df_working_age) == 0:
            return {}

        # create survey design for working age population
        working_design = SurveyDesign(
            data=df_working_age,
            weight_col=self.design.weight_col,
            strata_col=self.design.strata_col,
            psu_col=self.design.psu_col,
            fpc=self.design.fpc,
        )

        # calculate employment rate
        employment_estimates = working_design.estimate_proportion(
            variable="employed", by=by, confidence_level=confidence_level
        )

        # wrap results
        results = {}
        for domain, estimate in employment_estimates.items():
            est_pct = estimate.as_pct()
            results[domain] = IndicatorResult(
                indicator_name="Employment Rate",
                estimate=est_pct,
                domain=domain if domain != "overall" else None,
                metadata={
                    "min_working_age": min_working_age,
                    "sample_size": len(df_working_age),
                    "denominator": "Working age population",
                },
            )

        return results

    def calculate_neet_rate(
        self,
        by: Optional[List[str]] = None,
        age_range: tuple = (15, 24),
        confidence_level: float = 0.95,
    ) -> Dict[str, IndicatorResult]:
        """Calculate NEET rate (Not in Employment, Education, or Training).

        Args:
            by: Grouping variables
            age_range: Age range for NEET calculation (default 15-24)
            confidence_level: Confidence level for intervals

        Returns:
            Dictionary of NEET rate estimates by domain
        """
        min_age, max_age = age_range

        # filter to target age group
        df_youth = self.data.filter((pl.col("age") >= min_age) & (pl.col("age") <= max_age))

        if len(df_youth) == 0:
            return {}

        # NEET indicator: not employed and not in school
        df_youth = df_youth.with_columns(
            [
                (
                    ~pl.col("employed")
                    & ~pl.col("in_school").fill_null(False)  # assume not in school if missing
                ).alias("neet")
            ]
        )

        # create survey design for youth
        youth_design = SurveyDesign(
            data=df_youth,
            weight_col=self.design.weight_col,
            strata_col=self.design.strata_col,
            psu_col=self.design.psu_col,
            fpc=self.design.fpc,
        )

        # calculate NEET rate
        neet_estimates = youth_design.estimate_proportion(
            variable="neet", by=by, confidence_level=confidence_level
        )

        # wrap results
        results = {}
        for domain, estimate in neet_estimates.items():
            est_pct = estimate.as_pct()
            results[domain] = IndicatorResult(
                indicator_name="NEET Rate",
                estimate=est_pct,
                domain=domain if domain != "overall" else None,
                metadata={
                    "age_range": age_range,
                    "sample_size": len(df_youth),
                    "denominator": f"Population aged {min_age}-{max_age}",
                },
            )

        return results

    def calculate_underemployment_rate(
        self,
        by: Optional[List[str]] = None,
        hours_threshold: int = 35,
        confidence_level: float = 0.95,
    ) -> Dict[str, IndicatorResult]:
        """Calculate underemployment rate (time-related underemployment).

        Args:
            by: Grouping variables
            hours_threshold: Hours threshold for underemployment
            confidence_level: Confidence level for intervals

        Returns:
            Dictionary of underemployment rate estimates
        """
        # filter to employed persons
        df_employed = self.data.filter(pl.col("employed"))

        if len(df_employed) == 0:
            return {}

        # underemployment indicator
        df_employed = df_employed.with_columns(
            [(pl.col("hours_worked") < hours_threshold).alias("underemployed_time")]
        )

        # create survey design for employed
        employed_design = SurveyDesign(
            data=df_employed,
            weight_col=self.design.weight_col,
            strata_col=self.design.strata_col,
            psu_col=self.design.psu_col,
            fpc=self.design.fpc,
        )

        # calculate underemployment rate
        underempl_estimates = employed_design.estimate_proportion(
            variable="underemployed_time", by=by, confidence_level=confidence_level
        )

        # wrap results
        results = {}
        for domain, estimate in underempl_estimates.items():
            est_pct = estimate.as_pct()
            results[domain] = IndicatorResult(
                indicator_name="Time-related Underemployment Rate",
                estimate=est_pct,
                domain=domain if domain != "overall" else None,
                metadata={
                    "hours_threshold": hours_threshold,
                    "sample_size": len(df_employed),
                    "denominator": "Employed persons",
                },
            )

        return results

    def calculate_all_indicators(
        self,
        by: Optional[List[str]] = None,
        min_working_age: int = 15,
        confidence_level: float = 0.95,
    ) -> Dict[str, Dict[str, IndicatorResult]]:
        """Calculate all standard labor force indicators.

        Args:
            by: Grouping variables
            min_working_age: Minimum working age
            confidence_level: Confidence level for intervals

        Returns:
            Dictionary of all indicator results
        """
        results = {}

        # labor force participation rate
        results["lfpr"] = self.calculate_labor_force_participation_rate(
            by=by, min_working_age=min_working_age, confidence_level=confidence_level
        )

        # unemployment rate
        results["unemployment_rate"] = self.calculate_unemployment_rate(
            by=by, confidence_level=confidence_level
        )

        # employment rate
        results["employment_rate"] = self.calculate_employment_rate(
            by=by, min_working_age=min_working_age, confidence_level=confidence_level
        )

        # NEET rate (if education data available)
        if "in_school" in self.data.columns:
            results["neet"] = self.calculate_neet_rate(by=by, confidence_level=confidence_level)

        # underemployment rate (if hours data available)
        if "hours_worked" in self.data.columns:
            results["underemployment"] = self.calculate_underemployment_rate(
                by=by, confidence_level=confidence_level
            )

        return results


def calculate_indicators(
    survey_design: SurveyDesign,
    indicators: List[str],
    by: Optional[List[str]] = None,
    confidence_level: float = 0.95,
    **kwargs,
) -> Dict[str, Dict[str, IndicatorResult]]:
    """Calculate specified labor force indicators.

    Args:
        survey_design: Survey design object
        indicators: List of indicators to calculate (English or Indonesian names)
        by: Grouping variables for domain estimation
        confidence_level: Confidence level for intervals
        **kwargs: Additional arguments for specific indicators

    Returns:
        Dictionary of indicator results by indicator and domain

    Example:
        >>> import statskita as sk
        >>> df = sk.load_sakernas("data.sav")
        >>> design = sk.declare_survey(df, weight="WEIGHT")
        >>> # English names
        >>> results = sk.calculate_indicators(design, ["lfpr", "unemployment_rate"])
        >>> # Indonesian aliases (backward compatibility)
        >>> results = sk.calculate_indicators(design, ["tpak", "tpt"], by=["province_code"])
    """
    calculator = IndicatorCalculator(survey_design)
    results = {}

    # Primary English indicator methods
    indicator_methods = {
        "lfpr": calculator.calculate_labor_force_participation_rate,
        "unemployment_rate": calculator.calculate_unemployment_rate,
        "employment_rate": calculator.calculate_employment_rate,
        "neet_rate": calculator.calculate_neet_rate,
        "underemployment_rate": calculator.calculate_underemployment_rate,
    }

    # Indonesian aliases for backward compatibility
    # TODO: migrate examples to use English names by v0.3.0
    indonesian_aliases = {
        "tpak": "lfpr",  # TPAK -> Labour Force Participation Rate (TPAK)
        "tpt": "unemployment_rate",  # TPT -> Unemployment Rate (TPT)
        "tingkat_kerja": "employment_rate",
        "neet": "neet_rate",
        "setengah_menganggur": "underemployment_rate",
        # common English variants
        "labour_force_participation_rate": "lfpr",
        "labour_force_rate": "lfpr",
        "labor_force_participation_rate": "lfpr",
        "unemployment": "unemployment_rate",
        "underemployment": "underemployment_rate",
    }

    for indicator in indicators:
        # resolve Indonesian aliases to English names
        english_name = indonesian_aliases.get(indicator, indicator)

        if english_name not in indicator_methods:
            available = list(indicator_methods.keys()) + list(indonesian_aliases.keys())
            raise ValueError(f"Unknown indicator: {indicator}. Available: {available}")

        method = indicator_methods[english_name]

        # call method with appropriate arguments based on English name
        if english_name == "neet_rate":
            results[indicator] = method(
                by=by,
                confidence_level=confidence_level,
                age_range=kwargs.get("age_range", (15, 24)),
            )
        elif english_name in ["lfpr", "employment_rate"]:
            results[indicator] = method(
                by=by,
                confidence_level=confidence_level,
                min_working_age=kwargs.get("min_working_age", 15),
            )
        elif english_name == "underemployment_rate":
            results[indicator] = method(
                by=by,
                confidence_level=confidence_level,
                hours_threshold=kwargs.get("hours_threshold", 35),
            )
        else:  # unemployment_rate and others
            results[indicator] = method(by=by, confidence_level=confidence_level)

    return results
