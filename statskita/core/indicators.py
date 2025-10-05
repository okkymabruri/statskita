"""Labor force and demographic indicator calculations with survey design awareness."""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import polars as pl

from .survey import SurveyDesign, SurveyEstimate

# indicator priority order for table display
INDICATOR_PRIORITY = [
    "labor_force_participation_rate",
    "employment_rate",
    "unemployment_rate",
    "underemployment_rate",
    "inactivity_rate",
    "female_labor_force_participation_rate",
    "average_wage",
    "neet_rate",
    "informal_employment_rate",
    "per_capita_expenditure",
    "poverty_headcount",
    "poverty_gap",
    "poverty_severity",
    "gini_coefficient",
]

# indicator units for display
INDICATOR_UNITS = {
    "labor_force_participation_rate": "%",
    "employment_rate": "%",
    "unemployment_rate": "%",
    "underemployment_rate": "%",
    "inactivity_rate": "%",
    "female_labor_force_participation_rate": "%",
    "neet_rate": "%",
    "informal_employment_rate": "%",
    "average_wage": "M Rp",  # millions of Rupiah
    "per_capita_expenditure": "Rp",
    "poverty_headcount": "%",
    "poverty_gap": "",
    "poverty_severity": "",
    "gini_coefficient": "",
}


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

    def calculate_female_labor_force_participation_rate(
        self,
        by: Optional[List[str]] = None,
        min_working_age: int = 15,
        confidence_level: float = 0.95,
    ) -> Dict[str, IndicatorResult]:
        """Calculate Female Labor Force Participation Rate.

        FLFPR = (Female Labor Force / Female Working Age Population) * 100

        Args:
            by: Grouping variables
            min_working_age: Minimum working age
            confidence_level: Confidence level for intervals

        Returns:
            Dictionary of FLFPR estimates by domain
        """
        # check gender column exists
        if "gender" not in self.data.columns:
            return {}

        # filter working age females
        df_working = self.data.with_columns(
            [
                (pl.col("age") >= min_working_age).alias("working_age"),
                pl.when(pl.col("in_labor_force").is_not_null())
                .then(pl.col("in_labor_force"))
                .otherwise(False)
                .alias("in_lf"),
            ]
        ).filter((pl.col("working_age")) & (pl.col("gender") == "PEREMPUAN"))

        if len(df_working) == 0:
            return {}

        # create survey design for working age females
        female_design = SurveyDesign(
            data=df_working,
            weight_col=self.design.weight_col,
            strata_col=self.design.strata_col,
            psu_col=self.design.psu_col,
            fpc=self.design.fpc,
        )

        # calculate female lfpr
        flfpr_estimates = female_design.estimate_proportion(
            variable="in_lf", by=by, confidence_level=confidence_level
        )

        # wrap results
        results = {}
        for domain, estimate in flfpr_estimates.items():
            est_pct = estimate.as_pct()
            results[domain] = IndicatorResult(
                indicator_name="Female Labor Force Participation Rate (TPAK Perempuan)",
                estimate=est_pct,
                domain=domain if domain != "overall" else None,
                metadata={
                    "min_working_age": min_working_age,
                    "sample_size": len(df_working),
                    "denominator": "Female working age population",
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
        # filter to working age (15+) labor force only
        df_labor_force = self.data.filter((pl.col("age") >= 15) & pl.col("in_labor_force"))

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
                indicator_name="Employment Rate (Tingkat Kesempatan Kerja)",
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

        # check if in_school column exists
        if "in_school" not in df_youth.columns:
            # if no education data, cannot calculate NEET properly
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

        # check if hours_worked column exists
        if "hours_worked" not in df_employed.columns:
            # cannot calculate underemployment without hours data
            return {}

        # underemployment indicator - handle missing hours_worked
        df_employed = df_employed.with_columns(
            [
                pl.when(pl.col("hours_worked").is_not_null())
                .then(pl.col("hours_worked") < hours_threshold)
                .otherwise(False)
                .alias("underemployed_time")
            ]
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
                indicator_name="Underemployment Rate (Setengah Menganggur)",
                estimate=est_pct,
                domain=domain if domain != "overall" else None,
                metadata={
                    "hours_threshold": hours_threshold,
                    "sample_size": len(df_employed),
                    "denominator": "Employed persons",
                },
            )

        return results

    def calculate_inactivity_rate(
        self,
        by: Optional[List[str]] = None,
        min_working_age: int = 15,
        confidence_level: float = 0.95,
    ) -> Dict[str, IndicatorResult]:
        """Calculate Inactivity Rate (Tingkat Ketidakaktifan).

        Inactivity Rate = 100 - Labor Force Participation Rate

        Args:
            by: Grouping variables
            min_working_age: Minimum working age
            confidence_level: Confidence level for intervals

        Returns:
            Dictionary of inactivity rate estimates
        """
        # Get LFPR results
        lfpr_results = self.calculate_labor_force_participation_rate(
            by=by, min_working_age=min_working_age, confidence_level=confidence_level
        )

        # Calculate inactivity rate as 100 - LFPR
        results = {}
        for domain, lfpr_result in lfpr_results.items():
            # Create new estimate with inverted value
            lfpr_estimate = lfpr_result.estimate
            inactivity_estimate = SurveyEstimate(
                value=100.0 - lfpr_estimate.value,
                se=lfpr_estimate.se,  # Same standard error
                ci_low=100.0 - lfpr_estimate.ci_high,  # Note: inverted
                ci_high=100.0 - lfpr_estimate.ci_low,  # Note: inverted
                df=lfpr_estimate.df,
                deff=lfpr_estimate.deff if hasattr(lfpr_estimate, "deff") else None,
            )

            results[domain] = IndicatorResult(
                indicator_name="Inactivity Rate (Tingkat Ketidakaktifan)",
                estimate=inactivity_estimate,
                domain=domain if domain != "overall" else None,
                metadata={
                    "min_working_age": min_working_age,
                    "denominator": "Working age population",
                },
            )

        return results

    def calculate_average_wage(
        self,
        by: Optional[List[str]] = None,
        confidence_level: float = 0.95,
    ) -> Dict[str, IndicatorResult]:
        """Calculate Average Wage (Rata-Rata Upah).

        Average of total wages (cash + goods) for employed persons with wages.

        Args:
            by: Grouping variables
            confidence_level: Confidence level for intervals

        Returns:
            Dictionary of average wage estimates
        """
        # Filter to employed persons with wage data
        if "total_wage" in self.data.columns:
            wage_col = "total_wage"
        elif "wage_cash" in self.data.columns:
            wage_col = "wage_cash"
        else:
            return {}

        df_employed_wage = self.data.filter(
            pl.col("employed") & pl.col(wage_col).is_not_null() & (pl.col(wage_col) > 0)
        )

        if len(df_employed_wage) == 0:
            return {}

        # Create survey design for employed with wages
        wage_design = SurveyDesign(
            data=df_employed_wage,
            weight_col=self.design.weight_col,
            strata_col=self.design.strata_col,
            psu_col=self.design.psu_col,
            fpc=self.design.fpc,
        )

        # Calculate average wage
        wage_estimates = wage_design.estimate_mean(
            variable=wage_col, by=by, confidence_level=confidence_level
        )

        # Wrap results
        results = {}
        for domain, estimate in wage_estimates.items():
            results[domain] = IndicatorResult(
                indicator_name="Average Wage (Rata-Rata Upah)",
                estimate=estimate,
                domain=domain if domain != "overall" else None,
                metadata={
                    "sample_size": len(df_employed_wage),
                    "denominator": "Employed persons with wages",
                    "unit": "Rupiah per month",
                },
            )

        return results

    def calculate_informal_employment_rate(
        self,
        by: Optional[List[str]] = None,
        confidence_level: float = 0.95,
    ) -> Dict[str, IndicatorResult]:
        """Calculate Informal Employment Rate.

        Percentage of employed persons working in informal activities.
        According to BPS: informal = employment status 1, 2, 5, 6, 7

        Args:
            by: Grouping variables
            confidence_level: Confidence level for intervals

        Returns:
            Dictionary of informal employment rate estimates
        """
        # Check for informal employment indicator
        if "informal_employment" not in self.data.columns:
            return {}

        # Filter to employed persons with non-null informal employment status
        df_employed = self.data.filter(
            pl.col("employed") & pl.col("informal_employment").is_not_null()
        )

        if len(df_employed) == 0:
            return {}

        # Create survey design for employed
        employed_design = SurveyDesign(
            data=df_employed,
            weight_col=self.design.weight_col,
            strata_col=self.design.strata_col,
            psu_col=self.design.psu_col,
            fpc=self.design.fpc,
        )

        # Calculate informal employment rate
        informal_estimates = employed_design.estimate_proportion(
            variable="informal_employment", by=by, confidence_level=confidence_level
        )

        # Wrap results
        results = {}
        for domain, estimate in informal_estimates.items():
            est_pct = estimate.as_pct()
            results[domain] = IndicatorResult(
                indicator_name="Informal Employment Rate (Tingkat Kegiatan Informal)",
                estimate=est_pct,
                domain=domain if domain != "overall" else None,
                metadata={
                    "sample_size": len(df_employed),
                    "denominator": "Employed persons",
                    "definition": "Employment status 1, 2, 5, 6, 7 (BPS definition)",
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

        # priority 1: core labor indicators
        results["labor_force_participation_rate"] = self.calculate_labor_force_participation_rate(
            by=by, min_working_age=min_working_age, confidence_level=confidence_level
        )

        results["unemployment_rate"] = self.calculate_unemployment_rate(
            by=by, confidence_level=confidence_level
        )

        results["employment_rate"] = self.calculate_employment_rate(
            by=by, min_working_age=min_working_age, confidence_level=confidence_level
        )

        # priority 2: supplementary indicators
        results["inactivity_rate"] = self.calculate_inactivity_rate(
            by=by, min_working_age=min_working_age, confidence_level=confidence_level
        )

        # priority 3: conditional indicators (if data available)
        if "in_school" in self.data.columns:
            results["neet_rate"] = self.calculate_neet_rate(
                by=by, confidence_level=confidence_level
            )

        if "hours_worked" in self.data.columns:
            results["underemployment_rate"] = self.calculate_underemployment_rate(
                by=by, confidence_level=confidence_level
            )

        if "informal_employment" in self.data.columns:
            results["informal_employment_rate"] = self.calculate_informal_employment_rate(
                by=by, confidence_level=confidence_level
            )

        # priority 4: wage data (if available)
        if "total_wage" in self.data.columns or "wage_cash" in self.data.columns:
            results["average_wage"] = self.calculate_average_wage(
                by=by, confidence_level=confidence_level
            )

        return results

    def calculate_per_capita_expenditure_indicator(
        self,
        by: Optional[List[str]] = None,
        confidence_level: float = 0.95,
    ) -> Dict[str, IndicatorResult]:
        """Weighted mean of monthly per-capita expenditure using KAPITA from blok43."""

        # prioritize KAPITA from blok43
        column = next(
            (
                col
                for col in (
                    "KAPITA",  # priority: BPS pre-calculated value
                    "kapita",
                    "per_capita_expenditure",
                )
                if col in self.data.columns
            ),
            None,
        )
        if column is None:
            return {}

        estimates = self.design.estimate_mean(column, by=by, confidence_level=confidence_level)

        results: Dict[str, IndicatorResult] = {}
        for domain, estimate in estimates.items():
            results[domain] = IndicatorResult(
                indicator_name="Average Per Capita Expenditure (SUSENAS)",
                estimate=estimate,
                domain=domain if domain != "overall" else None,
                metadata={"unit": "Rupiah per month", "source": column},
            )

        return results

    def calculate_poverty_headcount(
        self,
        by: Optional[List[str]] = None,
        confidence_level: float = 0.95,
    ) -> Dict[str, IndicatorResult]:
        """Calculate poverty headcount rate (P0) using KAPITA from blok43."""

        # import from indicators module
        from ..indicators import calculate_poverty_headcount
        from ..loaders.bps_api import fetch_poverty_lines

        # check if this is SUSENAS data with KAPITA
        if "KAPITA" not in self.data.columns and "kapita" not in self.data.columns:
            return {}

        # ensure required columns for poverty calculation
        df_for_poverty = self.data
        if "wert" not in df_for_poverty.columns and "WERT" not in df_for_poverty.columns:
            # use survey_weight as wert if not present
            if "survey_weight" in df_for_poverty.columns:
                df_for_poverty = df_for_poverty.with_columns(pl.col("survey_weight").alias("wert"))

        try:
            # fetch poverty lines
            poverty_lines = fetch_poverty_lines(2024, "march")
        except Exception:
            # use default if API fails
            poverty_lines = {("INDONESIA", "urban"): 601871, ("INDONESIA", "rural"): 556874}

        # calculate poverty
        result_df = calculate_poverty_headcount(
            df_for_poverty,
            poverty_lines,
        )

        # convert to IndicatorResult format
        poverty_rate = result_df["poverty_rate_pct"][0]
        poor_pop = result_df["poor_population"][0]
        total_pop = result_df["total_population"][0]

        # create simple estimate for now
        estimate = SurveyEstimate(
            value=poverty_rate,
            se=0.0,  # would need bootstrap for SE
            ci_low=poverty_rate,
            ci_high=poverty_rate,
            df=len(self.data) - 1,
        )

        return {
            "overall": IndicatorResult(
                indicator_name="Poverty Headcount Rate (P0)",
                estimate=estimate,
                domain=None,
                metadata={
                    "poor_population": poor_pop,
                    "total_population": total_pop,
                },
            )
        }

    def calculate_poverty_gap(
        self,
        by: Optional[List[str]] = None,
        confidence_level: float = 0.95,
    ) -> Dict[str, IndicatorResult]:
        """Calculate poverty gap index (P1)."""

        from ..indicators import calculate_poverty_fgt
        from ..loaders.bps_api import fetch_poverty_lines

        if "KAPITA" not in self.data.columns and "kapita" not in self.data.columns:
            return {}

        # ensure required columns for poverty calculation
        df_for_poverty = self.data
        if "wert" not in df_for_poverty.columns and "WERT" not in df_for_poverty.columns:
            if "survey_weight" in df_for_poverty.columns:
                df_for_poverty = df_for_poverty.with_columns(pl.col("survey_weight").alias("wert"))

        try:
            poverty_lines = fetch_poverty_lines(2024, "march")
        except Exception:
            poverty_lines = {("INDONESIA", "urban"): 601871, ("INDONESIA", "rural"): 556874}

        # P1 has alpha=1
        result_df = calculate_poverty_fgt(
            df_for_poverty,
            poverty_lines,
            alpha=1,
        )

        # extract value
        gap_value = float(result_df["fgt_index"][0]) if "fgt_index" in result_df.columns else 0.0

        estimate = SurveyEstimate(
            value=gap_value,
            se=0.0,
            ci_low=gap_value,
            ci_high=gap_value,
            df=len(self.data) - 1,
        )

        return {
            "overall": IndicatorResult(
                indicator_name="Poverty Gap Index (P1)",
                estimate=estimate,
                domain=None,
                metadata={},
            )
        }

    def calculate_poverty_severity(
        self,
        by: Optional[List[str]] = None,
        confidence_level: float = 0.95,
    ) -> Dict[str, IndicatorResult]:
        """Calculate poverty severity index (P2)."""

        from ..indicators import calculate_poverty_fgt
        from ..loaders.bps_api import fetch_poverty_lines

        if "KAPITA" not in self.data.columns and "kapita" not in self.data.columns:
            return {}

        # ensure required columns for poverty calculation
        df_for_poverty = self.data
        if "wert" not in df_for_poverty.columns and "WERT" not in df_for_poverty.columns:
            if "survey_weight" in df_for_poverty.columns:
                df_for_poverty = df_for_poverty.with_columns(pl.col("survey_weight").alias("wert"))

        try:
            poverty_lines = fetch_poverty_lines(2024, "march")
        except Exception:
            poverty_lines = {("INDONESIA", "urban"): 601871, ("INDONESIA", "rural"): 556874}

        # P2 has alpha=2
        result_df = calculate_poverty_fgt(
            df_for_poverty,
            poverty_lines,
            alpha=2,
        )

        severity_value = (
            float(result_df["fgt_index"][0]) if "fgt_index" in result_df.columns else 0.0
        )

        estimate = SurveyEstimate(
            value=severity_value,
            se=0.0,
            ci_low=severity_value,
            ci_high=severity_value,
            df=len(self.data) - 1,
        )

        return {
            "overall": IndicatorResult(
                indicator_name="Poverty Severity Index (P2)",
                estimate=estimate,
                domain=None,
                metadata={},
            )
        }

    def calculate_gini_coefficient(
        self,
        by: Optional[List[str]] = None,
        confidence_level: float = 0.95,
    ) -> Dict[str, IndicatorResult]:
        """Calculate Gini coefficient for inequality measurement."""

        from ..indicators import calculate_gini

        # determine value column
        value_col = next(
            (
                col
                for col in ("KAPITA", "kapita", "per_capita_expenditure")
                if col in self.data.columns
            ),
            None,
        )

        if value_col is None:
            return {}

        # determine weight column
        weight_col = next(
            (
                col
                for col in ("WEIND", "weind", "survey_weight", "weight")
                if col in self.data.columns
            ),
            None,
        )

        if weight_col is None:
            weight_col = self.design.weight_col

        # calculate gini
        gini_value = calculate_gini(
            self.data,
            weight_col=weight_col,
            value_col=value_col,
        )

        estimate = SurveyEstimate(
            value=gini_value,
            se=0.0,
            ci_low=gini_value,
            ci_high=gini_value,
            df=len(self.data) - 1,
        )

        return {
            "overall": IndicatorResult(
                indicator_name="Gini Coefficient",
                estimate=estimate,
                domain=None,
                metadata={
                    "value_column": value_col,
                },
            )
        }


def calculate_indicators(
    survey_design: SurveyDesign,
    indicators: Optional[List[str]] = None,
    by: Optional[List[str]] = None,
    confidence_level: float = 0.95,
    as_table: bool = True,  # Default to table format
    include_ci: bool = False,  # Include confidence intervals and std error
    **kwargs,
) -> Dict[str, Dict[str, IndicatorResult]]:
    """Calculate specified labor force indicators.

    Args:
        survey_design: Survey design object
        indicators: List of indicators to calculate, or "all" for all indicators
        by: Grouping variables for domain estimation
        confidence_level: Confidence level for intervals (default: 0.95)
        as_table: If True, return results formatted as a DataFrame table (default: True)
        include_ci: If True, include confidence intervals and standard errors (default: False)
        **kwargs: Additional arguments for specific indicators

    Returns:
        Dictionary of indicator results by indicator and domain, or DataFrame if as_table=True

    Example:
        >>> import statskita as sk
        >>> df = sk.load_sakernas("data.sav")
        >>> design = sk.declare_survey(df, weight="WEIGHT")
        >>> # Calculate specific indicators
        >>> results = sk.calculate_indicators(design, ["lfpr", "unemployment_rate"])
        >>> # Calculate all indicators as table
        >>> results = sk.calculate_indicators(design, "all", as_table=True)
        >>> # Indonesian aliases (backward compatibility)
        >>> results = sk.calculate_indicators(design, ["tpak", "tpt"], by=["province_code"])
    """
    calculator = IndicatorCalculator(survey_design)
    results = {}

    # Primary English indicator methods
    indicator_methods = {
        "labor_force_participation_rate": calculator.calculate_labor_force_participation_rate,
        "female_labor_force_participation_rate": calculator.calculate_female_labor_force_participation_rate,
        "unemployment_rate": calculator.calculate_unemployment_rate,
        "employment_rate": calculator.calculate_employment_rate,
        "neet_rate": calculator.calculate_neet_rate,
        "underemployment_rate": calculator.calculate_underemployment_rate,
        "inactivity_rate": calculator.calculate_inactivity_rate,
        "average_wage": calculator.calculate_average_wage,
        "informal_employment_rate": calculator.calculate_informal_employment_rate,
        "per_capita_expenditure": calculator.calculate_per_capita_expenditure_indicator,
        "poverty_headcount": calculator.calculate_poverty_headcount,
        "poverty_gap": calculator.calculate_poverty_gap,
        "poverty_severity": calculator.calculate_poverty_severity,
        "gini_coefficient": calculator.calculate_gini_coefficient,
    }

    # Comprehensive aliases for convenience
    aliases = {
        # Short English alias (only widely recognized abbreviations)
        "lfpr": "labor_force_participation_rate",  # common abbreviation
        "flfpr": "female_labor_force_participation_rate",
        # Indonesian names (BPS official terminology)
        "tpak": "labor_force_participation_rate",  # Tingkat Partisipasi Angkatan Kerja
        "tpak_perempuan": "female_labor_force_participation_rate",
        "tpt": "unemployment_rate",  # Tingkat Pengangguran Terbuka
        "tingkat_kerja": "employment_rate",  # Tingkat Kesempatan Kerja
        "tingkat_kesempatan_kerja": "employment_rate",
        "setengah_menganggur": "underemployment_rate",
        "tingkat_ketidakaktifan": "inactivity_rate",
        "rata_rata_upah": "average_wage",
        "tingkat_informal": "informal_employment_rate",
        "kegiatan_informal": "informal_employment_rate",
        "tingkat_kegiatan_informal": "informal_employment_rate",
        # Common English variants (descriptive, not cryptic)
        "labour_force_participation_rate": "labor_force_participation_rate",
        "labour_force_rate": "labor_force_participation_rate",
        "female_lfpr": "female_labor_force_participation_rate",
        "female_labour_force_participation_rate": "female_labor_force_participation_rate",
        "unemployment": "unemployment_rate",
        "employment": "employment_rate",
        "underemployment": "underemployment_rate",
        "informal": "informal_employment_rate",
        "informal_employment": "informal_employment_rate",
        "wage": "average_wage",
        "wages": "average_wage",
        "neet": "neet_rate",
        "inactivity": "inactivity_rate",
        "per_capita_expenditure": "per_capita_expenditure",
        # poverty and inequality indicators
        "p0": "poverty_headcount",
        "p1": "poverty_gap",
        "p2": "poverty_severity",
        "gini": "gini_coefficient",
        "poverty_rate": "poverty_headcount",
        "poverty": "poverty_headcount",
    }

    # Handle indicators="all" or indicators=None
    if indicators == "all" or indicators is None:
        indicators_to_calc = list(indicator_methods.keys())
    elif isinstance(indicators, str):
        # Single indicator as string
        indicators_to_calc = [indicators]
    else:
        indicators_to_calc = indicators

    for indicator in indicators_to_calc:
        # resolve aliases to primary English names
        english_name = aliases.get(indicator, indicator)

        if english_name not in indicator_methods:
            available = list(indicator_methods.keys()) + list(aliases.keys())
            raise ValueError(f"Unknown indicator: {indicator}. Available: {available}")

        method = indicator_methods[english_name]

        # call method with appropriate arguments based on English name
        if english_name == "neet_rate":
            results[indicator] = method(
                by=by,
                confidence_level=confidence_level,
                age_range=kwargs.get("age_range", (15, 24)),
            )
        elif english_name in [
            "labor_force_participation_rate",
            "female_labor_force_participation_rate",
            "employment_rate",
            "inactivity_rate",
        ]:
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
        elif english_name in [
            "poverty_headcount",
            "poverty_gap",
            "poverty_severity",
            "gini_coefficient",
        ]:
            results[indicator] = method(
                by=by,
                confidence_level=confidence_level,
            )
        else:  # unemployment_rate, average_wage, informal_employment_rate, per_capita_expenditure
            results[indicator] = method(by=by, confidence_level=confidence_level)

    # Convert to table format if requested (default: True)
    if as_table:
        return format_indicators_as_table(results, include_ci=include_ci)

    return results


def format_indicators_as_table(
    results: Dict[str, Dict[str, IndicatorResult]], include_ci: bool = False
) -> pl.DataFrame:
    """Format indicator results as a DataFrame table.

    Args:
        results: Dictionary of indicator results
        include_ci: Include confidence intervals and standard error columns

    Returns:
        DataFrame with columns: indicator, domain, estimate, and optionally std_error, ci_lower, ci_upper
    """
    rows = []
    has_multiple_domains = False

    # Check if we have domain-specific results (not just "overall")
    for indicator_name, domains in results.items():
        if len(domains) > 1 or (len(domains) == 1 and "overall" not in domains):
            has_multiple_domains = True
            break

    for indicator_name, domains in results.items():
        for domain, result in domains.items():
            estimate = result.estimate

            # Format estimate based on indicator type
            if indicator_name in ["average_wage", "rata_rata_upah"]:
                # format wage in millions for readability
                estimate_val = round(estimate.value / 1_000_000, 2)  # convert to millions
            else:
                estimate_val = round(estimate.value, 2)  # percentages

            row = {
                "indicator": indicator_name,
                "unit": INDICATOR_UNITS.get(indicator_name, ""),
                "estimate": estimate_val,
            }

            # Only include domain column if we have multiple domains
            if has_multiple_domains:
                row["domain"] = domain if domain != "overall" else "Total"

            if include_ci:
                # Format CI/SE based on indicator type
                if indicator_name in ["average_wage", "rata_rata_upah"]:
                    row.update(
                        {
                            "std_error": estimate.se,
                            "ci_lower": estimate.ci_low,
                            "ci_upper": estimate.ci_high,
                        }
                    )
                else:
                    row.update(
                        {
                            "std_error": round(estimate.se, 2),
                            "ci_lower": round(estimate.ci_low, 2),
                            "ci_upper": round(estimate.ci_high, 2),
                        }
                    )
            rows.append(row)

    if not rows:
        # Return empty DataFrame with correct schema
        schema = {
            "indicator": [],
            "estimate": [],
        }
        if has_multiple_domains:
            schema["domain"] = []
        if include_ci:
            schema.update(
                {
                    "std_error": [],
                    "ci_lower": [],
                    "ci_upper": [],
                }
            )
        return pl.DataFrame(schema)

    df = pl.DataFrame(rows)

    # sort by priority order then domain
    def get_priority(indicator_name):
        try:
            return INDICATOR_PRIORITY.index(indicator_name)
        except ValueError:
            return 999

    df = (
        df.with_columns(
            pl.col("indicator").map_elements(get_priority, return_dtype=pl.Int32).alias("_priority")
        )
        .sort(["_priority"] + (["domain"] if "domain" in df.columns else []))
        .drop("_priority")
    )

    # Add a print method as a convenience
    def print_table():
        print("\n" + "=" * 75)
        print("Labor Force Indicators")
        print("=" * 75)
        for row in df.iter_rows(named=True):
            ind = row["indicator"]
            dom = row["domain"]
            est = row["estimate"]
            if include_ci and "std_error" in row:
                se = row["std_error"]
                print(f"{ind:25} {dom:15} {est:6.2f}% ± {se:4.2f}%")
            else:
                print(f"{ind:25} {dom:15} {est:6.2f}%")
        print("=" * 75)

    # Attach print method to DataFrame
    df.print_table = print_table

    return df
