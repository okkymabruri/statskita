"""Simplified survey design wrapper using samplics for the essentials."""

from typing import Any, Dict, List, Optional

import numpy as np
import polars as pl
from samplics import PopParam, TaylorEstimator


class SurveyEstimate:
    """Point estimate with standard error and confidence interval."""

    __slots__ = (
        "value",
        "se",
        "ci_low",
        "ci_high",
        "df",
        "deff",
    )

    def __init__(
        self,
        value: float,
        se: float,
        ci_low: float,
        ci_high: float,
        df: int,
        deff: float | None = None,
    ):
        self.value = float(value)
        self.se = float(se)
        self.ci_low = float(ci_low)
        self.ci_high = float(ci_high)
        self.df = df
        self.deff = deff  # design effect – optional

    # convenience helpers for different output formats
    def as_pct(self) -> "SurveyEstimate":
        """Return a *new* copy expressed in percent."""
        return SurveyEstimate(
            value=self.value * 100,
            se=self.se * 100,
            ci_low=self.ci_low * 100,
            ci_high=self.ci_high * 100,
            df=self.df,
            deff=self.deff,
        )

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"{self.value:0.2f} ± {self.se:0.2f} "
            f"(95% CI {self.ci_low:0.2f}–{self.ci_high:0.2f})"
        )

    # backward compatibility properties
    @property
    def estimate(self) -> float:
        return self.value

    @property
    def std_error(self) -> float:
        return self.se

    @property
    def lower_ci(self) -> float:
        return self.ci_low

    @property
    def upper_ci(self) -> float:
        return self.ci_high

    @property
    def degrees_freedom(self) -> int:
        return self.df

    @property
    def design_effect(self) -> Optional[float]:
        return self.deff


class SurveyDesign:
    """Survey design object for complex survey data analysis."""

    def __init__(
        self,
        data: pl.DataFrame,
        weight_col: str,
        strata_col: Optional[str] = None,
        psu_col: Optional[str] = None,
        fpc: bool = True,  # finite pop correction
        domain_cols: Optional[List[str]] = None,
    ):
        """Initialize survey design.

        Args:
            data: Survey data as polars DataFrame
            weight_col: Column name for survey weights
            strata_col: Column name for strata
            psu_col: Column name for primary sampling units
            fpc: Whether to apply finite population correction
            domain_cols: Columns defining domains for subgroup analysis
        """
        self.data = data
        self.weight_col = weight_col
        self.strata_col = strata_col
        self.psu_col = psu_col
        self.fpc = fpc
        self.domain_cols = domain_cols or []

        self._validate_design()
        self._setup_samplics_design()

    def _validate_design(self):
        # quick validation
        required_cols = [self.weight_col]
        if self.strata_col:
            required_cols.append(self.strata_col)
        if self.psu_col:
            required_cols.append(self.psu_col)

        missing_cols = [col for col in required_cols if col not in self.data.columns]
        if missing_cols:
            raise ValueError(f"missing: {missing_cols}")

        # check weight validity
        if self.data[self.weight_col].min() <= 0:
            raise ValueError("invalid weights")

    def _setup_samplics_design(self):
        # TODO: polars support would be nice
        self._pd_data = self.data.to_pandas()

        # setup design parameters
        self._design_params = {
            "strata": self._pd_data[self.strata_col] if self.strata_col else None,
            "psu": self._pd_data[self.psu_col] if self.psu_col else None,
            "ssu": None,  # secondary sampling units
            "fpc": None if not self.fpc else "default",  # samplics convention
        }

    def estimate_total(
        self, variable: str, by: Optional[List[str]] = None, confidence_level: float = 0.95
    ) -> Dict[str, SurveyEstimate]:
        """Estimate population total with proper weighting.

        Args:
            variable: Column to sum up
            by: Group by these columns (e.g. ["province", "urban_rural"])
        """
        if variable not in self.data.columns:
            raise ValueError(f"Variable '{variable}' not found in data")

        # prepare data
        y_var = self._pd_data[variable].values
        weights = self._pd_data[self.weight_col].values

        # create taylor estimator object for total estimation
        estimator = TaylorEstimator(param=PopParam.total)

        if by is None:
            # overall estimate
            estimator.estimate(
                y=y_var,
                samp_weight=weights,
                stratum=self._design_params["strata"],
                psu=self._design_params["psu"],
                remove_nan=True,
            )

            margin = estimator.stderror * 1.96  # 95% CI approximation

            return {
                "overall": SurveyEstimate(
                    value=float(estimator.point_est),
                    se=float(estimator.stderror),
                    ci_low=float(estimator.point_est - margin),
                    ci_high=float(estimator.point_est + margin),
                    df=estimator.degree_of_freedom
                    if hasattr(estimator, "degree_of_freedom")
                    else 0,
                )
            }
        else:
            # by domain
            domain_data = self._pd_data[by + [variable, self.weight_col]]
            if self.strata_col:
                domain_data = domain_data.merge(
                    self._pd_data[[self.strata_col]], left_index=True, right_index=True
                )

            results = {}
            for domain_values, group in domain_data.groupby(by):
                if isinstance(domain_values, str) or not hasattr(domain_values, "__iter__"):
                    domain_key = str(domain_values)
                else:
                    domain_key = "_".join(str(v) for v in domain_values)

                y_domain = group[variable].values
                w_domain = group[self.weight_col].values

                if len(y_domain) == 0:
                    continue

                estimator.estimate(
                    y=y_domain,
                    samp_weight=w_domain,
                    stratum=group[self.strata_col].values if self.strata_col else None,
                    psu=group[self.psu_col].values if self.psu_col else None,
                    remove_nan=True,
                )

                margin = estimator.stderror * 1.96

                results[domain_key] = SurveyEstimate(
                    value=float(estimator.point_est),
                    se=float(estimator.stderror),
                    ci_low=float(estimator.point_est - margin),
                    ci_high=float(estimator.point_est + margin),
                    df=estimator.degree_of_freedom
                    if hasattr(estimator, "degree_of_freedom")
                    else 0,
                )

            return results

    def estimate_mean(
        self, variable: str, by: Optional[List[str]] = None, confidence_level: float = 0.95
    ) -> Dict[str, SurveyEstimate]:
        """Estimate population mean with survey design."""
        if variable not in self.data.columns:
            raise ValueError(f"Variable '{variable}' not found in data")

        y_var = self._pd_data[variable].values
        weights = self._pd_data[self.weight_col].values

        estimator = TaylorEstimator(param=PopParam.mean)

        if by is None:
            estimator.estimate(
                y=y_var,
                samp_weight=weights,
                stratum=self._design_params["strata"],
                psu=self._design_params["psu"],
                remove_nan=True,
            )

            margin = estimator.stderror * 1.96

            return {
                "overall": SurveyEstimate(
                    value=float(estimator.point_est),
                    se=float(estimator.stderror),
                    ci_low=float(estimator.point_est - margin),
                    ci_high=float(estimator.point_est + margin),
                    df=estimator.degree_of_freedom
                    if hasattr(estimator, "degree_of_freedom")
                    else 0,
                )
            }
        else:
            # domain estimates similar to total
            results = {}
            domain_data = self._pd_data[by + [variable, self.weight_col]]
            if self.strata_col:
                domain_data = domain_data.merge(
                    self._pd_data[[self.strata_col]], left_index=True, right_index=True
                )
            if self.psu_col:
                domain_data = domain_data.merge(
                    self._pd_data[[self.psu_col]], left_index=True, right_index=True
                )

            for domain_values, group in domain_data.groupby(by):
                domain_key = (
                    str(domain_values)
                    if isinstance(domain_values, str)
                    else "_".join(str(v) for v in domain_values)
                )

                y_domain = group[variable].values
                w_domain = group[self.weight_col].values

                if len(y_domain) == 0:
                    continue

                estimator.estimate(
                    y=y_domain,
                    samp_weight=w_domain,
                    stratum=group[self.strata_col].values if self.strata_col else None,
                    psu=group[self.psu_col].values if self.psu_col else None,
                    remove_nan=True,
                )

                margin = estimator.stderror * 1.96

                results[domain_key] = SurveyEstimate(
                    value=float(estimator.point_est),
                    se=float(estimator.stderror),
                    ci_low=float(estimator.point_est - margin),
                    ci_high=float(estimator.point_est + margin),
                    df=int(estimator.degree_of_freedom)
                    if hasattr(estimator, "degree_of_freedom")
                    else 0,
                )

            return results

    def estimate_proportion(
        self, variable: str, by: Optional[List[str]] = None, confidence_level: float = 0.95
    ) -> Dict[str, SurveyEstimate]:
        """Estimate population proportion with survey design.

        Args:
            variable: Binary variable (0/1 or True/False)
            by: Grouping variables
            confidence_level: Confidence level

        Returns:
            Dictionary of proportion estimates
        """
        if variable not in self.data.columns:
            raise ValueError(f"Variable '{variable}' not found in data")

        # convert to binary if needed
        y_var = self._pd_data[variable].astype(int).values
        weights = self._pd_data[self.weight_col].values

        estimator = TaylorEstimator(param=PopParam.prop)

        if by is None:
            estimator.estimate(
                y=y_var,
                samp_weight=weights,
                stratum=self._design_params["strata"],
                psu=self._design_params["psu"],
                remove_nan=True,
            )

            # samplics returns dict for binary vars - extract proportion of 1s
            if isinstance(estimator.point_est, dict):
                point_est = estimator.point_est.get(1, estimator.point_est.get(np.int64(1), 0))
                stderror = estimator.stderror.get(1, estimator.stderror.get(np.int64(1), 0))
            else:
                point_est = float(estimator.point_est)
                stderror = float(estimator.stderror)

            margin = stderror * 1.96

            return {
                "overall": SurveyEstimate(
                    value=float(point_est),
                    se=float(stderror),
                    ci_low=float(max(0, point_est - margin)),
                    ci_high=float(min(1, point_est + margin)),
                    df=estimator.degree_of_freedom
                    if hasattr(estimator, "degree_of_freedom")
                    else 0,
                )
            }
        else:
            results = {}
            domain_data = self._pd_data[by + [variable, self.weight_col]]
            if self.strata_col:
                domain_data = domain_data.merge(
                    self._pd_data[[self.strata_col]], left_index=True, right_index=True
                )
            if self.psu_col:
                domain_data = domain_data.merge(
                    self._pd_data[[self.psu_col]], left_index=True, right_index=True
                )

            for domain_values, group in domain_data.groupby(by):
                domain_key = (
                    str(domain_values)
                    if isinstance(domain_values, str)
                    else "_".join(str(v) for v in domain_values)
                )

                y_domain = group[variable].astype(int).values
                w_domain = group[self.weight_col].values

                if len(y_domain) == 0:
                    continue

                estimator.estimate(
                    y=y_domain,
                    samp_weight=w_domain,
                    stratum=group[self.strata_col].values if self.strata_col else None,
                    psu=group[self.psu_col].values if self.psu_col else None,
                    remove_nan=True,
                )

                # handle dict return for binary variables
                if isinstance(estimator.point_est, dict):
                    point_est = estimator.point_est.get(1, estimator.point_est.get(np.int64(1), 0))
                    stderror = estimator.stderror.get(1, estimator.stderror.get(np.int64(1), 0))
                else:
                    point_est = float(estimator.point_est)
                    stderror = float(estimator.stderror)

                margin = stderror * 1.96

                results[domain_key] = SurveyEstimate(
                    value=float(point_est),
                    se=float(stderror),
                    ci_low=float(max(0, point_est - margin)),
                    ci_high=float(min(1, point_est + margin)),
                    df=int(estimator.degree_of_freedom)
                    if hasattr(estimator, "degree_of_freedom")
                    else 0,
                )

            return results

    def get_design_summary(self) -> Dict[str, Any]:
        """Get summary of survey design."""
        return {
            "sample_size": len(self.data),
            "weight_col": self.weight_col,
            "strata_col": self.strata_col,
            "psu_col": self.psu_col,
            "n_strata": len(self.data[self.strata_col].unique()) if self.strata_col else None,
            "n_psu": len(self.data[self.psu_col].unique()) if self.psu_col else None,
            "weight_range": (
                float(self.data[self.weight_col].min()),
                float(self.data[self.weight_col].max()),
            ),
            "fpc": self.fpc,
            "domain_cols": self.domain_cols,
        }


def declare_survey(
    data: pl.DataFrame,
    weight: str,
    strata: Optional[str] = None,
    psu: Optional[str] = None,
    fpc: bool = True,
    domain_cols: Optional[List[str]] = None,
) -> SurveyDesign:
    """Declare survey design for existing data.

    Specifies survey design characteristics to calculate
    proper standard errors using Taylor linearization.

    Args:
        data: Survey dataframe
        weight: Column with survey weights
        strata: Stratification column (if stratified sampling)
        psu: Primary sampling unit/cluster column
        fpc: Use finite population correction (default True)

    Example:
        >>> spec = sk.declare_survey(df, weight="WEIGHT", strata="STRATA")
        >>> tpak = spec.estimate_proportion("in_labor_force")
    """
    return SurveyDesign(
        data=data,
        weight_col=weight,
        strata_col=strata,
        psu_col=psu,
        fpc=fpc,
        domain_cols=domain_cols,
    )


# Stata-style alias for compatibility
def svyset(*args, **kwargs) -> SurveyDesign:
    """Stata-style alias for declare_survey. Same params."""
    # shorter to type, familiar to Stata users
    return declare_survey(*args, **kwargs)
