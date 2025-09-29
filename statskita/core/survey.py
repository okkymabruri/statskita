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
        return f"{self.value:0.2f} ± {self.se:0.2f} (95% CI {self.ci_low:0.2f}–{self.ci_high:0.2f})"

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
        ssu_col: Optional[str] = None,
        fpc: bool = True,  # finite pop correction
        domain_cols: Optional[List[str]] = None,
    ):
        """Initialize survey design.

        Args:
            data: Survey data as polars DataFrame
            weight_col: Column name for survey weights
            strata_col: Column name for strata
            psu_col: Column name for primary sampling units
            ssu_col: Column name for secondary sampling units
            fpc: Whether to apply finite population correction
            domain_cols: Columns defining domains for subgroup analysis
        """
        self.data = data
        self.weight_col = weight_col
        self.strata_col = strata_col
        self.psu_col = psu_col
        self.ssu_col = ssu_col
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
        if self.ssu_col:
            required_cols.append(self.ssu_col)

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
            "ssu": self._pd_data[self.ssu_col] if self.ssu_col else None,
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

                y_domain = group[variable].astype(float).values
                w_domain = group[self.weight_col].astype(float).values

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

        # Convert to float to handle Decimal types
        y_var = self._pd_data[variable].astype(float).values
        weights = self._pd_data[self.weight_col].astype(float).values

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

                y_domain = group[variable].astype(float).values
                w_domain = group[self.weight_col].astype(float).values

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

        # convert to binary if needed, handling nulls
        y_var = self._pd_data[variable].fillna(0).astype(int).values
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

                # Get strata and PSU values
                strata_values = group[self.strata_col].values if self.strata_col else None
                psu_values = group[self.psu_col].values if self.psu_col else None

                # Handle singleton PSUs by collapsing strata
                if strata_values is not None and psu_values is not None:
                    import pandas as pd
                    strata_psu_df = pd.DataFrame({'strata': strata_values, 'psu': psu_values})
                    psu_counts = strata_psu_df.groupby('strata')['psu'].nunique()
                    singleton_strata = psu_counts[psu_counts == 1].index

                    if len(singleton_strata) > 0:
                        # Collapse singleton strata - combine with nearest stratum
                        strata_values_adj = strata_values.copy()
                        all_strata = sorted(psu_counts.index)

                        for s in singleton_strata:
                            # Find the closest non-singleton stratum
                            non_singleton = psu_counts[psu_counts > 1].index
                            if len(non_singleton) > 0:
                                # Find numerically closest stratum
                                s_idx = all_strata.index(s)
                                # Try next stratum first
                                if s_idx + 1 < len(all_strata) and all_strata[s_idx + 1] in non_singleton:
                                    replacement = all_strata[s_idx + 1]
                                # Try previous stratum
                                elif s_idx > 0 and all_strata[s_idx - 1] in non_singleton:
                                    replacement = all_strata[s_idx - 1]
                                # Use any non-singleton
                                else:
                                    replacement = non_singleton[0]
                                strata_values_adj[strata_values == s] = replacement
                        strata_values = strata_values_adj

                try:
                    estimator.estimate(
                        y=y_domain,
                        samp_weight=w_domain,
                        stratum=strata_values,
                        psu=psu_values,
                        remove_nan=True,
                    )
                except Exception as e:
                    # If still fails, try without stratification for this domain
                    if "Only one PSU" in str(e):
                        estimator.estimate(
                            y=y_domain,
                            samp_weight=w_domain,
                            stratum=None,  # Ignore strata for problematic domains
                            psu=psu_values,
                            remove_nan=True,
                        )
                    else:
                        raise

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
        """Get summary of survey design (deprecated, use summary() instead)."""
        import warnings
        warnings.warn("get_design_summary() is deprecated, use summary() instead", DeprecationWarning)
        return self.summary()

    def summary(self, stats: bool = False) -> Dict[str, Any]:
        """Get summary of survey design.

        Args:
            stats: If True, include weight diagnostics (CV, Kish ESS, etc.)

        Returns:
            Dictionary with design summary statistics
        """
        basic = {
            "sample_size": len(self.data),
            "weight_col": self.weight_col,
            "strata_col": self.strata_col,
            "psu_col": self.psu_col,
            "ssu_col": self.ssu_col,
            "n_strata": len(self.data[self.strata_col].unique()) if self.strata_col else None,
            "n_psu": len(self.data[self.psu_col].unique()) if self.psu_col else None,
            "n_ssu": len(self.data[self.ssu_col].unique()) if self.ssu_col else None,
            "weight_range": (
                float(self.data[self.weight_col].min()),
                float(self.data[self.weight_col].max()),
            ),
            "fpc": self.fpc,
            "domain_cols": self.domain_cols,
        }

        if stats and self.weight_col:
            import numpy as np
            w = self.data[self.weight_col].to_numpy()
            basic.update({
                "cv_weights": float(np.std(w, ddof=0) / np.mean(w)),
                "kish_ess": float(np.sum(w)**2 / np.sum(w**2)),
                "median_weight": float(np.median(w)),
            })

        return basic

    def __repr__(self) -> str:
        """String representation of the survey design."""
        s = self.summary()
        return (f"SurveyDesign(n={s['sample_size']:,}, strata={s['n_strata']}, "
                f"psu={s['n_psu']}, weight='{s['weight_col']}')")

    def info(self, stats: bool = False) -> None:
        """Print formatted summary of survey design.

        Args:
            stats: If True, include weight diagnostics
        """
        s = self.summary(stats=stats)

        print("\n" + "="*60)
        print("Survey Design Summary")
        print("="*60)
        print(f"Sample size:         {s['sample_size']:,}")
        print(f"Weight column:       {s['weight_col']}")
        print(f"Strata:              {s['n_strata']} (column: {s['strata_col']})")
        print(f"PSUs:                {s['n_psu']} (column: {s['psu_col']})")
        if s['ssu_col']:
            print(f"SSUs:                {s['n_ssu']} (column: {s['ssu_col']})")
        print(f"Weight range:        {s['weight_range'][0]:.1f} - {s['weight_range'][1]:.1f}")
        print(f"FPC:                 {s['fpc']}")

        if stats and 'cv_weights' in s:
            print("\nWeight Diagnostics:")
            print(f"  CV of weights:     {s['cv_weights']:.3f}")
            print(f"  Kish ESS:          {s['kish_ess']:,.0f}")
            print(f"  Median weight:     {s['median_weight']:.1f}")
        print("="*60)


def declare_survey(
    data: pl.DataFrame,
    weight: str,
    strata: Optional[str] = None,
    psu: Optional[str] = None,
    ssu: Optional[str] = None,
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
        ssu: Secondary sampling unit column
        fpc: Use finite population correction (default True)
        domain_cols: Columns defining domains for subgroup analysis

    Example:
        >>> spec = sk.declare_survey(df, weight="WEIGHT", strata="STRATA", psu="PSU", ssu="SSU")
        >>> tpak = spec.estimate_proportion("in_labor_force")
    """
    return SurveyDesign(
        data=data,
        weight_col=weight,
        strata_col=strata,
        psu_col=psu,
        ssu_col=ssu,
        fpc=fpc,
        domain_cols=domain_cols,
    )


# Stata-style alias for compatibility
def svyset(*args, **kwargs) -> SurveyDesign:
    """Stata-style alias for declare_survey. Same params."""
    # shorter to type, familiar to Stata users
    return declare_survey(*args, **kwargs)
