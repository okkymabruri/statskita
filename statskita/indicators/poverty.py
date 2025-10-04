"""Poverty indicators for SUSENAS data.

Key findings:
- Exclude single-person households (R301=1) to match BPS methodology
- Use KAPITA from blok43 when available (Stata files)
- DBF files have NULL KAPITA - must calculate from components
"""

import warnings
from typing import Dict, Optional, Tuple

import polars as pl

from ..core.survey import SurveyDesign


def calculate_poverty_fgt(
    df_blok43: pl.DataFrame,
    poverty_lines: Dict[Tuple[str, str], float],
    alpha: float = 0,
    exclude_single_person: bool = False,
    design: Optional[SurveyDesign] = None,
) -> pl.DataFrame:
    """Calculate Foster-Greer-Thorbecke (FGT) poverty measures.

    FGT measures:
    - alpha=0: P0 (Headcount ratio)
    - alpha=1: P1 (Poverty gap index)
    - alpha=2: P2 (Poverty severity index)

    Args:
        df_blok43: SUSENAS blok43 data with KAPITA column
        poverty_lines: Dict mapping (province, urban/rural) to poverty lines
        alpha: FGT parameter (0, 1, or 2)
        exclude_single_person: Whether to exclude single-person households (R301=1)
                             Set True to match BPS methodology (9.03%)
        design: Survey design (optional)

    Returns:
        DataFrame with FGT poverty measure
    """
    # apply single-person exclusion to match BPS methodology
    if exclude_single_person:
        df_blok43 = df_blok43.filter(pl.col("R301") != 1)

    # continue with existing implementation
    return _calculate_poverty_internal(df_blok43, poverty_lines, alpha, design)


def calculate_poverty_headcount(
    df_blok43: pl.DataFrame,
    poverty_lines: Dict[Tuple[str, str], float],
    exclude_single_person: bool = False,
    design: Optional[SurveyDesign] = None,
) -> pl.DataFrame:
    """Calculate poverty headcount rate using KAPITA from blok43.

    Uses BPS pre-calculated KAPITA values when available (Stata files).
    Set exclude_single_person=True to match BPS methodology.

    Returns DataFrame with poverty_rate_pct, poor_population, total_population.
    """
    # apply single-person exclusion to match BPS methodology
    if exclude_single_person:
        df_blok43 = df_blok43.filter(pl.col("R301") != 1)

    # check if KAPITA exists and is populated
    if "KAPITA" not in df_blok43.columns and "kapita" not in df_blok43.columns:
        raise ValueError(
            "KAPITA column not found in blok43 data. Provide blok43 data that includes KAPITA "
            "before computing poverty indicators."
        )

    # normalize column names
    df = df_blok43.rename({c: c.lower() for c in df_blok43.columns})

    # check if KAPITA has values
    kapita_non_null = df["kapita"].drop_nulls().shape[0]

    if kapita_non_null == 0:
        raise ValueError(
            "KAPITA values are NULL in blok43. Provide blok43 data that includes KAPITA before "
            "computing poverty indicators."
        )

    print(f"Using KAPITA from blok43 ({kapita_non_null:,} households)")

    # normalize other columns
    required_cols = ["urut", "kapita", "r105", "r101", "r301", "wert"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # apply poverty lines by R105 (urban=1, rural=2)
    def get_poverty_line(prov_code: int, urban_rural: int) -> float:
        province_name = PROVINCE_CODE_TO_NAME.get(int(prov_code))
        category = "urban" if urban_rural == 1 else "rural"

        # try province-specific first
        if province_name:
            line = poverty_lines.get((province_name, category))
            if line is not None:
                return float(line)

        # fallback to national
        national_line = poverty_lines.get(("INDONESIA", category))
        if national_line is not None:
            return float(national_line)

        # hard-coded fallback
        return 601871.0 if urban_rural == 1 else 556874.0

    df = df.with_columns(
        [
            pl.struct(["r101", "r105"])
            .map_elements(
                lambda row: get_poverty_line(row["r101"], row["r105"]), return_dtype=pl.Float64
            )
            .alias("poverty_line")
        ]
    )

    # identify poor: KAPITA < poverty_line
    df = df.with_columns(
        [
            (pl.col("kapita") < pl.col("poverty_line")).alias("is_poor"),
        ]
    )

    # calculate WEIGHTED poverty rate (individual-level)
    total_pop = (df["r301"] * df["wert"]).sum()
    poor_pop = (df.filter(pl.col("is_poor"))["r301"] * df.filter(pl.col("is_poor"))["wert"]).sum()

    if total_pop == 0:
        warnings.warn("Total population weight is zero")
        poverty_rate = float("nan")
    else:
        poverty_rate = poor_pop / total_pop * 100

    # return summary
    return pl.DataFrame(
        {
            "indicator": ["poverty_headcount"],
            "poverty_rate_pct": [poverty_rate],
            "poor_population": [poor_pop],
            "total_population": [total_pop],
        }
    )


PROVINCE_CODE_TO_NAME = {
    11: "ACEH",
    12: "SUMATERA UTARA",
    13: "SUMATERA BARAT",
    14: "RIAU",
    15: "JAMBI",
    16: "SUMATERA SELATAN",
    17: "BENGKULU",
    18: "LAMPUNG",
    19: "BANGKA BELITUNG",
    21: "KEPULAUAN RIAU",
    31: "DKI JAKARTA",
    32: "JAWA BARAT",
    33: "JAWA TENGAH",
    34: "DI YOGYAKARTA",
    35: "JAWA TIMUR",
    36: "BANTEN",
    51: "BALI",
    52: "NUSA TENGGARA BARAT",
    53: "NUSA TENGGARA TIMUR",
    61: "KALIMANTAN BARAT",
    62: "KALIMANTAN TENGAH",
    63: "KALIMANTAN SELATAN",
    64: "KALIMANTAN TIMUR",
    65: "KALIMANTAN UTARA",
    71: "SULAWESI UTARA",
    72: "SULAWESI TENGAH",
    73: "SULAWESI SELATAN",
    74: "SULAWESI TENGGARA",
    75: "GORONTALO",
    76: "SULAWESI BARAT",
    81: "MALUKU",
    82: "MALUKU UTARA",
    91: "PAPUA BARAT",
    92: "PAPUA BARAT DAYA",
    94: "PAPUA",
    95: "PAPUA SELATAN",
    96: "PAPUA TENGAH",
    97: "PAPUA PEGUNUNGAN",
}


def _calculate_poverty_internal(
    df_blok43: pl.DataFrame,
    poverty_lines: Dict[Tuple[str, str], float],
    alpha: float = 0,
    design: Optional[SurveyDesign] = None,
) -> pl.DataFrame:
    """Internal function for FGT poverty calculations.

    Implements Foster-Greer-Thorbecke formula:
    FGT(alpha) = (1/n) * Σ((z - y_i)/z)^alpha for y_i < z

    Where:
    - z = poverty line
    - y_i = per capita expenditure
    - alpha = 0 (headcount), 1 (gap), 2 (severity)
    """
    # normalize column names
    df = df_blok43.rename({c: c.lower() for c in df_blok43.columns})

    # check KAPITA exists
    if "kapita" not in df.columns:
        raise ValueError("KAPITA column not found")

    # ensure required columns exist
    required_cols = ["urut", "kapita", "r105", "r101", "r301"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # use WEIND for individual-level statistics if available, else R301×WERT
    # Note: In SUSENAS 2024-03, WEIND = R301×WERT exactly (validated)
    if "weind" in df.columns:
        df = df.with_columns(pl.col("weind").cast(pl.Float64).alias("individual_weight"))
    elif "wert" in df.columns:
        df = df.with_columns(
            (pl.col("r301").cast(pl.Float64) * pl.col("wert").cast(pl.Float64)).alias(
                "individual_weight"
            )
        )
    else:
        raise ValueError("No weight column found (WEIND or WERT)")

    # use individual_weight for calculations
    weight_col = "individual_weight"

    # apply poverty lines
    def get_poverty_line(prov_code: int, urban_rural: int) -> float:
        province_name = PROVINCE_CODE_TO_NAME.get(int(prov_code))
        category = "urban" if urban_rural == 1 else "rural"

        if province_name:
            line = poverty_lines.get((province_name, category))
            if line is not None:
                return float(line)

        # fallback to national
        national_line = poverty_lines.get(("INDONESIA", category))
        if national_line is not None:
            return float(national_line)

        # hard-coded fallback
        return 601871.0 if urban_rural == 1 else 556874.0

    df = df.with_columns(
        [
            pl.struct(["r101", "r105"])
            .map_elements(
                lambda row: get_poverty_line(row["r101"], row["r105"]), return_dtype=pl.Float64
            )
            .alias("poverty_line")
        ]
    )

    # calculate FGT measure
    if alpha == 0:
        # P0: headcount ratio
        df = df.with_columns(
            [
                (pl.col("kapita") < pl.col("poverty_line")).alias("is_poor"),
            ]
        )

        total_pop = df[weight_col].sum()
        poor_pop = df.filter(pl.col("is_poor"))[weight_col].sum()

        fgt_value = poor_pop / total_pop if total_pop > 0 else 0.0
    else:
        # P1 (gap) and P2 (severity)
        df = df.with_columns(
            [
                pl.when(pl.col("kapita") < pl.col("poverty_line"))
                .then(
                    ((pl.col("poverty_line") - pl.col("kapita")) / pl.col("poverty_line")) ** alpha
                )
                .otherwise(0.0)
                .alias("gap_contribution")
            ]
        )

        # weighted mean of gap contributions
        total_weight = df[weight_col].sum()
        weighted_sum = (df["gap_contribution"] * df[weight_col]).sum()

        fgt_value = weighted_sum / total_weight if total_weight > 0 else 0.0

    # scale P1 and P2 by 100 to match BPS convention
    if alpha > 0:
        fgt_value = fgt_value * 100

    return pl.DataFrame(
        {
            "fgt_index": [fgt_value],
            "alpha": [alpha],
        }
    )
