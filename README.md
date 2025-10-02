# StatsKita: Python toolkit for Indonesian official microdata (SAKERNAS)

> **v0.2.0-beta**: Multi-wave analysis with 100% validation. Production-ready for 4 waves (2023-02 to 2025-02).

## TL;DR

Python toolkit for SAKERNAS labor force survey data with multi-wave analysis.

## Quick Start

```python
import statskita as sk

# load SAKERNAS data (supports .dbf, .dta, .sav, .parquet)
df = sk.load_sakernas("sakernas_2025-02.parquet")

# wrangle and harmonize
clean_df = sk.wrangle(df, harmonize=True, source_wave="2025-02")

# declare survey design
design = sk.declare_survey(clean_df, weight="survey_weight", strata=None, psu="psu")

# calculate indicators
results = sk.calculate_indicators(
    design,
    indicators="all",
    as_table=True,
    include_ci=False
)
```

## Multi-Wave Analysis

```python
# load multiple waves
waves = ["2023-02", "2023-08", "2024-02", "2025-02"]
harmonized = {}

for wave in waves:
    df = sk.load_sakernas(f"sakernas_{wave}.parquet", wave=wave)
    harmonized[wave] = sk.wrangle(df, source_wave=wave, harmonize=True)

# compare across waves
results = sk.calculate_indicators_multi(
    harmonized,
    indicators="all",
    as_wide=True
)

print(results)
```

**Output** (wide-format comparison):
```
┌─────────────────────────────────┬──────┬─────────┬─────────┬─────────┬─────────┐
│ indicator                       ┆ unit ┆ 2023-02 ┆ 2023-08 ┆ 2024-02 ┆ 2025-02 │
├─────────────────────────────────┼──────┼─────────┼─────────┼─────────┼─────────┤
│ labor_force_participation_rate  ┆ %    ┆ ...     ┆ ...     ┆ ...     ┆ ...     │
│ employment_rate                 ┆ %    ┆ ...     ┆ ...     ┆ ...     ┆ ...     │
│ unemployment_rate               ┆ %    ┆ 5.45    ┆ 5.32    ┆ 4.82    ┆ 4.76    │
│ underemployment_rate            ┆ %    ┆ ...     ┆ ...     ┆ ...     ┆ ...     │
│ female_labor_force_participat…  ┆ %    ┆ ...     ┆ ...     ┆ ...     ┆ ...     │
│ average_wage                    ┆ M Rp ┆ ...     ┆ ...     ┆ ...     ┆ ...     │
└─────────────────────────────────┴──────┴─────────┴─────────┴─────────┴─────────┘
```

## Installation

```bash
pip install statskita
```

## Features

- **Multi-wave support**: Compare indicators across 4 validated waves (2023-02 to 2025-02)
- **Multiple formats**: Load .dbf, .dta, .sav, .parquet files
- **Config-driven**: Automatic harmonization across waves
- **Survey-aware**: Proper handling of weights, PSU, strata
- **Fast processing**: Polars backend for large datasets
- **Complete indicators**: LFPR, unemployment, underemployment, wages, and more

See examples/ directory for detailed usage.