"""
SAKERNAS Historical Analysis - Multi-Wave Comparison (2023-02 to 2025-02)

Demonstrates:
- Data exploration and field discovery
- Single-wave analysis
- Multi-wave loading and comparison
- Cross-wave indicator trends
- Provincial and sectoral analysis

All 4 waves achieve 100% exact match with BPS official statistics.
"""

# %%
import os
from pathlib import Path

import polars as pl
from dotenv import load_dotenv

import statskita as sk
from statskita.loaders.sakernas import SakernasLoader

load_dotenv()
PARQUET_DIR = Path(os.environ.get("SAKERNAS_PARQUET_DIR", "."))

# %%
# data exploration (single wave)

df_latest = sk.load_sakernas(PARQUET_DIR / "sakernas_2025-02.parquet")
print(f"Loaded 2025-02: {len(df_latest):,} observations")

# %%
# explore available fields
wave = "2025-02"
loader = SakernasLoader()
loader._load_config(wave)

print("\nField categories:")
loader.print_categories()

# %%
print("\nDemographic fields:")
loader.print_labels("demographics")

# %%
print("\nDEM_* variables:")
loader.filter_labels("DEM_*")

# %%
# multi-wave loading (4 validated waves)

waves = ["2023-02", "2023-08", "2024-02", "2025-02"]
datasets = {}

print("\nLoading multiple waves:")
for wave in waves:
    file_path = PARQUET_DIR / f"sakernas_{wave}.parquet"
    datasets[wave] = sk.load_sakernas(file_path, wave=wave)
    print(f"  {wave}: {len(datasets[wave]):,} observations")

# %%
# harmonize and create designs
harmonized = {}
designs = {}

for wave, df in datasets.items():
    harmonized[wave] = sk.wrangle(df, source_wave=wave, harmonize=True)
    designs[wave] = sk.declare_survey(
        harmonized[wave],
        weight="survey_weight",
        strata=None,
        psu="psu"
    )

print(f"\nCreated survey designs for {len(designs)} waves")

# %%
# calculate indicators using multi-wave function

results_wide = sk.calculate_indicators_multi(
    harmonized,
    indicators="all",
    as_wide=True
)

print("\nCross-wave comparison:")
print(results_wide)

# %%
# validation against BPS official
official_rates = {
    "2023-02": 5.45,
    "2023-08": 5.32,
    "2024-02": 4.82,
    "2025-02": 4.76,
}

print("\nValidation against BPS:")
print("Wave     | StatsKita | BPS Official")
for wave in ["2023-02", "2023-08", "2024-02", "2025-02"]:
    # Get unemployment rate from results_wide
    row = results_wide.filter(pl.col("indicator") == "unemployment_rate")
    if len(row) > 0 and wave in row.columns:
        calc = row[wave][0]
        bps = official_rates[wave]
        print(f"{wave} | {calc:6.2f}%   | {bps:6.2f}%")

# %%
# provincial analysis

design_latest = designs["2025-02"]

provincial = sk.calculate_indicators(
    design_latest,
    indicators=["unemployment_rate", "labor_force_participation_rate"],
    by=["province_code"],
    as_table=True,
    include_ci=False
)

print("\nProvincial unemployment (2025-02):")
print("  Top 5 highest:")
print(provincial.sort("estimate", descending=True).head())
print("\n  Top 5 lowest:")
print(provincial.sort("estimate").head())

# %%
# industry sector analysis

industry_results = sk.calculate_indicators(
    design_latest,
    indicators=["labor_force_participation_rate", "informal_employment_rate"],
    by=["industry_sector"],
    as_table=True,
    include_ci=False
)

print("\nIndustry analysis:")
print(industry_results.sort("estimate", descending=True))

# %%
# survey design diagnostics

print("\nSurvey design info (2025-02):")
design_latest.info(stats=True)

# %%
