"""
SAKERNAS Analysis - Comparing August 2024 and February 2025
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
# Load both waves
df_feb = sk.load_sakernas(PARQUET_DIR / "sakernas_2025-02.parquet")
df_aug = sk.load_sakernas(PARQUET_DIR / "sakernas_2024-08.parquet")

# %%
# Explore available fields
wave = "2025-02"  # change to "2024-08" to explore August fields
loader = SakernasLoader()
loader._load_config(wave)
loader.print_categories()
# %%
loader.print_labels("demographics")
# %%
loader.filter_labels("DEM_*")
# %%
# Harmonize
sak_feb = sk.wrangle(df_feb, harmonize=True, source_wave="2025-02")
sak_aug = sk.wrangle(df_aug, harmonize=True, source_wave="2024-08")

# %%
# Create survey designs
design_feb = sk.declare_survey(
    sak_feb,
    weight="WEIGHT",
    strata="STRATA",
    psu="PSU",
    ssu="SSU"
)

design_aug = sk.declare_survey(
    sak_aug,
    weight="WEIGHT",
    strata=None,  # avoid singleton PSU issues
    psu="PSU"
)

# %%
# Calculate indicators
results_feb = sk.calculate_indicators(design_feb, indicators="all", as_table=True)
results_aug = sk.calculate_indicators(design_aug, indicators="all", as_table=True)

# %%
# Combine and compare
results_feb = results_feb.with_columns(pl.lit("2025-02").alias("wave"))
results_aug = results_aug.with_columns(pl.lit("2024-08").alias("wave"))

combined = pl.concat([results_aug, results_feb])

# Pivot to get waves as columns
combined_pivot = combined.pivot(
    values="estimate",
    index="indicator",
    on="wave",
    aggregate_function="first"
).sort("indicator")

# Add change columns
combined_pivot = combined_pivot.with_columns(
    (pl.col("2025-02") - pl.col("2024-08")).alias("change"),
    ((pl.col("2025-02") - pl.col("2024-08")) / pl.col("2024-08") * 100).alias("change_pct")
)

print(combined_pivot)

# %%
# Provincial LFPR for Feb 2025
provincial_results = sk.calculate_indicators(
    design_feb,
    indicators=["labor_force_participation_rate"],
    by=["province_code"],
    as_table=True
).sort("estimate")

print(provincial_results)
# %%
# Get survey design info
design_aug.info(stats=True)  # Pretty print with weight diagnostics
# %%
# Industry analysis
industry_results = sk.calculate_indicators(
    design_feb,
    indicators=["labor_force_participation_rate", "informal_employment_rate"],
    by=["industry_sector"],
    as_table=True
)

print(industry_results)

# %%
