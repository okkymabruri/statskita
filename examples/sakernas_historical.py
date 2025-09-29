"""
SAKERNAS Analysis - Testing All StatsKita Features

Tests all statskita functions with recent SAKERNAS data:
- load_sakernas: Load from .parquet/.dta/.sav/.dbf
- wrangle: Harmonize and clean data
- declare_survey: Create survey design
- calculate_indicators: Compute labor force indicators
- export_*: Export to various formats (skip for now)

Run `python dev/convert_sakernas_to_parquet.py` first to convert to .parquet
"""
# %%
# Setup
import os
from pathlib import Path

import polars as pl
from dotenv import load_dotenv

import statskita as sk
from statskita.core.harmonizer import SurveyHarmonizer
from statskita.loaders.sakernas import SakernasLoader

# load environment variables
load_dotenv()

# data paths
DATA_DIR = Path(os.environ.get("SAKERNAS_RAW_DIR", "."))
PARQUET_DIR = Path(os.environ.get("SAKERNAS_PARQUET_DIR", "."))

# %%
# 1: Load SAKERNAS data
print("Loading SAKERNAS Feb 2025...")
df = sk.load_sakernas(PARQUET_DIR / "sakernas_2025-02.parquet")
print(f"Loaded {len(df):,} observations")
df
# %%
# 1b: Explore available fields
loader = SakernasLoader()
loader._load_config("2025-02")
loader.print_categories()  # show available categories
# %%
loader.print_labels("demographics")
# %%
loader.filter_labels("DEM_B*")
# %%
# 2: Wrangle and harmonize data
print("\nHarmonizing data...")

# Use automatic harmonization - harmonizer now reads from YAML configs
sak_df = sk.wrangle(
    df,
    harmonize=True,
    source_wave="2025-02",
    fix_types=True,
    validate_weights=True,
    create_indicators=True,
)

sak_df
# %%
# 3: Declare survey design
print("3: Declare survey design")
design = sk.declare_survey(
    sak_df,
    weight="WEIGHT",         # harmonized weight column
    strata="STRATA",         # stratification variable
    psu="PSU",               # primary sampling unit
    ssu="SSU"                # secondary sampling unit
)
print(f"Created survey design with {len(design.data):,} observations")
# %%
# 4: Calculate labor force indicators

# Option 1: Calculate all indicators and return as table (NEW!)
results_table = sk.calculate_indicators(
    design,
    indicators="all",  # Calculate all available indicators
    as_table=True      # Return as DataFrame table
)
results_table
# %%
# 5: Domain analysis
print("\nCalculating provincial LFPR...")

# calculate by province
provincial_results = sk.calculate_indicators(
    design,
    indicators=["lfpr"],
    by=["province_code"], as_table=True
)

provincial_results.sort("estimate")
# %%
# 6: Additional features
# Use new improved methods
design.info(stats=True)  # Pretty print with weight diagnostics

# Or get summary as dict
summary = design.summary(stats=True)
print(f"\nKish ESS: {summary['kish_ess']:,.0f} (effective sample size)")
print(f"CV of weights: {summary['cv_weights']:.3f}")
summary
# %%
# 7: Validate harmonization
harmonizer = SurveyHarmonizer(dataset_type="sakernas")
validation = harmonizer.validate_harmonization(df, sak_df, "2025-02")
validation
# %%
# 7: Conversion utilities
# Available: dbf_to_parquet(), dta_to_parquet(), batch_convert_*()

# %%
# 8: Industry sector analysis
sk.calculate_indicators(
    design,
    indicators=["lfpr"],
    by=["industry_sector"], as_table=True
)
# %%
