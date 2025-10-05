"""
SUSENAS Historical Analysis - Multi-Wave Poverty and Inequality (2023-2024)

Demonstrates:
- Multi-wave loading
- Automatic poverty line loading
- Poverty indicators (P0, P1, P2)
- Inequality measurement (Gini coefficient)

Both waves achieve perfect match with BPS official statistics.
"""

# %%
import os
from pathlib import Path

import polars as pl
from dotenv import load_dotenv

import statskita as sk

load_dotenv()
DATA_DIR = Path(os.environ.get("SUSENAS_DATA_DIR", "."))

# %%
# multi-wave loading

waves = ["2023-03", "2024-03"]
datasets = {}

print("Loading SUSENAS waves:")
for wave in waves:
    datasets[wave] = sk.load_susenas(file_path=DATA_DIR, wave=wave, module="kp", category="housing")
    print(f"  {wave}: {len(datasets[wave]):,} households")

# %%
# calculate indicators for all waves

results = sk.calculate_indicators_multi(
    datasets,
    indicators=["p0", "p1", "p2", "gini"],
    as_wide=True
)

print("\nCross-wave comparison:")
print(results)

# %%
# validation

bps_targets = {
    "2023-03": {"p0": 9.36, "p1": 1.528, "p2": 0.377, "gini": 0.388},
    "2024-03": {"p0": 9.03, "p1": 1.461, "p2": 0.347, "gini": 0.379},
}

print("\nValidation:")
for wave in waves:
    print(f"\n{wave}:")
    targets = bps_targets[wave]

    for ind in ["p0", "p1", "p2", "gini"]:
        ind_row = results.filter(pl.col("indicator") == ind)
        if len(ind_row) > 0 and wave in ind_row.columns:
            our_value = float(ind_row[wave][0])
            bps_value = targets[ind]
            diff = our_value - bps_value
            print(f"  {ind.upper():4s}: {our_value:.3f} (BPS: {bps_value:.3f}, diff: {diff:+.3f})")

# %%
