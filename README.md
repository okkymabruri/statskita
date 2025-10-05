# StatsKita: Python toolkit for Indonesian official microdata (SAKERNAS, SUSENAS)

[![PyPI version](https://badge.fury.io/py/statskita.svg)](https://pypi.org/project/statskita/)
[![PyPI Downloads](https://static.pepy.tech/badge/statskita)](https://pepy.tech/projects/statskita)
[![Build Status](https://github.com/okkymabruri/statskita/actions/workflows/ci.yml/badge.svg)](https://github.com/okkymabruri/statskita/actions)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> **v0.3.0**: SAKERNAS (labor) + SUSENAS (poverty/inequality)

## What's Supported

| Dataset | Waves | Indicators |
|---------|-------|------------|
| SAKERNAS | 5 (2023-02 → 2025-02) | Labor force, employment, wages |
| SUSENAS | 2 (2023-03, 2024-03) | Poverty (P0, P1, P2), Gini |

## Installation

```bash
pip install statskita
```

## Quick Start

**Single wave:**
```python
import statskita as sk

# SAKERNAS
df = sk.load_sakernas(wave="2025-02")
clean_df = sk.wrangle(df, harmonize=True, source_wave="2025-02")
design = sk.declare_survey(clean_df, weight="survey_weight", psu="psu")
results = sk.calculate_indicators(design, ["unemployment_rate", "lfpr"])

# SUSENAS
df = sk.load_susenas(wave="2024-03", module="kp", category="housing")
design = sk.declare_survey(df, weight="WEIND", wave="2024-03")
results = sk.calculate_indicators(design, ["p0", "p1", "p2", "gini"])
```

**Multi-wave comparison:**
```python
# SAKERNAS
sakernas_waves = {w: sk.load_sakernas(wave=w) for w in ["2023-02", "2023-08", "2024-02", "2025-02"]}
harmonized = {w: sk.wrangle(df, harmonize=True, source_wave=w) for w, df in sakernas_waves.items()}
sakernas_results = sk.calculate_indicators_multi(harmonized, "all", as_wide=True)

# SUSENAS
susenas_waves = {w: sk.load_susenas(wave=w, module="kp", category="housing") for w in ["2023-03", "2024-03"]}
susenas_results = sk.calculate_indicators_multi(susenas_waves, ["p0", "p1", "p2", "gini"], as_wide=True)
```

**SAKERNAS output:**
```
┌─────────────────────────────────┬──────┬─────────┬─────────┬─────────┬─────────┐
│ indicator                       ┆ unit ┆ 2023-02 ┆ 2023-08 ┆ 2024-02 ┆ 2025-02 │
├─────────────────────────────────┼──────┼─────────┼─────────┼─────────┼─────────┤
│ labor_force_participation_rate  ┆ %    ┆ ...     ┆ ...     ┆ ...     ┆ ...     │
│ employment_rate                 ┆ %    ┆ ...     ┆ ...     ┆ ...     ┆ ...     │
│ unemployment_rate               ┆ %    ┆ 5.45    ┆ 5.32    ┆ 4.82    ┆ 4.76    │
│ underemployment_rate            ┆ %    ┆ ...     ┆ ...     ┆ ...     ┆ ...     │
│ female_lfpr                     ┆ %    ┆ ...     ┆ ...     ┆ ...     ┆ ...     │
│ average_wage                    ┆ M Rp ┆ ...     ┆ ...     ┆ ...     ┆ ...     │
│ neet_rate                       ┆ %    ┆ ...     ┆ ...     ┆ ...     ┆ ...     │
└─────────────────────────────────┴──────┴─────────┴─────────┴─────────┴─────────┘
```

**SUSENAS output:**
```
┌───────────┬──────┬─────────┬─────────┐
│ indicator ┆ unit ┆ 2023-03 ┆ 2024-03 │
├───────────┼──────┼─────────┼─────────┤
│ p0        ┆      ┆ 9.36    ┆ 9.03    │
│ p1        ┆      ┆ 1.53    ┆ 1.46    │
│ p2        ┆      ┆ 0.38    ┆ 0.35    │
│ gini      ┆      ┆ 0.39    ┆ 0.38    │
└───────────┴──────┴─────────┴─────────┘
```

## Features

- **Multi-wave analysis**: Built-in cross-wave comparison
- **Survey-aware**: Handles weights, strata, PSU correctly
- **Open source**: Free, no licensing costs

See `examples/` for detailed usage.