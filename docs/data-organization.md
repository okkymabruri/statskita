# Data Organization & Conversion Guide

## Recommended Directory Structure

```
your_project/
├── data/
│   ├── sakernas/
│   │   ├── raw/              # Original BPS files
│   │   └── parquet/          # Converted files
│   │       ├── sakernas_2023-02.parquet
│   │       ├── sakernas_2024-02.parquet
│   │       └── sakernas_2025-02.parquet
│   └── susenas/
│       ├── raw/              # Original BPS files
│       └── parquet/          # Converted files
│           ├── susenas_2023-03_kp_blok43.parquet
│           └── susenas_2024-03_kp_blok43.parquet
├── .env                      # Path configuration
└── analysis.py               # Your analysis code
```

## File Naming Convention

**SAKERNAS:**
- Raw: `Sak0224_p1.dta`, `Sak0224_p2.dta`
- Parquet: `sakernas_2024-02.parquet` (merged p1+p2)

**SUSENAS:**
- Raw: `ssn202403_kor_rt.dbf`, `ssn202403_kp_blok41_11_31.dbf`
- Parquet: `susenas_2024-03_kor_rt.parquet`, `susenas_2024-03_kp_blok41_11-31.parquet`

## Converting Your Own Data

### Requirements

```bash
uv pip install dbfrs pyreadstat polars
```

### Quick Start

Set environment variables in `.env`:
```bash
SUSENAS_DATA_DIR=/path/to/bps-susenas
SUSENAS_PARQUET_DIR=/path/to/bps-susenas-pq

SAKERNAS_DATA_DIR=/path/to/bps-sakernas
SAKERNAS_PARQUET_DIR=/path/to/bps-sakernas-pq
```

Convert data:
```bash
uv run scripts/convert_to_parquet.py susenas
uv run scripts/convert_to_parquet.py sakernas
```

### Python API

```python
import statskita as sk

# convert single file
pq_file = sk.dbf_to_parquet("input.dbf")

# convert directory
from pathlib import Path
for dbf_file in Path("raw_data").glob("*.dbf"):
    sk.dbf_to_parquet(dbf_file)
```

## What the Conversion Does

**SAKERNAS:**
- Merges p1 (demographics) + p2 (labor) into single file
- Preserves all variable labels

**SUSENAS:**
- Converts each module file separately
- Renames to standard format
- Uses dbfrs to preserve BPS original KAPITA values
- Geographic splits kept separate

## Common Issues

**"No files found for wave X"**
- Check `.env` has correct paths
- Verify file naming: `susenas_YYYY-MM_*`

**STRATA missing (2023-03 only)**
- Separate file: `susenas_2023-03_kor_strata.parquet`
- Loader auto-merges when needed
