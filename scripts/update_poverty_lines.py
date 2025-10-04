#!/usr/bin/env python
"""Fetch poverty lines from BPS API and save to config."""

import sys
from datetime import date
from pathlib import Path

from statskita.indicators.poverty import PROVINCE_CODE_TO_NAME
from statskita.loaders.bps_api import fetch_poverty_lines


def main():
    if len(sys.argv) != 3:
        print("Usage: uv run scripts/update_poverty_lines.py 2024 march")
        sys.exit(1)

    year = int(sys.argv[1])
    period = sys.argv[2].lower()

    if period not in ("march", "september"):
        print("Period must be 'march' or 'september'")
        sys.exit(1)

    print(f"Fetching {period.capitalize()} {year} poverty lines from BPS...")

    try:
        lines = fetch_poverty_lines(year, period)
    except Exception as e:
        print(f"Failed: {e}")
        sys.exit(1)

    # build config structure
    config = {
        "period": f"{year}-{period[:2]}",
        "national": {},
        "provinces": {},
    }

    # organize data
    for (province, area), value in sorted(lines.items()):
        if province == "INDONESIA":
            config["national"][area] = int(value)
        else:
            # find province code
            prov_code = None
            for code, name in PROVINCE_CODE_TO_NAME.items():
                if name == province:
                    prov_code = code
                    break

            if prov_code:
                if prov_code not in config["provinces"]:
                    config["provinces"][prov_code] = {}
                config["provinces"][prov_code][area] = int(value)

    # save to file
    output_dir = Path("statskita/configs")
    output_dir.mkdir(exist_ok=True)

    month_num = "03" if period == "march" else "09"
    filename = f"poverty_lines_{year}_{month_num}.yaml"
    filepath = output_dir / filename

    # write yaml with comments
    with open(filepath, "w") as f:
        f.write(f"# BPS Poverty Lines {period.capitalize()} {year}\n")
        f.write(f"# Source: BPS API var 195, retrieved {date.today()}\n\n")
        f.write(f"period: {year}-{month_num}\n")

        f.write("national:\n")
        for area in ["urban", "rural"]:
            if area in config["national"]:
                f.write(f"  {area}: {config['national'][area]}\n")

        f.write("\n# Provincial poverty lines\n")
        f.write("provinces:\n")

        for code in sorted(config["provinces"].keys()):
            province_name = PROVINCE_CODE_TO_NAME[code]
            values = config["provinces"][code]

            if "rural" in values:
                f.write(f"  {code}: {{urban: {values['urban']}, rural: {values['rural']}}}  # {province_name}\n")
            else:
                # jakarta has no rural
                f.write(f"  {code}: {{urban: {values['urban']}}}                  # {province_name} (no rural)\n")

    print(f"Saved to {filepath}")
    print(f"Total: {len(config['provinces'])} provinces")


if __name__ == "__main__":
    main()
