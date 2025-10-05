"""Indicator calculation modules for different survey types."""

from .inequality import (
    calculate_gini,
    calculate_percentile_ratios,
)
from .poverty import (
    calculate_poverty_fgt,
    calculate_poverty_headcount,
    load_poverty_lines_from_config,
)

__all__ = [
    # poverty indicators (BPS official)
    "calculate_poverty_headcount",
    "calculate_poverty_fgt",
    "load_poverty_lines_from_config",
    # inequality indicators (BPS + international standards)
    "calculate_gini",
    "calculate_percentile_ratios",
]
