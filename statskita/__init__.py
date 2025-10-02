"""StatsKita: Python toolkit for Indonesian official microdata (SAKERNAS)."""

__version__ = "0.1.0"
__author__ = "Okky Mabruri"
__email__ = "okkymbrur@gmail.com"

from . import sakernas  # metadata exploration API (print_categories, print_labels, etc.)
from .core import calculate_indicators, declare_survey, svyset, wrangle
from .core.multilevel import (
    analyze_subgroups,
    calculate_indicators_by_groups,
    compare_across_groups,
    create_crosstab,
    create_hierarchical_breakdown,
    format_hierarchical_table,
    pivot_indicators_wide,
)
from .exporters import export_excel, export_excel_multiple_sheets, export_parquet, export_stata
from .loaders import load_sakernas
from .loaders.multi_wave import (
    calculate_indicators_multi,
    compare_waves,
    load_and_harmonize_multi,
    load_sakernas_multi,
)
from .utils import batch_convert_dbf_to_parquet, dbf_to_parquet


# placeholder loaders - coming in v0.2.0
def load_susenas(*args, **kwargs):
    """SUSENAS loader - not yet implemented."""
    raise NotImplementedError("SUSENAS loader coming in v0.2.0. See dev/susenas.py for draft.")


def load_podes(*args, **kwargs):
    """PODES loader - not yet implemented."""
    raise NotImplementedError("PODES loader coming in v0.2.0. See dev/podes.py for draft.")


__all__ = [
    "__version__",
    "__author__",
    "__email__",
    "load_sakernas",
    "load_sakernas_multi",  # multi-wave loading
    "load_and_harmonize_multi",  # multi-wave with harmonization
    "load_susenas",
    "load_podes",
    "declare_survey",
    "svyset",  # stata-style alias
    "calculate_indicators",
    "calculate_indicators_multi",  # multi-wave indicators
    "compare_waves",  # wave comparison with change calculation
    "calculate_indicators_by_groups",  # multi-level grouping (simplified)
    "compare_across_groups",  # compare indicator across dimensions
    "analyze_subgroups",  # subgroup analysis with sample size checks
    "create_crosstab",  # crosstab analysis
    "create_hierarchical_breakdown",  # hierarchical breakdown (level1 + level1xlevel2)
    "format_hierarchical_table",  # format hierarchical results
    "pivot_indicators_wide",  # pivot multiple indicators to wide format
    "wrangle",
    "export_stata",
    "export_excel",
    "export_excel_multiple_sheets",
    "export_parquet",
    "dbf_to_parquet",
    "batch_convert_dbf_to_parquet",
    "sakernas",  # metadata API
]
