"""StatsKita: Python toolkit for Indonesian official microdata."""

__version__ = "0.1.0"
__author__ = "Okky Mabruri"
__email__ = "okkymbrur@gmail.com"

from .core import calculate_indicators, declare_survey, svyset, wrangle
from .exporters import export_excel, export_excel_multiple_sheets, export_parquet, export_stata
from .loaders import load_sakernas
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
    "load_susenas",
    "load_podes",
    "declare_survey",
    "svyset",  # stata-style alias
    "calculate_indicators",
    "wrangle",
    "export_stata",
    "export_excel",
    "export_excel_multiple_sheets",
    "export_parquet",
    "dbf_to_parquet",
    "batch_convert_dbf_to_parquet",
]
