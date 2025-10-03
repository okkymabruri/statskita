"""Data loaders for Indonesian government statistics."""

from .sakernas import SakernasLoader, load_sakernas
from .susenas import SusenasLoader, load_susenas

# placeholder loaders - coming in future versions
# from .podes import load_podes
# from .bps_api import BPSAPIClient

__all__ = ["load_sakernas", "SakernasLoader", "load_susenas", "SusenasLoader"]
