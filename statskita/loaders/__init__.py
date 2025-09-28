"""Data loaders for Indonesian government statistics."""

from .sakernas import SakernasLoader, load_sakernas

# placeholder loaders moved to dev/ - coming in v0.2.0
# from .susenas import load_susenas
# from .podes import load_podes
# from .bps_api import BPSAPIClient

__all__ = ["load_sakernas", "SakernasLoader"]
