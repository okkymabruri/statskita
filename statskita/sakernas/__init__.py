"""Convenient API for SAKERNAS metadata access."""

from typing import Optional
from ..loaders.sakernas import SakernasLoader

# Cache for loader instances to avoid reloading YAML files
_loader_cache: dict[str, SakernasLoader] = {}


def _get_loader(wave: str) -> SakernasLoader:
    """Get or create a cached loader for a specific wave."""
    if wave not in _loader_cache:
        loader = SakernasLoader()
        loader._load_config(wave)
        _loader_cache[wave] = loader
    return _loader_cache[wave]


def print_categories(wave: str = "2025-02") -> None:
    """Print available field categories for a SAKERNAS wave.

    Args:
        wave: Survey wave (e.g., "2025-02", "2024-08")

    Example:
        >>> import statskita as sk
        >>> sk.sakernas.print_categories(wave="2025-02")
    """
    loader = _get_loader(wave)
    loader.print_categories()


def print_labels(category: Optional[str] = None, wave: str = "2025-02") -> None:
    """Print field labels for a category.

    Args:
        category: Category name (demographics, work_status, etc.)
        wave: Survey wave (e.g., "2025-02", "2024-08")

    Example:
        >>> import statskita as sk
        >>> sk.sakernas.print_labels("demographics", wave="2025-02")
    """
    loader = _get_loader(wave)
    loader.print_labels(category)


def filter_labels(pattern: str, wave: str = "2025-02") -> None:
    """Print field labels matching a pattern.

    Args:
        pattern: Pattern to match (supports * wildcard)
        wave: Survey wave (e.g., "2025-02", "2024-08")

    Example:
        >>> import statskita as sk
        >>> sk.sakernas.filter_labels("DEM_*", wave="2025-02")
    """
    loader = _get_loader(wave)
    loader.filter_labels(pattern)


def get_field_info(field_name: str, wave: str = "2025-02") -> dict:
    """Get detailed information about a specific field.

    Args:
        field_name: Field name to look up
        wave: Survey wave (e.g., "2025-02", "2024-08")

    Returns:
        Dictionary with field information

    Example:
        >>> import statskita as sk
        >>> info = sk.sakernas.get_field_info("DEM_SEX", wave="2025-02")
    """
    loader = _get_loader(wave)
    return loader.get_variable_info(field_name)


def clear_cache() -> None:
    """Clear the loader cache to free memory."""
    global _loader_cache
    _loader_cache.clear()


__all__ = [
    "print_categories",
    "print_labels",
    "filter_labels",
    "get_field_info",
    "clear_cache",
]