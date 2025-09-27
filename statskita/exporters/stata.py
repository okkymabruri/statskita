"""Export to Stata format (.dta).

Note: Native Polars support available via polars_readstat:
https://github.com/jrothbaum/polars_readstat
Consider using for direct Polars-Stata integration.
"""

from pathlib import Path
from typing import Union

import polars as pl


def export_stata(
    df: pl.DataFrame, file_path: Union[str, Path], preserve_labels: bool = True, **kwargs
) -> None:
    """Export polars DataFrame to Stata format.

    Note: Currently uses pyreadstat via pandas conversion.
    Alternative: polars_readstat for native Polars support (future enhancement).

    Args:
        df: Polars DataFrame to export
        file_path: Output file path
        preserve_labels: Whether to preserve variable and value labels
        **kwargs: Additional arguments for pyreadstat

    Raises:
        NotImplementedError: Stata export not yet fully implemented
    """
    # TODO: consider polars_readstat for native support
    # convert to pandas first (pyreadstat requirement)
    df_pd = df.to_pandas()

    try:
        import pyreadstat

        # basic export (labels not yet supported)
        pyreadstat.write_dta(df_pd, str(file_path), **kwargs)
        print(f"Data exported to: {file_path}")

    except ImportError:
        raise ImportError(
            "pyreadstat is required for Stata export. Install with: uv add pyreadstat"
        )
    except Exception as e:
        # fallback to CSV with warning
        csv_path = Path(file_path).with_suffix(".csv")
        df.write_csv(csv_path)
        print(f"Warning: Stata export failed ({e}). Exported to CSV instead: {csv_path}")
        raise NotImplementedError(
            "Full Stata export with labels not yet implemented. Coming in v0.2.0"
        )
