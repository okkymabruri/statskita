"""Export to Excel format using polars native functions."""

from pathlib import Path
from typing import Dict, Union

import polars as pl


def export_excel(
    df: pl.DataFrame,
    file_path: Union[str, Path],
    sheet_name: str = "Data",
    include_index: bool = False,
    **kwargs,
) -> None:
    """Export DataFrame to Excel using polars.

    Args:
        df: Data to export
        file_path: Output .xlsx file
        sheet_name: Excel sheet name
        include_index: Add row numbers (ignored - for compat)
    """
    # export to excel
    df.write_excel(
        workbook=str(file_path),
        worksheet=sheet_name,
        include_header=True,
        **kwargs
    )
    print(f"Exported to: {file_path}")


def export_excel_multiple_sheets(
    dataframes: Dict[str, pl.DataFrame],
    file_path: Union[str, Path],
    **kwargs
) -> None:
    """Export multiple DataFrames to Excel sheets.

    Args:
        dataframes: {sheet_name: dataframe} dict
        file_path: Output .xlsx file
    """
    import xlsxwriter

    # create workbook
    workbook = xlsxwriter.Workbook(str(file_path))

    # write each dataframe to a separate sheet
    for sheet_name, df in dataframes.items():
        df.write_excel(
            workbook=workbook,
            worksheet=sheet_name,
            include_header=True,
            **kwargs
        )

    # close workbook
    workbook.close()

    print(f"Exported {len(dataframes)} sheets to: {file_path}")
