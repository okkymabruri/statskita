"""Base loader classes for all statistical data loaders."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, Union

import polars as pl
from pydantic import BaseModel, ConfigDict


class DatasetMetadata(BaseModel):
    """Metadata for statistical datasets."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    dataset_name: str
    survey_wave: str
    reference_period: str
    sample_size: Optional[int] = None
    weight_variable: Optional[str] = None
    strata_variable: Optional[str] = None
    psu_variable: Optional[str] = None
    province_variable: Optional[str] = None
    urban_rural_variable: Optional[str] = None
    file_path: Optional[Path] = None
    created_at: Optional[str] = None


class SurveyDesignInfo(BaseModel):
    """Survey design information for statistical inference."""

    weight_col: str
    strata_col: Optional[str] = None
    psu_col: Optional[str] = None
    finite_population_correction: bool = True
    domain_cols: Optional[list[str]] = None


class BaseLoader(ABC):
    """Abstract base class for all data loaders."""

    def __init__(self, preserve_labels: bool = True):
        self.preserve_labels = preserve_labels
        self._metadata: Optional[DatasetMetadata] = None

    @abstractmethod
    def load(self, file_path: Union[str, Path], **kwargs) -> pl.DataFrame:
        """Load data from file."""
        pass

    @abstractmethod
    def get_survey_design(self) -> SurveyDesignInfo:
        """Get survey design information."""
        pass

    @property
    def metadata(self) -> Optional[DatasetMetadata]:
        """Get dataset metadata."""
        return self._metadata

    def _validate_file_exists(self, file_path: Union[str, Path]) -> Path:
        """Validate file exists and return as Path object."""
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Data file not found: {path}")
        return path

    def _extract_wave_from_path(self, file_path: Path) -> str:
        """Extract survey wave from file path/name."""
        # basic year finder - override for fancier patterns
        filename = file_path.stem.lower()

        # look for year patterns
        import re

        year_match = re.search(r"20\d{2}", filename)
        if year_match:
            return year_match.group()

        return "unknown"
