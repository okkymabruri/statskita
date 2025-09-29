"""SAKERNAS (Labor Force Survey) data loader with survey design awareness."""

import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import polars as pl
import pyreadstat
import yaml

from ..utils.config_utils import load_config_with_inheritance
from .base import BaseLoader, DatasetMetadata, SurveyDesignInfo


class SakernasLoader(BaseLoader):
    """Loader for SAKERNAS (Labor Force Survey) data files."""

    # bps var mappings
    VARIABLE_MAPPINGS = {
        # admin
        "prov": ["PROV", "B1R1", "R101", "kode_prov"],
        "kab_kot": ["KAB", "B1R2", "R102"],
        "kec": ["KEC", "B1R3", "R103"],
        "desa": ["DESA", "B1R4", "R104"],
        "urb_rur": ["B1R5", "R105", "URBAN", "RURAL"],
        # old names (TODO: deprecate)
        "province": ["PROV", "B1R1", "R101", "kode_prov"],
        "regency": ["KAB", "B1R2", "R102"],
        "district": ["KEC", "B1R3", "R103"],
        "village": ["DESA", "B1R4", "R104"],
        "urban_rural": ["B1R5", "R105", "URBAN", "RURAL"],
        # weights etc
        "weight": ["WEIGHT", "WTPER", "WT", "WFINAL"],
        "strata": ["STRATA", "STRATUM", "STRT"],
        "psu": ["PSU", "KLUSTER", "CLUSTER"],
        # demo
        "age": ["B4K5", "R405", "AGE", "UMUR"],
        "gender": ["B4K4", "R404", "JENIS_KELAMIN"],
        "education": ["B4K8", "R408", "PENDIDIKAN"],
        "marital_status": ["B4K6", "R406", "STATUS_KAWIN"],
        # labor
        "work_status": ["B5R1", "R501", "STATUS_KERJA"],
        "industry": ["B5R16A", "R516A", "INDUSTRI_UTAMA"],
        "occupation": ["B5R15A", "R515A", "PEKERJAAN_UTAMA"],
        "hours_worked": ["B5R28", "R528", "JAM_KERJA"],
        "job_seeking": ["B5R33", "R533", "MENCARI_KERJA"],
        "willing_to_work": ["B5R37", "R537", "BERSEDIA_KERJA"],
    }

    # wave-specific configurations
    WAVE_CONFIGS = {
        "2021": {
            "reference_period": "February 2021",
            "weight_var": "WEIGHT",
            "province_var": "PROV",
        },
        "2022": {
            "reference_period": "February 2022",
            "weight_var": "WEIGHT",
            "province_var": "PROV",
        },
        "2023": {
            "reference_period": "February 2023",
            "weight_var": "WEIGHT",
            "province_var": "PROV",
        },
        "2024": {
            "reference_period": "February 2024",
            "weight_var": "WEIGHT",
            "province_var": "PROV",
        },
        "2025": {
            "reference_period": "February 2025",
            "weight_var": "WEIGHT",
            "province_var": "PROV",
        },
    }

    def __init__(self, preserve_labels: bool = True):
        super().__init__(preserve_labels)
        self._value_labels: Optional[Dict[str, Dict[Any, str]]] = None
        self._variable_labels: Optional[Dict[str, str]] = None
        self._config: Optional[Dict[str, Any]] = None
        self._reverse_mappings: Optional[Dict[str, str]] = None

    def load(
        self,
        file_path: Union[str, Path],
        wave: Optional[str] = None,
        **kwargs,
    ) -> pl.DataFrame:
        """Load SAKERNAS data file (.sav, .dta, .dbf, or .parquet).

        Args:
            file_path: Path to data file
            wave: Survey wave (e.g., '2025-02') for config selection
            **kwargs: Extra pyreadstat options
        """
        path = self._validate_file_exists(file_path)

        # detect file format
        if path.suffix.lower() == ".sav":
            # Never apply pyreadstat's value formats - we use our YAML configs instead
            df_pd, meta = pyreadstat.read_sav(
                str(path), apply_value_formats=False, **kwargs
            )
            # store metadata
            self._value_labels = meta.value_labels if hasattr(meta, "value_labels") else {}
            self._variable_labels = meta.variable_labels if hasattr(meta, "variable_labels") else {}

            # convert to polars
            df = pl.from_pandas(df_pd)

        elif path.suffix.lower() == ".dta":
            # Never apply pyreadstat's value formats - we use our YAML configs instead
            df_pd, meta = pyreadstat.read_dta(
                str(path), apply_value_formats=False, **kwargs
            )
            # store metadata
            self._value_labels = meta.value_labels if hasattr(meta, "value_labels") else {}
            self._variable_labels = meta.variable_labels if hasattr(meta, "variable_labels") else {}

            # convert to polars
            df = pl.from_pandas(df_pd)

        elif path.suffix.lower() == ".dbf":
            # dbf loading with rust-based reader
            import dbfrs
            import pandas as pd

            # get field names
            fields = dbfrs.get_dbf_fields(str(path))
            field_names = [field.name for field in fields]

            # load records (returns tuples)
            records = dbfrs.load_dbf(str(path))

            # convert via pandas (efficient for large datasets)
            df_pd = pd.DataFrame(records, columns=field_names)
            df = pl.from_pandas(df_pd)

            # dbf has no metadata, create empty dicts
            self._value_labels = {}
            self._variable_labels = {col: col for col in df.columns}

        elif path.suffix.lower() == ".parquet":
            # parquet loading (fastest, no metadata)
            df = pl.read_parquet(path)

            # parquet has no metadata, create empty dicts
            self._value_labels = {}
            self._variable_labels = {col: col for col in df.columns}

        else:
            raise ValueError(f"Unsupported file format: {path.suffix}")

        # extract wave info
        detected_wave = self._extract_wave_from_path(path)
        wave = wave or detected_wave

        # load configuration
        self._load_config(wave)

        # create metadata
        self._metadata = DatasetMetadata(
            dataset_name="SAKERNAS",
            survey_wave=wave,
            reference_period=self.WAVE_CONFIGS.get(wave, {}).get(
                "reference_period", f"{wave} (unknown period)"
            ),
            sample_size=df.shape[0],
            weight_variable=self._find_variable("weight"),
            strata_variable=self._find_variable("strata"),
            psu_variable=self._find_variable("psu"),
            province_variable=self._find_variable("province"),
            urban_rural_variable=self._find_variable("urban_rural"),
            file_path=path,
            created_at=datetime.now().isoformat(),
        )

        return df

    def get_survey_design(self) -> SurveyDesignInfo:
        """Get survey design information for SAKERNAS."""
        if not self._metadata:
            raise ValueError("Must load data first before getting survey design")

        # try to get survey design from config
        weight_col = self._metadata.weight_variable or "WEIGHT"
        strata_col = self._metadata.strata_variable
        psu_col = self._metadata.psu_variable

        # override with config if available
        if self._config and "survey_design" in self._config:
            survey_config = self._config["survey_design"]
            if survey_config.get("weight_var"):
                weight_col = survey_config["weight_var"]
            if survey_config.get("strata_var"):
                strata_col = survey_config["strata_var"]
            if survey_config.get("psu_var"):
                psu_col = survey_config["psu_var"]

        return SurveyDesignInfo(
            weight_col=weight_col,
            strata_col=strata_col,
            psu_col=psu_col,
            finite_population_correction=True,
            domain_cols=["PROV", "B1R5"]
            if self._metadata.province_variable and self._metadata.urban_rural_variable
            else None,
        )

    def _load_config(self, wave: Optional[str] = None):
        """Load configuration from YAML files.

        Priority:
        1. If wave matches existing YAML config, use that with inheritance
        2. Else fall back to defaults
        """
        config_dir = Path(__file__).parent.parent / "configs" / "sakernas"

        # tier 1: existing yaml config with inheritance
        if wave:
            wave_config_path = config_dir / f"{wave}.yaml"
            if wave_config_path.exists():
                try:
                    self._config = load_config_with_inheritance(wave_config_path)
                    self._build_reverse_mappings()
                    return
                except Exception as e:
                    print(f"Warning: Failed to load wave config {wave}: {e}")

        # tier 2: defaults
        defaults_path = config_dir / "defaults.yaml"
        try:
            with open(defaults_path, "r") as f:
                self._config = yaml.safe_load(f)
            self._build_reverse_mappings()
        except Exception as e:
            print(f"Warning: Failed to load defaults: {e}")
            # fallback to hardcoded mappings
            self._config = None
            self._reverse_mappings = None

    def _build_reverse_mappings(self):
        """Build reverse mapping from canonical names to raw field names."""
        if not self._config or "fields" not in self._config:
            return

        self._reverse_mappings = {}
        for raw_name, field_info in self._config["fields"].items():
            canon_name = field_info.get("canon_name", raw_name.lower())
            if canon_name not in self._reverse_mappings:
                self._reverse_mappings[canon_name] = []
            self._reverse_mappings[canon_name].append(raw_name)

    def _find_variable(self, var_type: str) -> Optional[str]:
        """Find variable name in data by type."""
        if not hasattr(self, "_metadata") or not self._metadata:
            return None

        # get available variables
        if hasattr(self, "_variable_labels") and self._variable_labels:
            available_vars = list(self._variable_labels.keys())
        else:
            available_vars = []

        # try config-based mapping first
        if self._reverse_mappings and var_type in self._reverse_mappings:
            possible_names = self._reverse_mappings[var_type]
            for name in possible_names:
                if name in available_vars:
                    return name

        # fallback to hardcoded mappings for backward compatibility
        possible_names = self.VARIABLE_MAPPINGS.get(var_type, [])
        for name in possible_names:
            if name in available_vars:
                return name

        return None

    def _extract_wave_from_path(self, file_path: Path) -> str:
        # extract wave from various naming conventions
        filename = file_path.stem.lower()

        # try common patterns:
        patterns = [
            r"sak(?:ernas)?[_-]?(\d{4})[_-](\d{2})",  # sakernas_2025-02 or sak2025_02
            r"sak(?:ernas)?_?(\d{4})",  # sakernas_2025
            r"(\d{4})[_-](\d{2})[_-]?sak",  # 2025-02_sak
            r"(\d{4})_?sak",  # 2025_sak
            r"sakernas(\d{2})(feb|aug)",  # sakernas25feb
            r"(\d{4})",  # fallback - just year
        ]

        for pattern in patterns:
            match = re.search(pattern, filename)
            if match:
                if len(match.groups()) == 2:
                    # year and month found
                    year = match.group(1)
                    month = match.group(2)
                    # handle 2-digit year
                    if len(year) == 2:
                        year = "20" + year
                    if 2015 <= int(year) <= 2030:
                        return f"{year}-{month.zfill(2)}"
                else:
                    # just year
                    year = match.group(1)
                    if 2015 <= int(year) <= 2030:
                        return year

        return "unknown"

    def get_value_labels(self, variable: Optional[str] = None) -> Union[Dict[str, Dict], Dict]:
        """Get value labels."""
        if not self._value_labels:
            return {}

        if variable:
            return self._value_labels.get(variable, {})

        return self._value_labels

    def get_variable_labels(self) -> Dict[str, str]:
        """Get variable labels (descriptions)."""
        return self._variable_labels or {}

    def get_config(self) -> Optional[Dict[str, Any]]:
        """Get the loaded configuration."""
        return self._config

    def get_canonical_mapping(self) -> Optional[Dict[str, List[str]]]:
        """Get the canonical name to raw field mappings."""
        return self._reverse_mappings

    def list_variables_by_category(self) -> Dict[str, List[str]]:
        """List available variables by category."""
        if not self._metadata:
            raise ValueError("Must load data first")

        result = {}

        # use config-based mappings if available
        if self._reverse_mappings:
            for category, var_names in self._reverse_mappings.items():
                available_vars = []
                if hasattr(self, "_variable_labels") and self._variable_labels:
                    for var_name in var_names:
                        if var_name in self._variable_labels:
                            available_vars.append(var_name)
                result[category] = available_vars

        # fallback to hardcoded mappings
        for category, var_names in self.VARIABLE_MAPPINGS.items():
            if category not in result:
                found_var = self._find_variable(category)
                if found_var:
                    result[category] = [found_var]
                else:
                    result[category] = []

        return result

    def describe(self, variable_name: Optional[str] = None) -> Union[Dict[str, Any], None]:
        """Describe variable(s) with labels and metadata.

        Args:
            variable_name: Variable to describe. If None, lists all variables.

        Returns:
            Dictionary with variable metadata or None if not found.

        Example:
            >>> loader.describe('DEM_AGE')
            {
                'raw_name': 'DEM_AGE',
                'canon_name': 'age',
                'label': '209. Umur (tahun)',
                'dtype': 'N',
                'length': 2,
                'category': 'demographics',
                'validation': 'range(0, 120)'
            }
        """
        if not self._config or "fields" not in self._config:
            print("Warning: No configuration loaded")
            return None

        fields = self._config["fields"]

        # if no variable specified, list all
        if variable_name is None:
            result = {}
            for field_name, field_info in fields.items():
                result[field_name] = {
                    "canon_name": field_info.get("canon_name", field_name.lower()),
                    "label": field_info.get("label", ""),
                    "category": field_info.get("category", "unknown"),
                }
            return result

        # check raw name first
        if variable_name in fields:
            field = fields[variable_name]
            return {
                "raw_name": variable_name,
                "canon_name": field.get("canon_name", variable_name.lower()),
                "label": field.get("label", ""),
                "dtype": field.get("dtype", "C"),
                "length": field.get("length"),
                "category": field.get("category", "unknown"),
                "values": field.get("values"),
                "validation": field.get("validation"),
            }

        # check canonical name
        for raw_name, field_info in fields.items():
            if field_info.get("canon_name") == variable_name.lower():
                return {
                    "raw_name": raw_name,
                    "canon_name": field_info.get("canon_name"),
                    "label": field_info.get("label", ""),
                    "dtype": field_info.get("dtype", "C"),
                    "length": field_info.get("length"),
                    "category": field_info.get("category", "unknown"),
                    "values": field_info.get("values"),
                    "validation": field_info.get("validation"),
                }

        print(f"Variable '{variable_name}' not found in configuration")
        return None

    def print_labels(self, category: Optional[str] = None) -> None:
        """Print variable labels by category.

        Args:
            category: Category to filter by (demographics, education, etc.)
                     If None, prints all categories.

        Example:
            >>> loader.print_labels('demographics')
            Demographics Variables:
            DEM_SEX      204. Jenis kelamin
              1: Laki-laki
              2: Perempuan
            DEM_AGE      209. Umur (tahun)
        """
        if not self._config or "fields" not in self._config:
            print("No configuration loaded")
            return

        fields = self._config["fields"]

        # group by category
        categories = {}
        for field_name, field_info in fields.items():
            cat = field_info.get("category", "uncategorized")
            if category and cat != category:
                continue
            if cat not in categories:
                categories[cat] = []
            categories[cat].append((field_name, field_info))

        # print by category
        for cat, vars in sorted(categories.items()):
            print(f"\n{cat.title()} Variables:")
            print("-" * 60)
            for var_name, var_info in vars:
                label = var_info.get("label", "")
                print(f"{var_name:15} {label}")

                # print value labels if present
                if "values" in var_info:
                    for code, val_label in var_info["values"].items():
                        print(f"  {code}: {val_label}")

    def list_categories(self) -> List[str]:
        """List all available categories from the configuration.

        Returns:
            List of category names found in the configuration.

        Example:
            >>> loader.list_categories()
            ['admin', 'demographics', 'work_status', ...]
        """
        if not self._config or "fields" not in self._config:
            print("No configuration loaded")
            return []

        categories = set()
        for field_info in self._config["fields"].values():
            cat = field_info.get("category", "uncategorized")
            categories.add(cat)

        return sorted(list(categories))

    def print_categories(self) -> None:
        """Print all available categories with field counts.

        Example:
            >>> loader.print_categories()
            Available Categories:
            --------------------
            admin            (5 fields)  - Administrative identifiers
            demographics    (37 fields)  - Personal characteristics
            work_status      (3 fields)  - Employment status
            ...
        """
        if not self._config:
            print("No configuration loaded")
            return

        # count fields per category
        category_counts = {}
        for field_info in self._config.get("fields", {}).values():
            cat = field_info.get("category", "uncategorized")
            category_counts[cat] = category_counts.get(cat, 0) + 1

        # category descriptions (from defaults.yaml or hardcoded)
        category_descriptions = {
            "admin": "Administrative identifiers",
            "survey_design": "Survey weights and design variables",
            "household": "Household composition",
            "demographics": "Personal characteristics",
            "migration": "Migration history",
            "disability": "Disability status",
            "ict_usage": "Technology and internet usage",
            "work_status": "Employment status",
            "absent_work": "Temporary absence from work",
            "main_job": "Main job characteristics",
            "main_job_time": "Working hours and duration",
            "secondary_job": "Additional employment",
            "job_seeking": "Job search activities",
            "availability": "Availability for work",
            "other": "Miscellaneous fields",
            "uncategorized": "Fields without category",
        }

        print("\nAvailable Categories:")
        print("-" * 70)
        for cat in sorted(category_counts.keys()):
            count = category_counts[cat]
            desc = category_descriptions.get(cat, "")
            print(f"{cat:15} ({count:3} fields)  - {desc}")

        print(
            f"\nTotal: {sum(category_counts.values())} fields in {len(category_counts)} categories"
        )
        print("\nUse loader.print_labels('category_name') to see fields in a category")


def load_sakernas(
    file_path: Union[str, Path],
    preserve_labels: bool = True,
    wave: Optional[str] = None,
    **kwargs,
) -> pl.DataFrame:
    """Load SAKERNAS labor force survey data.

    Args:
        file_path: Path to .sav, .dta, .dbf, or .parquet file
        preserve_labels: If False, convert numeric codes to Indonesian text labels
        wave: Survey wave (e.g., '2025-02') for config selection
        **kwargs: Extra options for pyreadstat

    Example:
        >>> df = sk.load_sakernas("sakernas_2024.sav")
        >>> # Load with automatic wave detection and label conversion
        >>> df = sk.load_sakernas("sakernas_2025-02.parquet", preserve_labels=False)
        >>> # Or specify wave explicitly for older files
        >>> df = sk.load_sakernas("sak202502.dbf", wave="2025-02", preserve_labels=False)
        >>> print(f"Loaded {len(df)} observations")
    """
    loader = SakernasLoader(preserve_labels=preserve_labels)
    df = loader.load(file_path, wave=wave, **kwargs)

    # get the actual wave (either passed explicitly or detected from filename)
    actual_wave = wave or loader._extract_wave_from_path(Path(file_path))

    # apply value labels from config if preserve_labels=False
    if not preserve_labels and actual_wave and actual_wave != "unknown":
        from ..core.harmonizer import SurveyHarmonizer
        harmonizer = SurveyHarmonizer(dataset_type="sakernas")
        df, _ = harmonizer.harmonize(df, source_wave=actual_wave, preserve_labels=False)

    # attach loader metadata to dataframe
    if hasattr(df, "_statskita_metadata"):
        df._statskita_metadata = {
            "loader": loader,
            "metadata": loader.metadata,
            "survey_design": loader.get_survey_design(),
        }

    return df
