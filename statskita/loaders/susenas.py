"""SUSENAS (Socioeconomic Survey) data loader with multi-file support."""

import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union

import dbfrs
import pandas as pd
import polars as pl
import yaml

from ..utils.config_utils import load_config_with_inheritance
from .base import BaseLoader, DatasetMetadata, SurveyDesignInfo


class SusenasLoader(BaseLoader):
    """Loader for SUSENAS (Socioeconomic Survey) data files."""

    def __init__(self, preserve_labels: bool = True):
        super().__init__(preserve_labels)
        self._value_labels: Optional[Dict[str, Dict[Any, str]]] = None
        self._variable_labels: Optional[Dict[str, str]] = None
        self._config: Optional[Dict[str, Any]] = None
        self._data_dir: Optional[Path] = None
        self._reverse_mappings: Optional[Dict[str, List[str]]] = None
        self._file_patterns: Optional[Dict[str, Any]] = None
        self._wave: Optional[str] = None

    def load(
        self,
        file_path: Union[str, Path, None] = None,
        module: Literal["kor", "kp", "both"] = "kor",
        table: Optional[Literal["rt", "ind1", "ind2", "mig"]] = None,
        category: Optional[Literal["food", "nonfood", "housing", "all"]] = None,
        merged: bool = True,
        wave: Optional[str] = None,
        **kwargs,
    ) -> pl.DataFrame:
        """Load SUSENAS data from DBF or Parquet files.

        KOR module has household data, KP module has consumption data.
        Auto-detects file format and handles multi-file structure.
        """
        # determine data directory
        if file_path:
            self._data_dir = Path(file_path) if Path(file_path).is_dir() else Path(file_path).parent
        else:
            data_dir_env = os.environ.get("SUSENAS_DATA_DIR")
            if not data_dir_env:
                raise ValueError("file_path not provided and SUSENAS_DATA_DIR not set in .env")
            self._data_dir = Path(data_dir_env)

        if not self._data_dir.exists():
            raise FileNotFoundError(f"Data directory not found: {self._data_dir}")

        # detect wave if not provided
        if not wave:
            wave = "2024-03"  # default for now

        self._wave = wave

        # detect available files for wave
        self._file_patterns = self._detect_files(wave)

        # load config
        self._load_config(wave)

        # load requested module(s)
        if module == "kor":
            df = self._load_kor_module(merged=merged, table=table)
        elif module == "kp":
            df = self._load_kp_module(category=category or "all")
        elif module == "both":
            df_kor = self._load_kor_module(merged=True, table="rt")
            df_kp = self._load_kp_module(category="housing")
            df = df_kor.join(df_kp, on="URUT", how="left")
        else:
            raise ValueError(f"Invalid module: {module}")

        # derive sample size (prefer unique households when URUT is present)
        sample_size = None
        if isinstance(df, pl.DataFrame):
            if "URUT" in df.columns:
                sample_size = int(df.select(pl.col("URUT").n_unique()).item())
            else:
                sample_size = df.shape[0]

        # create metadata
        self._metadata = DatasetMetadata(
            dataset_name="SUSENAS",
            survey_wave=wave,
            reference_period=f"{wave} (March 2024)",
            sample_size=sample_size,
            weight_variable="FWT",
            strata_variable="STRATA",
            psu_variable="PSU",
            province_variable="R101",
            urban_rural_variable="R105",
            file_path=self._data_dir,
            created_at=datetime.now().isoformat(),
        )

        return df

    def _load_kor_module(self, merged: bool, table: Optional[str]) -> pl.DataFrame:
        """Load KOR module (household/individual data)."""
        kor_files = self._file_patterns["kor"]

        if not merged and table:
            if table not in kor_files:
                raise ValueError(f"Table {table} not found. Available: {list(kor_files.keys())}")
            return self._load_dbf_file(kor_files[table])

        # load rt table
        if "rt" not in kor_files:
            raise ValueError(f"RT table not found. Available: {list(kor_files.keys())}")

        df_rt = self._load_dbf_file(kor_files["rt"])

        if not merged:
            return df_rt

        return df_rt

    def _load_kp_module(self, category: str) -> pl.DataFrame:
        """Load KP module (consumption data)."""
        kp_files = self._file_patterns["kp"]

        if category == "food":
            if "food" not in kp_files:
                raise ValueError(f"Food category not found. Available: {list(kp_files.keys())}")
            files = kp_files["food"]
            return self._load_and_stack(files) if isinstance(files, list) else self._load_dbf_file(files)
        elif category == "nonfood":
            if "nonfood" not in kp_files:
                raise ValueError(f"Nonfood category not found. Available: {list(kp_files.keys())}")
            files = kp_files["nonfood"]
            return self._load_and_stack(files) if isinstance(files, list) else self._load_dbf_file(files)
        elif category == "housing":
            if "housing" not in kp_files:
                raise ValueError(f"Housing category not found. Available: {list(kp_files.keys())}")
            files = kp_files["housing"]
            return self._load_and_stack(files) if isinstance(files, list) else self._load_dbf_file(files)
        elif category == "all":
            dfs = []
            for cat in ["food", "nonfood", "housing"]:
                if cat in kp_files:
                    files = kp_files[cat]
                    df = self._load_and_stack(files) if isinstance(files, list) else self._load_dbf_file(files)
                    df = df.with_columns(pl.lit(cat).alias("_kp_category"))
                    dfs.append(df)

            if not dfs:
                raise ValueError("No KP files found")

            return pl.concat(dfs, how="diagonal_relaxed")
        else:
            raise ValueError(f"Invalid category: {category}")

    def _load_dbf_file(self, filename: str) -> pl.DataFrame:
        """Load single DBF or Parquet file using dbfrs or polars."""
        file_path = self._data_dir / filename

        # try parquet first (faster, no data quality issues)
        parquet_path = file_path.with_suffix(".parquet")
        if parquet_path.exists():
            return pl.read_parquet(parquet_path)

        # check for matching parquet in -pq directory
        if "bps-susenas" in str(self._data_dir):
            pq_dir = Path(str(self._data_dir).replace("bps-susenas", "bps-susenas-pq"))
            pq_file = pq_dir / file_path.with_suffix(".parquet").name
            if pq_file.exists():
                return pl.read_parquet(pq_file)

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        # load DBF using dbfrs with field filtering
        all_fields = dbfrs.get_dbf_fields(str(file_path))

        # skip known problematic fields with malformed numeric data
        problematic = ["R1806B"]
        clean_fields = [f for f in all_fields if f.name not in problematic]

        # load with filtered fields
        field_names = [f.name for f in clean_fields]
        records = dbfrs.load_dbf(str(file_path), clean_fields)
        df_pd = pd.DataFrame(records, columns=field_names)
        df = pl.from_pandas(df_pd)

        return df

    def _load_and_stack(self, file_list: List[str]) -> pl.DataFrame:
        """Load multiple files and stack vertically."""
        dfs = []
        for filename in file_list:
            df = self._load_dbf_file(filename)
            dfs.append(df)

        return pl.concat(dfs, how="diagonal_relaxed")

    def _load_config(self, wave: Optional[str] = None):
        """Load configuration from YAML files."""
        config_dir = Path(__file__).parent.parent / "configs" / "susenas"

        if wave:
            wave_config_path = config_dir / f"{wave}.yaml"
            if wave_config_path.exists():
                try:
                    self._config = load_config_with_inheritance(wave_config_path)
                    self._build_reverse_mappings()
                    return
                except Exception as e:
                    print(f"Warning: Failed to load wave config {wave}: {e}")

        # fallback to defaults
        defaults_path = config_dir / "defaults.yaml"
        try:
            with open(defaults_path, "r") as f:
                self._config = yaml.safe_load(f)
            self._build_reverse_mappings()
        except Exception as e:
            print(f"Warning: Failed to load defaults: {e}")
            self._config = None
            self._reverse_mappings = None

    def get_survey_design(self) -> SurveyDesignInfo:
        """Get survey design information for SUSENAS."""
        if not self._metadata:
            raise ValueError("Must load data first")

        return SurveyDesignInfo(
            weight_col="FWT",
            strata_col="STRATA",
            psu_col="PSU",
            finite_population_correction=True,
            domain_cols=["R101", "R105"],
        )

    def _build_reverse_mappings(self) -> None:
        """Build reverse mapping from canonical names to raw SUSENAS field names."""
        if not self._config or "fields" not in self._config:
            self._reverse_mappings = None
            return

        reverse: Dict[str, List[str]] = {}
        for raw_name, field_info in self._config["fields"].items():
            canon = field_info.get("canon_name", raw_name.lower())
            reverse.setdefault(canon, []).append(raw_name)

        self._reverse_mappings = reverse

    def _detect_files(self, wave: str) -> Dict[str, Any]:
        """Auto-detect available files for a wave."""
        pattern = f"susenas_{wave}_*"
        parquet_files = list(self._data_dir.glob(f"{pattern}.parquet"))

        if not parquet_files:
            # check -pq directory
            if "bps-susenas" in str(self._data_dir):
                pq_dir = Path(str(self._data_dir).replace("bps-susenas", "bps-susenas-pq"))
                if pq_dir.exists():
                    parquet_files = list(pq_dir.glob(f"{pattern}.parquet"))

        # fallback to dbf
        files = parquet_files if parquet_files else list(self._data_dir.glob(f"{pattern}.dbf"))

        if not files:
            available_waves = self._get_available_waves()
            raise FileNotFoundError(
                f"No files found for wave {wave}. Available: {', '.join(available_waves) if available_waves else 'none'}"
            )

        # parse and group
        kor_files = {}
        kp_groups = {"food": [], "nonfood": [], "housing": []}

        for file in files:
            parts = file.stem.split("_")
            if len(parts) < 4:
                continue

            module = parts[2]
            submodule = parts[3]

            if module == "kor":
                kor_files[submodule] = file.stem
            elif module == "kp":
                category_map = {"blok41": "food", "blok42": "nonfood", "blok43": "housing"}
                category = category_map.get(submodule)
                if category:
                    kp_groups[category].append(file.stem)

        # convert single-item lists to strings
        kp_files = {}
        for category, files_list in kp_groups.items():
            if len(files_list) == 0:
                continue
            elif len(files_list) == 1:
                kp_files[category] = files_list[0]
            else:
                kp_files[category] = sorted(files_list)

        return {"kor": kor_files, "kp": kp_files}

    def _get_available_waves(self) -> List[str]:
        """Get list of available waves in data directory."""
        files = list(self._data_dir.glob("susenas_*"))

        # check -pq directory too
        if "bps-susenas" in str(self._data_dir):
            pq_dir = Path(str(self._data_dir).replace("bps-susenas", "bps-susenas-pq"))
            if pq_dir.exists():
                files.extend(list(pq_dir.glob("susenas_*")))

        waves = set()
        for file in files:
            parts = file.stem.split("_")
            if len(parts) >= 2:
                waves.add(parts[1])

        return sorted(waves)


def load_susenas(
    file_path: Union[str, Path, None] = None,
    module: Literal["kor", "kp", "both"] = "kor",
    wave: Optional[str] = None,
    **kwargs,
) -> pl.DataFrame:
    """Load SUSENAS socioeconomic survey data.

    Module options: 'kor' (household), 'kp' (consumption), or 'both'.
    Uses SUSENAS_DATA_DIR environment variable if file_path not provided.
    """
    loader = SusenasLoader()
    return loader.load(file_path=file_path, module=module, wave=wave, **kwargs)
