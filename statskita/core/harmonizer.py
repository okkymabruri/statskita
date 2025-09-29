"""Cross-wave harmonization for Indonesian statistical surveys."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import polars as pl
import yaml


@dataclass
class VariableMapping:
    """Mapping between different variable names across survey waves."""

    standard_name: str
    wave_names: Dict[str, str]  # wave -> variable_name
    value_mappings: Optional[Dict[str, Dict[Any, Any]]] = None  # wave -> {old_value: new_value}
    description: str = ""


class SurveyHarmonizer:
    """Harmonize variables across different survey waves."""

    def __init__(self, dataset_type: str = "sakernas"):
        self.dataset_type = dataset_type
        self._load_harmonization_rules()

    def _load_yaml_rules(self) -> Dict[str, VariableMapping]:
        """Parse all YAML config files and return harmonization rules.

        Reads from configs/<dataset_type>/*.yaml and converts to VariableMapping objects.
        """
        cfg_dir = Path(__file__).parents[1] / "configs" / self.dataset_type
        rules: Dict[str, VariableMapping] = {}

        if not cfg_dir.exists():
            return rules

        for yaml_path in cfg_dir.glob("*.yaml"):
            # skip base.yaml as it's extended by other files
            if yaml_path.stem == "base":
                continue

            try:
                with open(yaml_path, "r", encoding="utf-8") as f:
                    cfg = yaml.safe_load(f)

                wave = cfg.get("wave")
                if not wave:
                    continue

                overrides = cfg.get("overrides", {})
                for field_name, meta in overrides.items():
                    canon_name = meta.get("canon_name")
                    if not canon_name:
                        continue

                    # create or update mapping
                    if canon_name not in rules:
                        rules[canon_name] = VariableMapping(
                            standard_name=canon_name,
                            wave_names={},
                            value_mappings={},
                            description=meta.get("label", "")
                        )

                    # add wave-specific mapping
                    rules[canon_name].wave_names[wave] = field_name

                    # add value labels if present
                    if "value_labels" in meta:
                        rules[canon_name].value_mappings[wave] = meta["value_labels"]

            except Exception as e:
                print(f"Warning: Failed to load {yaml_path}: {e}")

        return rules

    def _load_harmonization_rules(self):
        """Load harmonization rules for the dataset type."""
        # load yaml-based rules
        yaml_rules = self._load_yaml_rules()

        # load hardcoded rules (as fallback)
        if self.dataset_type == "sakernas":
            code_rules = self._get_sakernas_rules()
        elif self.dataset_type == "susenas":
            code_rules = self._get_susenas_rules()
        else:
            code_rules = {}

        # merge: yaml overrides hardcoded
        self._rules = {**code_rules, **yaml_rules}

    def _get_sakernas_rules(self) -> Dict[str, VariableMapping]:
        """Get harmonization rules for SAKERNAS."""
        return {
            "province_code": VariableMapping(
                standard_name="province_code",
                wave_names={
                    "2021": "PROV",
                    "2022": "PROV",
                    "2023": "PROV",
                    "2024": "PROV",
                    "2025": "kode_prov",
                    "2025-02": "KODE_PROV",
                },
                description="Province code (2-digit)",
            ),
            "urban_rural": VariableMapping(
                standard_name="urban_rural",
                wave_names={
                    "2021": "B1R5",
                    "2022": "B1R5",
                    "2023": "B1R5",
                    "2024": "B1R5",
                    "2025": "B1R5",
                },
                value_mappings={"all": {1: "Urban", 2: "Rural"}},
                description="Urban/Rural classification",
            ),
            "age": VariableMapping(
                standard_name="age",
                wave_names={
                    "2021": "B4K5",
                    "2022": "B4K5",
                    "2023": "B4K5",
                    "2024": "B4K5",
                    "2025": "dem_age",
                    "2025-02": "DEM_AGE",
                },
                description="Age in completed years",
            ),
            "gender": VariableMapping(
                standard_name="gender",
                wave_names={
                    "2021": "B4K4",
                    "2022": "B4K4",
                    "2023": "B4K4",
                    "2024": "B4K4",
                    "2025": "dem_sex",
                    "2025-02": "DEM_SEX",
                },
                value_mappings={"all": {1: "Male", 2: "Female"}},
                description="Gender",
            ),
            "education_level": VariableMapping(
                standard_name="education_level",
                wave_names={
                    "2021": "B4K8",
                    "2022": "B4K8",
                    "2023": "B4K8",
                    "2024": "B4K8",
                    "2025": "dem_sklh",
                    "2025-02": "DEM_SKLH",
                },
                value_mappings={
                    "all": {
                        1: "No Education",
                        2: "Elementary (not completed)",
                        3: "Elementary",
                        4: "Junior High",
                        5: "Senior High",
                        6: "Academy/Diploma",
                        7: "University",
                    }
                },
                description="Highest education level completed",
            ),
            "work_status": VariableMapping(
                standard_name="work_status",
                wave_names={
                    "2021": "B5R1",
                    "2022": "B5R1",
                    "2023": "B5R1",
                    "2024": "B5R1",
                    "2025": "B5R1",
                    "2025-02": "JENISKEGIA",
                },
                value_mappings={
                    "all": {1: "Working", 2: "Not Working"},
                    "2025-02": {
                        1: "Working",
                        2: "Looking for work",
                        3: "Not Working",
                        4: "Not Working",
                        5: "Not Working",
                        6: "Not Working",
                    },
                },
                description="Work status in reference week",
            ),
            "hours_worked": VariableMapping(
                standard_name="hours_worked",
                wave_names={
                    "2021": "B5R28",
                    "2022": "B5R28",
                    "2023": "B5R28",
                    "2024": "B5R28",
                    "2025": "B5R28",
                    "2025-02": "WKT_JML_U",
                },
                description="Total hours worked in reference week",
            ),
            "survey_weight": VariableMapping(
                standard_name="survey_weight",
                wave_names={
                    "2021": "WEIGHT",
                    "2022": "WEIGHT",
                    "2023": "WEIGHT",
                    "2024": "WEIGHT",
                    "2025": "WEIGHT",
                },
                description="Survey sampling weight",
            ),
        }

    def _get_susenas_rules(self) -> Dict[str, VariableMapping]:
        """Get harmonization rules for SUSENAS."""
        # placeholder for SUSENAS rules
        return {}

    def harmonize(
        self, df: pl.DataFrame, source_wave: str, target_variables: Optional[List[str]] = None,
        preserve_labels: bool = False
    ) -> Tuple[pl.DataFrame, Dict[str, str]]:
        """Harmonize survey data to standard variable names and codes.

        Args:
            df: Input dataframe
            source_wave: Source survey wave (e.g., "2024")
            target_variables: List of variables to harmonize (None for all available)
            preserve_labels: If False, convert numeric codes to human-readable labels

        Returns:
            Tuple of (harmonized_dataframe, mapping_log)
        """
        if target_variables is None:
            target_variables = list(self._rules.keys())

        harmonized_df = df.clone()
        mapping_log = {}

        # create case-insensitive column lookup
        column_map = {col.lower(): col for col in df.columns}

        for target_var in target_variables:
            if target_var not in self._rules:
                continue

            rule = self._rules[target_var]

            # find source variable name (case-insensitive)
            source_var = rule.wave_names.get(source_wave)
            if not source_var:
                continue

            # case-insensitive lookup
            actual_column = column_map.get(source_var.lower())
            if not actual_column:
                continue

            # rename variable
            if actual_column != rule.standard_name:
                harmonized_df = harmonized_df.rename({actual_column: rule.standard_name})
                mapping_log[actual_column] = rule.standard_name

            # apply value mappings if preserve_labels=False
            if not preserve_labels and rule.value_mappings:
                value_map = rule.value_mappings.get(source_wave) or rule.value_mappings.get("all")
                if value_map:
                    # create mapping expression
                    harmonized_df = harmonized_df.with_columns(
                        pl.col(rule.standard_name)
                        .replace_strict(
                            old=list(value_map.keys()), new=list(value_map.values()), default=None
                        )
                        .alias(rule.standard_name)
                    )

        # apply value labels to all fields from config (not just harmonized ones)
        if not preserve_labels:
            # load config to get all value labels
            from pathlib import Path

            import yaml
            config_dir = Path(__file__).parent.parent / "configs" / self.dataset_type
            wave_config = config_dir / f"{source_wave}.yaml"

            if wave_config.exists():
                try:
                    with open(wave_config, "r", encoding="utf-8") as f:
                        cfg = yaml.safe_load(f)

                    # apply value labels from overrides section
                    overrides = cfg.get("overrides", {})
                    for field_name, meta in overrides.items():
                        value_labels = meta.get("value_labels")
                        if value_labels and field_name in harmonized_df.columns:
                            # apply labels to this field
                            harmonized_df = harmonized_df.with_columns(
                                pl.col(field_name)
                                .replace_strict(
                                    old=list(value_labels.keys()),
                                    new=list(value_labels.values()),
                                    default=None
                                )
                                .alias(field_name)
                            )
                            mapping_log[f"{field_name}_labels"] = "applied"

                    # also check base.yaml for value_labels
                    base_config = config_dir / "base.yaml"
                    if base_config.exists():
                        with open(base_config, "r", encoding="utf-8") as f:
                            base_cfg = yaml.safe_load(f)

                        fields = base_cfg.get("fields", {})
                        for field_name, field_info in fields.items():
                            value_labels = field_info.get("value_labels")
                            if value_labels and field_name in harmonized_df.columns:
                                # only apply if not already applied from overrides
                                if f"{field_name}_labels" not in mapping_log:
                                    harmonized_df = harmonized_df.with_columns(
                                        pl.col(field_name)
                                        .replace_strict(
                                            old=list(value_labels.keys()),
                                            new=list(value_labels.values()),
                                            default=None
                                        )
                                        .alias(field_name)
                                    )
                                    mapping_log[f"{field_name}_labels"] = "applied_from_base"

                except Exception as e:
                    print(f"Warning: Could not apply all value labels: {e}")

        return harmonized_df, mapping_log

    def create_labor_force_indicators(
        self, df: pl.DataFrame, min_working_age: int = 15
    ) -> pl.DataFrame:
        # create standard angkatan kerja indicators after harmonization
        result_df = df.clone()

        # PUK = penduduk usia kerja (working age population)
        if "age" in df.columns:
            result_df = result_df.with_columns(
                (pl.col("age") >= min_working_age).alias("working_age_population")
            )

        # Handle different ways of determining employment status
        # For 1994 data: main_activity (b4p4) is the primary indicator
        if "main_activity" in df.columns:
            # b4p4 codes: 1=Working, 2=Looking for work, 3=School, 4=Other, 5=Housekeeping
            # Check if we have numeric codes (before value mapping)
            sample_val = df["main_activity"].drop_nulls().head(1)
            if len(sample_val) > 0 and isinstance(sample_val[0], (int, float)):
                # Numeric codes
                result_df = result_df.with_columns([
                    (pl.col("main_activity") == 1).alias("employed"),
                    (pl.col("main_activity") == 2).alias("unemployed")
                ])
            else:
                # After value mapping - need to handle string values
                # This would need mapping in the YAML config
                result_df = result_df.with_columns([
                    (pl.col("main_activity") == "Working").alias("employed"),
                    (pl.col("main_activity") == "Looking for work").alias("unemployed")
                ])

        # Fallback: Use worked_1hour (b4p5) if main_activity not available
        elif "worked_1hour" in df.columns:
            # b4p5: 1=worked at least 1 hour, 2=didn't work
            sample_val = df["worked_1hour"].drop_nulls().head(1)
            if len(sample_val) > 0 and isinstance(sample_val[0], str):
                # String values: "Ya"/"Tidak"
                result_df = result_df.with_columns(
                    (pl.col("worked_1hour") == "Ya").alias("employed")
                )
                # For unemployment: also check those with work but temporarily absent
                if "temp_not_working" in df.columns:
                    result_df = result_df.with_columns(
                        pl.when(pl.col("temp_not_working") == "Ya")
                        .then(True)
                        .otherwise(pl.col("employed"))
                        .alias("employed")
                    )

                if "job_seeking" in df.columns:
                    result_df = result_df.with_columns(
                        ((~pl.col("employed")) & (pl.col("job_seeking") == "Ya")).alias("unemployed")
                    )
                else:
                    result_df = result_df.with_columns(pl.lit(False).alias("unemployed"))
            else:
                # Integer values
                result_df = result_df.with_columns(
                    (pl.col("worked_1hour") == 1).alias("employed")
                )
                # Include temporarily not working
                if "temp_not_working" in df.columns:
                    result_df = result_df.with_columns(
                        pl.when(pl.col("temp_not_working") == 1)
                        .then(True)
                        .otherwise(pl.col("employed"))
                        .alias("employed")
                    )

                if "job_seeking" in df.columns:
                    result_df = result_df.with_columns(
                        ((~pl.col("employed")) & (pl.col("job_seeking") == 1)).alias("unemployed")
                    )
                else:
                    result_df = result_df.with_columns(pl.lit(False).alias("unemployed"))

        # Modern format: work_status column
        elif "work_status" in df.columns:
            # BPS jeniskegia codes (Feb 2025 format):
            # 1 = Bekerja (working/employed)
            # 2 = Mencari kerja (looking for work/unemployed)
            # 3-6 = Sekolah, Mengurus RT, Lainnya, Tidak mampu (all = not in labor force)

            # Check if values are already strings (after harmonization)
            sample_val = df["work_status"].drop_nulls().head(1)
            if len(sample_val) > 0 and isinstance(sample_val[0], str):
                # Indonesian labels
                result_df = result_df.with_columns(
                    [
                        (pl.col("work_status") == "Bekerja").alias("employed"),
                        (pl.col("work_status") == "Mencari pekerjaan").alias("unemployed"),
                    ]
                )
            else:
                # English labels (legacy support)
                result_df = result_df.with_columns(
                    [
                        (pl.col("work_status") == "Working").alias("employed"),
                        (pl.col("work_status") == "Looking for work").alias("unemployed"),
                    ]
                )

        # Calculate labor force if we have employment indicators
        if "employed" in result_df.columns and "unemployed" in result_df.columns:
            # labor force = employed + unemployed
            result_df = result_df.with_columns(
                (pl.col("employed") | pl.col("unemployed")).alias("in_labor_force")
            )

            # not in labor force
            if "working_age_population" in result_df.columns:
                result_df = result_df.with_columns(
                    (pl.col("working_age_population") & ~pl.col("in_labor_force")).alias(
                        "not_in_labor_force"
                    )
                )

        # underemployment (working < 35 hours and willing to work more)
        if "hours_worked" in df.columns:
            result_df = result_df.with_columns(
                (pl.col("employed") & (pl.col("hours_worked") < 35)).alias("underemployed")
            )

        # create in_school indicator from DEM_SKLH or school_participation
        if "DEM_SKLH" in df.columns:
            # check if already converted to text labels
            sample_val = df["DEM_SKLH"].drop_nulls().head(1)
            if len(sample_val) > 0 and isinstance(sample_val[0], str):
                result_df = result_df.with_columns(
                    (pl.col("DEM_SKLH") == "Masih sekolah").alias("in_school")
                )
            else:
                # numeric: 2 = Masih sekolah
                result_df = result_df.with_columns(
                    (pl.col("DEM_SKLH") == 2).alias("in_school")
                )
        elif "school_participation" in df.columns:
            # harmonized name
            sample_val = df["school_participation"].drop_nulls().head(1)
            if len(sample_val) > 0 and isinstance(sample_val[0], str):
                result_df = result_df.with_columns(
                    (pl.col("school_participation") == "Masih sekolah").alias("in_school")
                )
            else:
                result_df = result_df.with_columns(
                    (pl.col("school_participation") == 2).alias("in_school")
                )

        return result_df

    def get_available_variables(self, wave: str) -> List[Tuple[str, str, str]]:
        # list harmonizable vars for this wave
        available = []
        for standard_name, rule in self._rules.items():
            source_name = rule.wave_names.get(wave)
            if source_name:
                available.append((standard_name, source_name, rule.description))

        return available

    def validate_harmonization(
        self, original_df: pl.DataFrame, harmonized_df: pl.DataFrame, wave: str
    ) -> Dict[str, Any]:
        """Validate harmonization results.

        Args:
            original_df: Original dataframe
            harmonized_df: Harmonized dataframe
            wave: Survey wave

        Returns:
            Validation report
        """
        report = {
            "wave": wave,
            "original_shape": original_df.shape,
            "harmonized_shape": harmonized_df.shape,
            "variables_mapped": [],
            "value_mappings_applied": [],
            "missing_variables": [],
            "validation_passed": True,
        }

        # check which variables were successfully mapped
        for standard_name, rule in self._rules.items():
            source_name = rule.wave_names.get(wave)
            if source_name and source_name in original_df.columns:
                if standard_name in harmonized_df.columns:
                    report["variables_mapped"].append(f"{source_name} -> {standard_name}")
                else:
                    report["missing_variables"].append(standard_name)
                    report["validation_passed"] = False

        # check row count consistency
        if original_df.shape[0] != harmonized_df.shape[0]:
            report["validation_passed"] = False
            report["error"] = "Row count mismatch between original and harmonized data"

        return report
