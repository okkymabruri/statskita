"""Cross-wave harmonization for Indonesian statistical surveys."""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import polars as pl


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

    def _load_harmonization_rules(self):
        """Load harmonization rules for the dataset type."""
        if self.dataset_type == "sakernas":
            self._rules = self._get_sakernas_rules()
        elif self.dataset_type == "susenas":
            self._rules = self._get_susenas_rules()
        else:
            self._rules = {}

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
        self, df: pl.DataFrame, source_wave: str, target_variables: Optional[List[str]] = None
    ) -> Tuple[pl.DataFrame, Dict[str, str]]:
        """Harmonize survey data to standard variable names and codes.

        Args:
            df: Input dataframe
            source_wave: Source survey wave (e.g., "2024")
            target_variables: List of variables to harmonize (None for all available)

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

            # apply value mappings
            if rule.value_mappings:
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

        return harmonized_df, mapping_log

    def create_labor_force_indicators(
        self, df: pl.DataFrame, min_working_age: int = 15
    ) -> pl.DataFrame:
        # create standard angkatan kerja indicators after harmonization
        result_df = df.clone()

        # PUK = penduduk usia kerja (working age population)
        result_df = result_df.with_columns(
            (pl.col("age") >= min_working_age).alias("working_age_population")
        )

        # create employment status from work_status (jeniskegia coding)
        if "work_status" in df.columns:
            # BPS jeniskegia codes (Feb 2025 format):
            # 1 = Bekerja (working/employed)
            # 2 = Mencari kerja (looking for work/unemployed)
            # 4 = Sekolah, 5 = Mengurus RT, 6 = Lainnya (all = not in labor force)

            result_df = result_df.with_columns(
                [
                    (pl.col("work_status") == "Working").alias("employed"),
                    (pl.col("work_status") == "Looking for work").alias("unemployed"),
                ]
            )

            # labor force = employed + unemployed
            result_df = result_df.with_columns(
                (pl.col("employed") | pl.col("unemployed")).alias("in_labor_force")
            )

            # not in labor force
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
