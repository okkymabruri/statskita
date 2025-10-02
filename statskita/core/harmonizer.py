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
                            description=meta.get("label", ""),
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
                description="Urban/Rural classification",
            ),
            "age": VariableMapping(
                standard_name="age",
                wave_names={
                    "2021": "B4K5",
                    "2022": "B4K5",
                    "2023": "B4K5",
                    "2024": "B4K5",
                    "2024-08": "K10",  # August 2024 - direct age value
                    "2025-02": "DEM_AGE",
                    # Note: 2023-02, 2023-08, 2024-02, 2024-08 use birth date columns that need calculation
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
                    "2024-08": "K4",  # August 2024
                    "2025-02": "DEM_SEX",
                },
                description="Gender",
            ),
            "education_level": VariableMapping(
                standard_name="education_level",
                wave_names={
                    "2021": "B4K8",
                    "2022": "B4K8",
                    "2023": "B4K8",
                    "2024": "B4K8",
                    "2025-02": "DEM_SKLH",
                },
                description="Highest education level completed",
            ),
            "work_status": VariableMapping(
                standard_name="work_status",
                wave_names={
                    "2021": "B5R1",
                    "2022": "B5R1",
                    "2023": "B5R1",
                    "2023-02": "JENISKEGIA",
                    "2023-08": "JENISKEGIA",
                    "2024": "B5R1",
                    "2024-02": "JENISKEGIA",
                    # Note: 2024-08 doesn't have direct work_status, needs derivation from R10A and R11
                    "2025": "B5R1",
                    "2025-02": "JENISKEGIA",
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
                    "2024-08": "R19A_JML",  # August 2024 total hours
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
                    "2023-02": "WEIGHT",
                    "2023-08": "WEIGHTR",  # different name in Aug 2023
                    "2024": "WEIGHT",
                    "2024-02": "WEIGHTR",  # different name in Feb 2024
                    "2024-08": "WEIGHT",
                    "2025": "WEIGHT",
                    "2025-02": "WEIGHT",
                },
                description="Survey sampling weight",
            ),
            "status_pekerjaan": VariableMapping(
                standard_name="status_pekerjaan",
                wave_names={
                    "2021": "MJJ_EMPREL",
                    "2022": "MJJ_EMPREL",
                    "2023": "MJJ_EMPREL",
                    "2024": "MJJ_EMPREL",
                    "2025": "MJJ_EMPREL",
                    "2025-02": "MJJ_EMPREL",
                },
                description="Status pekerjaan utama (Main job employment status)",
            ),
            "employment_status": VariableMapping(
                standard_name="employment_status",
                wave_names={
                    "2021": "STATUS_PEK",
                    "2022": "STATUS_PEK",
                    "2023": "STATUS_PEK",
                    "2024": "STATUS_PEK",
                    "2025": "STATUS_PEK",
                    "2025-02": "STATUS_PEK",
                },
                description="Employment status (1-7 scale for formal/informal)",
            ),
            "wage_cash": VariableMapping(
                standard_name="wage_cash",
                wave_names={
                    "2021": "MJJ_UPAH_U",
                    "2022": "MJJ_UPAH_U",
                    "2023": "MJJ_UPAH_U",
                    "2024": "MJJ_UPAH_U",
                    "2025": "MJJ_UPAH_U",
                    "2025-02": "MJJ_UPAH_U",
                },
                description="Wages/salary in cash from main job (monthly)",
            ),
            "wage_goods": VariableMapping(
                standard_name="wage_goods",
                wave_names={
                    "2021": "MJJ_UPAH_B",
                    "2022": "MJJ_UPAH_B",
                    "2023": "MJJ_UPAH_B",
                    "2024": "MJJ_UPAH_B",
                    "2025": "MJJ_UPAH_B",
                    "2025-02": "MJJ_UPAH_B",
                },
                description="Wages/salary in goods from main job (monthly value)",
            ),
            "psu": VariableMapping(
                standard_name="psu",
                wave_names={
                    "2021": "PSU",
                    "2022": "PSU",
                    "2023": "PSU",
                    "2023-02": "PSU",
                    "2023-08": "PSU",
                    "2024": "PSU",
                    "2024-02": "PSU",
                    "2024-08": "PSU",
                    "2025": "PSU",
                    "2025-02": "PSU",
                },
                description="Primary Sampling Unit (cluster)",
            ),
            "ssu": VariableMapping(
                standard_name="ssu",
                wave_names={
                    "2021": "SSU",
                    "2022": "SSU",
                    "2023": "SSU",
                    "2023-02": "SSU",
                    "2024": "SSU",
                    "2024-02": "SSU",
                    "2025": "SSU",
                    "2025-02": "SSU",
                    # Note: 2023-08 and 2024-08 don't have SSU
                },
                description="Secondary Sampling Unit",
            ),
            "strata": VariableMapping(
                standard_name="strata",
                wave_names={
                    "2021": "STRATA",
                    "2022": "STRATA",
                    "2023": "STRATA",
                    "2023-02": "STRATA",
                    "2023-08": "STRATA",
                    "2024": "STRATA",
                    "2024-02": "STRATA",
                    "2024-08": "STRATA",
                    "2025": "STRATA",
                    "2025-02": "STRATA",
                },
                description="Sampling stratum",
            ),
        }

    def _get_susenas_rules(self) -> Dict[str, VariableMapping]:
        """Get harmonization rules for SUSENAS."""
        # placeholder for SUSENAS rules
        return {}

    def harmonize(
        self,
        df: pl.DataFrame,
        source_wave: str,
        target_variables: Optional[List[str]] = None,
        preserve_labels: bool = False,
        preserve_original_names: bool = False,
    ) -> Tuple[pl.DataFrame, Dict[str, str]]:
        """Harmonize survey data to standard variable names and codes.

        Args:
            df: Input dataframe
            source_wave: Source survey wave (e.g., "2024")
            target_variables: List of variables to harmonize (None for all available)
            preserve_labels: If False, convert numeric codes to human-readable labels
            preserve_original_names: If True, keep original BPS field names instead of canonical names

        Returns:
            Tuple of (harmonized_dataframe, mapping_log)
        """
        if target_variables is None:
            target_variables = list(self._rules.keys())

        harmonized_df = df.clone()
        mapping_log = {}

        # Map age column using config system (proper 3-tier design)
        if "age" in self._rules and "age" not in harmonized_df.columns:
            age_rule = self._rules["age"]
            age_source = age_rule.wave_names.get(source_wave)

            if age_source and age_source in df.columns:
                # Config-driven mapping (primary)
                harmonized_df = harmonized_df.with_columns(pl.col(age_source).alias("age"))
                mapping_log[age_source] = "age"
            else:
                # Minimal fallback for waves without config
                for fallback_col in ["DEM_AGE", "K10", "B4K10", "K9"]:
                    if fallback_col in df.columns:
                        harmonized_df = harmonized_df.with_columns(pl.col(fallback_col).alias("age"))
                        mapping_log[fallback_col] = "age (fallback)"
                        break

        # Use JENISKEGIA for work status classification
        # JENISKEGIA is BPS pre-calculated employment status variable
        # Available in: 2023-02, 2023-08, 2024-02, 2025-02
        # Values: 1=Employed, 2=Unemployed, 4=School, 5=Housework, 6=Other
        if "JENISKEGIA" in df.columns:
            harmonized_df = harmonized_df.with_columns(
                pl.col("JENISKEGIA").alias("work_status")
            )
            mapping_log["JENISKEGIA"] = "work_status"

        # Note: 2024-08 wave is under investigation for work status derivation
        # Production waves: 2023-02, 2023-08, 2024-02, 2025-02

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

            # rename variable (only if preserve_original_names is False)
            # skip if target column already exists (e.g., special handling created it)
            if (
                not preserve_original_names
                and actual_column != rule.standard_name
                and rule.standard_name not in harmonized_df.columns
            ):
                harmonized_df = harmonized_df.rename({actual_column: rule.standard_name})
                mapping_log[actual_column] = rule.standard_name

            # apply value mappings if preserve_labels=False
            if not preserve_labels and rule.value_mappings:
                value_map = rule.value_mappings.get(source_wave) or rule.value_mappings.get("all")
                if value_map:
                    # use appropriate column name based on preserve_original_names setting
                    target_col = actual_column if preserve_original_names else rule.standard_name
                    # create mapping expression
                    harmonized_df = harmonized_df.with_columns(
                        pl.col(target_col)
                        .replace_strict(
                            old=list(value_map.keys()), new=list(value_map.values()), default=None
                        )
                        .alias(target_col)
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

                    # Load codelists from base.yaml first
                    codelists = {}
                    base_config = config_dir / "base.yaml"
                    if base_config.exists():
                        with open(base_config, "r", encoding="utf-8") as f:
                            base_cfg = yaml.safe_load(f)
                            codelists = base_cfg.get("codelists", {})

                    # apply value labels from overrides section
                    overrides = cfg.get("overrides", {})
                    for field_name, meta in overrides.items():
                        # Check for direct value_labels first
                        value_labels = meta.get("value_labels")

                        # If no value_labels, check for codelist reference
                        if not value_labels and "codelist" in meta:
                            codelist_name = meta["codelist"]
                            if codelist_name in codelists:
                                value_labels = codelists[codelist_name]

                        # check both original field name and canon_name
                        canon_name = meta.get("canon_name", field_name)
                        target_field = None

                        if field_name in harmonized_df.columns:
                            target_field = field_name
                        elif canon_name in harmonized_df.columns:
                            target_field = canon_name

                        if value_labels and target_field:
                            # apply labels to this field
                            harmonized_df = harmonized_df.with_columns(
                                pl.col(target_field)
                                .replace_strict(
                                    old=list(value_labels.keys()),
                                    new=list(value_labels.values()),
                                    default=None,
                                )
                                .alias(target_field)
                            )
                            mapping_log[f"{target_field}_labels"] = "applied"

                    # also check base.yaml for value_labels
                    base_config = config_dir / "base.yaml"
                    if base_config.exists():
                        with open(base_config, "r", encoding="utf-8") as f:
                            base_cfg = yaml.safe_load(f)

                        fields = base_cfg.get("fields", {})
                        for field_name, field_info in fields.items():
                            # Check for direct value_labels
                            value_labels = field_info.get("value_labels")

                            # If no value_labels, check for codelist reference
                            if not value_labels and "codelist" in field_info:
                                codelist_name = field_info["codelist"]
                                if codelist_name in codelists:
                                    value_labels = codelists[codelist_name]

                            # check both original field name and canon_name
                            canon_name = field_info.get("canon_name", field_name)
                            target_field = None

                            if field_name in harmonized_df.columns:
                                target_field = field_name
                            elif canon_name in harmonized_df.columns:
                                target_field = canon_name

                            if value_labels and target_field:
                                # only apply if not already applied from overrides
                                if f"{target_field}_labels" not in mapping_log:
                                    harmonized_df = harmonized_df.with_columns(
                                        pl.col(target_field)
                                        .replace_strict(
                                            old=list(value_labels.keys()),
                                            new=list(value_labels.values()),
                                            default=None,
                                        )
                                        .alias(target_field)
                                    )
                                    mapping_log[f"{target_field}_labels"] = "applied_from_base"

                except Exception:
                    # silently skip value label errors - they're not critical
                    pass

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
        # For modern SAKERNAS (2021+): work_status column
        if "work_status" in df.columns:
            # BPS work status codes (B5R1):
            # 1 = Bekerja (Working)
            # 2 = Pernah bekerja tetapi sedang tidak bekerja (Had work but temporarily not working)
            # 3 = Sekolah (School)
            # 4 = Mengurus rumah tangga (Housekeeping)
            # 5 = Lainnya (Other)
            # 6 = Tidak mampu bekerja (Unable to work)
            #
            # JENISKEGIA codes (2023-2025 format):
            # 1 = Bekerja (Working)
            # 2 = Pengangguran (Unemployed) - DIFFERENT from B5R1!
            # 4 = Mengurus rumah tangga (Housekeeping)
            # 5 = Lainnya (Other)
            # 6 = Tidak mampu bekerja (Unable to work)

            # Check data type
            col_dtype = df["work_status"].dtype

            if col_dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.Decimal]:
                # Numeric codes - most common in modern data
                # Status 1 = Bekerja (Working) - clearly employed
                # Status 2 = JENISKEGIA: Unemployed OR B5R1: Temporarily not working
                # We'll handle the distinction later
                result_df = result_df.with_columns(
                    [
                        pl.col("work_status").is_in([1]).alias("employed"),
                        pl.col("work_status").is_in([4, 5, 6]).alias("not_working"),
                        pl.col("work_status")
                        .is_in([2])
                        .alias("status_2"),  # ambiguous - handle below
                    ]
                )
            else:
                # String values (if value labels were applied)
                result_df = result_df.with_columns(
                    [
                        (pl.col("work_status") == "Bekerja").alias("employed"),
                        pl.col("work_status")
                        .is_in(
                            ["Sekolah", "Mengurus rumah tangga", "Lainnya", "Tidak mampu bekerja"]
                        )
                        .alias("not_working"),
                        (
                            pl.col("work_status") == "Pernah bekerja tetapi sedang tidak bekerja"
                        ).alias("temp_not_working"),
                    ]
                )

            # Handle status code 2 - different meaning depending on source
            # If JENISKEGIA: code 2 = Pengangguran (directly unemployed)
            # If B5R1: code 2 = temporarily not working (need job seeking check)
            #
            # Heuristic: JENISKEGIA doesn't have code 3 (Sekolah), B5R1 does
            has_code_3 = False
            if "work_status" in df.columns:
                has_code_3 = df.filter(pl.col("work_status") == 3).height > 0

            if "status_2" in result_df.columns:
                if not has_code_3:
                    # JENISKEGIA format: code 2 = unemployed directly
                    #
                    # KNOWN LIMITATION (2023-02, 2024-02):
                    # =====================================
                    # BPS unemployment definition (ILO 19th ICLS standards) includes:
                    # 1. Without work during reference week
                    # 2. Currently available for work
                    # 3. Seeking work OR waiting to start a job/business
                    #
                    # JENISKEGIA=2 ("Pengangguran") captures actively job-seeking unemployed,
                    # but appears to exclude:
                    # - Those waiting to start a new job (have offer, waiting start date)
                    # - Those preparing to start a business (made preparations, waiting to begin)
                    #
                    # This causes ~2-3pp underestimation in 2023-02 (3.1% vs 5.45% official)
                    # and 2024-02 (2.1% vs 4.82% official). The issue is resolved in 2025-02
                    # (4.76% matches official 4.76%).
                    #
                    # Root cause: Survey questionnaire flow in 2023-02/2024-02 does not
                    # have accessible columns to identify "waiting to start" categories.
                    # R10 (worked last week) is sparsely populated.
                    # R22A-R25 are only answered by employed seeking additional work.
                    #
                    # Users should be aware of this systematic undercount in historical data.
                    result_df = result_df.with_columns(pl.col("status_2").alias("unemployed"))
                else:
                    # B5R1 format: code 2 = temp not working, check job seeking
                    job_seeking_col = None
                    if "looking_for_work" in df.columns:
                        job_seeking_col = "looking_for_work"
                    elif "SRH_KERJA" in df.columns:
                        job_seeking_col = "SRH_KERJA"
                    elif "B5R17" in df.columns:
                        job_seeking_col = "B5R17"

                    if job_seeking_col:
                        # Check if job_seeking_col is numeric or string
                        col_dtype = result_df[job_seeking_col].dtype

                        if col_dtype in [pl.Int32, pl.Int64, pl.Float32, pl.Float64]:
                            seeking_condition = pl.col(job_seeking_col) == 1
                            not_seeking_condition = pl.col(job_seeking_col) != 1
                        else:
                            seeking_condition = pl.col(job_seeking_col).str.starts_with("Ya")
                            not_seeking_condition = ~pl.col(job_seeking_col).str.starts_with("Ya")

                        # Unemployed = temporarily not working AND actively seeking work
                        result_df = result_df.with_columns(
                            (pl.col("status_2") & seeking_condition).alias("unemployed")
                        )

                        # Update employed to include temp_not_working who are NOT looking for work
                        result_df = result_df.with_columns(
                            pl.when(pl.col("status_2") & not_seeking_condition)
                            .then(True)
                            .otherwise(pl.col("employed"))
                            .alias("employed")
                        )
                    else:
                        # No job seeking column, can't determine unemployment from B5R1
                        result_df = result_df.with_columns(pl.lit(False).alias("unemployed"))
            else:
                result_df = result_df.with_columns(pl.lit(False).alias("unemployed"))

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
                result_df = result_df.with_columns((pl.col("DEM_SKLH") == 2).alias("in_school"))
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

        # Create informal employment indicator
        # According to BPS: formal = status 3 (self-employed with permanent paid workers) & 4 (employee)
        # informal = status 1, 2, 5, 6, 7
        if "employment_status" in result_df.columns:
            # Check if numeric or string
            col_dtype = result_df["employment_status"].dtype
            if col_dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.Decimal]:
                # Numeric: formal = 3 or 4, informal = 1, 2, 5, 6, 7
                result_df = result_df.with_columns(
                    [
                        pl.col("employment_status").is_in([3, 4]).alias("formal_employment"),
                        pl.col("employment_status")
                        .is_in([1, 2, 5, 6, 7])
                        .alias("informal_employment"),
                    ]
                )
            else:
                # String values - would need mapping
                result_df = result_df.with_columns(
                    [
                        pl.lit(False).alias("formal_employment"),
                        pl.lit(False).alias("informal_employment"),
                    ]
                )
        elif "STATUS_PEK" in result_df.columns:
            # Direct check on STATUS_PEK if employment_status not harmonized
            col_dtype = result_df["STATUS_PEK"].dtype
            if col_dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.Decimal]:
                result_df = result_df.with_columns(
                    [
                        pl.col("STATUS_PEK").is_in([3, 4]).alias("formal_employment"),
                        pl.col("STATUS_PEK").is_in([1, 2, 5, 6, 7]).alias("informal_employment"),
                    ]
                )

        # Create total wages indicator (cash + goods)
        if "wage_cash" in result_df.columns:
            if "wage_goods" in result_df.columns:
                result_df = result_df.with_columns(
                    (pl.col("wage_cash").fill_null(0) + pl.col("wage_goods").fill_null(0)).alias(
                        "total_wage"
                    )
                )
            else:
                result_df = result_df.with_columns(pl.col("wage_cash").alias("total_wage"))

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
