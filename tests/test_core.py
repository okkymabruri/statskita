"""Core tests to ensure package works."""

import polars as pl


def test_imports():
    """Test that all main imports work."""
    # core imports
    import statskita
    from statskita import calculate_indicators, declare_survey, load_sakernas, wrangle

    # check version
    assert hasattr(statskita, "__version__")
    assert statskita.__version__ == "0.2.0"

    # check main functions exist
    assert callable(load_sakernas)
    assert callable(wrangle)
    assert callable(declare_survey)
    assert callable(calculate_indicators)


def test_loader_initialization():
    """Test that loader can be initialized without data."""
    from statskita.loaders.sakernas import SakernasLoader

    loader = SakernasLoader()
    assert loader is not None

    # check config loading for all available waves
    available_waves = ["2024-02", "2024-08", "2025-02"]

    for wave in available_waves:
        loader._load_config(wave=wave)
        config = loader.get_config()
        assert config is not None
        assert "dataset" in config
        assert config["dataset"] == "sakernas"
        assert "wave" in config
        assert config["wave"] == wave


def test_list_categories():
    """Test category listing functionality."""
    from statskita.loaders.sakernas import SakernasLoader

    loader = SakernasLoader()
    loader._load_config(wave="2025-02")

    categories = loader.list_categories()
    assert isinstance(categories, list)
    assert len(categories) > 0
    assert "demographics" in categories


def test_describe_variable():
    """Test variable description."""
    from statskita.loaders.sakernas import SakernasLoader

    loader = SakernasLoader()
    loader._load_config(wave="2025-02")

    # describe specific variable
    info = loader.describe("DEM_AGE")
    assert info is not None
    assert "label" in info
    assert "category" in info


def test_survey_design_creation():
    """Test survey design can be created with dummy data."""
    from statskita.core.survey import SurveyDesign

    # create minimal dummy data
    df = pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "weight": [1.0, 1.5, 1.2, 1.0, 1.3],
            "strata": [1, 1, 2, 2, 3],
            "psu": [1, 1, 2, 2, 3],
            "age": [25, 30, 35, 40, 45],
            "employed": [True, True, False, True, False],
        }
    )

    # create design
    design = SurveyDesign(df, weight_col="weight", strata_col="strata", psu_col="psu")
    assert design is not None
    assert design.weight_col == "weight"
    assert design.strata_col == "strata"
    assert design.psu_col == "psu"

    # get summary
    summary = design.summary()
    assert "sample_size" in summary
    assert summary["sample_size"] == 5


def test_wrangler_initialization():
    """Test wrangler can be initialized."""
    from statskita.core.wrangler import DataWrangler

    wrangler = DataWrangler()
    assert wrangler is not None

    # test with dummy data
    df = pl.DataFrame({"age": [15, 20, 25, 30, 65], "weight": [1.0, 1.0, 1.0, 1.0, 1.0]})

    # basic wrangling
    result = wrangler.wrangle(df, min_working_age=15)
    assert result is not None
    assert len(result) == len(df)


def test_indicator_calculation_dummy():
    """Test indicator calculation with dummy data."""
    from statskita.core.indicators import IndicatorCalculator
    from statskita.core.survey import SurveyDesign

    # create dummy data
    df = pl.DataFrame(
        {
            "weight": [1.0] * 10,
            "strata": [1] * 5 + [2] * 5,
            "psu": list(range(1, 11)),
            "age": [25, 30, 35, 40, 45, 50, 55, 60, 20, 22],
            "working_age_population": [True] * 10,
            "employed": [True, True, False, True, False, True, True, False, True, True],
            "in_labor_force": [True, True, True, True, True, True, True, False, True, True],
        }
    )

    # create design
    design = SurveyDesign(df, weight_col="weight")

    # calculate basic indicator
    calc = IndicatorCalculator(design)
    result = calc.calculate_employment_rate()

    assert result is not None
    assert "overall" in result
    assert hasattr(result["overall"], "estimate")
    assert result["overall"].estimate.value >= 0
    assert result["overall"].estimate.value <= 100


def test_export_functions_exist():
    """Test that export functions are available."""
    from statskita import export_excel, export_parquet, export_stata

    assert callable(export_excel)
    assert callable(export_parquet)
    assert callable(export_stata)


def test_config_utils():
    """Test configuration utilities."""
    from statskita.utils.config_utils import deep_merge

    # test deep merge
    base = {"a": 1, "b": {"c": 2}}
    override = {"b": {"d": 3}, "e": 4}
    result = deep_merge(base, override)

    assert result["a"] == 1
    assert result["b"]["c"] == 2
    assert result["b"]["d"] == 3
    assert result["e"] == 4
