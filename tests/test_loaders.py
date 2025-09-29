"""Minimal tests for data loaders."""

import polars as pl

from statskita.core.harmonizer import SurveyHarmonizer
from statskita.loaders.sakernas import SakernasLoader, load_sakernas


def test_loader_imports():
    """Test that loader modules can be imported."""
    assert callable(load_sakernas)
    assert SakernasLoader is not None
    assert SurveyHarmonizer is not None


def test_sakernas_loader_init():
    """Test SakernasLoader can be initialized."""
    loader = SakernasLoader()
    assert loader is not None
    # loader itself doesn't have dataset_name, it's in the metadata after loading


def test_sakernas_config():
    """Test that loader can load configuration."""
    loader = SakernasLoader()
    loader._load_config(wave="2025-02")

    config = loader.get_config()
    assert config is not None
    assert "fields" in config or "dataset" in config
    # config structure contains dataset info and fields


def test_harmonizer_init():
    """Test SurveyHarmonizer initialization."""
    harmonizer = SurveyHarmonizer("sakernas")
    assert harmonizer.dataset_type == "sakernas"
    assert harmonizer._rules is not None


def test_harmonizer_with_dummy_data():
    """Test harmonizer with minimal dummy data."""
    harmonizer = SurveyHarmonizer("sakernas")

    # minimal dummy data
    df = pl.DataFrame({
        "PROV": [11, 12],
        "B4K5": [25, 30]  # age field
    })

    # harmonize
    result, log = harmonizer.harmonize(df, "2025")
    assert result is not None
    # harmonizer preserves original columns
    assert "PROV" in result.columns
    # log should contain mapping info if any harmonization happened
