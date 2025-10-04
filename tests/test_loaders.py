"""Minimal tests for data loaders."""

from unittest.mock import Mock, patch

import polars as pl

from statskita.core.harmonizer import SurveyHarmonizer
from statskita.loaders.bps_api import BPSAPIClient
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


def test_harmonizer_init():
    """Test SurveyHarmonizer initialization."""
    harmonizer = SurveyHarmonizer("sakernas")
    assert harmonizer.dataset_type == "sakernas"
    assert harmonizer._rules is not None


def test_harmonizer_with_dummy_data():
    """Test harmonizer with minimal dummy data."""
    harmonizer = SurveyHarmonizer("sakernas")

    # minimal dummy data
    df = pl.DataFrame(
        {
            "PROV": [11, 12],
            "B4K5": [25, 30],  # age field
        }
    )

    # harmonize
    result, log = harmonizer.harmonize(df, "2025")
    assert result is not None
    # harmonizer preserves original columns
    assert "PROV" in result.columns
    # log should contain mapping info if any harmonization happened


def test_bps_api_client_parses_poverty_lines():
    """Ensure BPS API client parses province urban/rural lines and filters period."""

    sample_json = {
        "vervar": [{"val": 1100, "label": "ACEH"}],
        "datacontent": {
            "110019543012461": 704200,  # urban, period 61
            "110019543112461": 645000,  # rural, period 61
            "110019543012462": 700000,  # period 62 (should be ignored)
        },
    }

    with patch("statskita.loaders.bps_api.requests.get") as mock_get:
        mock_resp = Mock()
        mock_resp.json.return_value = sample_json
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        client = BPSAPIClient(api_key="dummy")
        lines = client.get_poverty_lines(2024, "march")

        expected_url = "https://webapi.bps.go.id/v1/api/list/model/data/lang/ind/domain/0000/var/195/th/124/key/dummy"
        mock_get.assert_called_once_with(expected_url, params={"tur": 61}, timeout=30, verify=False)

        assert lines[("ACEH", "urban")] == 704200.0
        assert lines[("ACEH", "rural")] == 645000.0
        # ensure period 62 entry was dropped
        assert len(lines) == 2
