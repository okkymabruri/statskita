"""BPS API client for fetching poverty lines and other statistics.

Minimal implementation for v0.3.0 SUSENAS support.
Full features planned for v0.4.0.
"""

import os
from typing import Dict, Optional, Tuple

import requests


class BPSAPIClient:
    """Client for BPS Web API.

    Currently supports:
    - Province-specific poverty lines

    Full API features planned for v0.4.0.
    """

    BASE_URL = "https://webapi.bps.go.id/v1/api"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.environ.get('BPS_API_KEY')
        if not self.api_key:
            raise ValueError("BPS_API_KEY not found in environment or provided")

    def get_poverty_lines(
        self, year: int = 2024, period: str = "march"
    ) -> Dict[Tuple[str, str], float]:
        """Fetch province-specific poverty lines.

        Args:
            year: Year (2024, 2025, etc.)
            period: 'march' or 'september'

        Returns:
            Dict mapping (province_name, 'urban'|'rural') to poverty line (Rp/capita/month)

        Example:
            >>> client = BPSAPIClient()
            >>> lines = client.get_poverty_lines(2024, 'march')
            >>> lines[('DKI JAKARTA', 'urban')]
            825288
        """
        # map year to BPS year code
        year_code = year - 1900  # 2024 -> 124

        # var 195 = poverty line
        url = f"{self.BASE_URL}/list/model/data/lang/ind/domain/0000/var/195/th/{year_code}/key/{self.api_key}"

        response = requests.get(url, timeout=30)
        response.raise_for_status()

        data = response.json()

        # parse province names
        provinces = {p["val"]: p["label"] for p in data.get("vervar", [])}

        # parse datacontent
        # key format: [prov_code 4 digits][...][430=urban or 431=rural][...]
        poverty_lines = {}

        for key, value in data.get("datacontent", {}).items():
            # extract province code (first 4 digits)
            prov_code = int(key[:4])
            prov_name = provinces.get(prov_code)

            if not prov_name:
                continue

            # determine urban/rural from key
            if '430' in key:
                category = 'urban'
            elif '431' in key:
                category = 'rural'
            else:
                continue

            # store poverty line
            poverty_lines[(prov_name, category)] = float(value)

        return poverty_lines



def fetch_poverty_lines(year: int = 2024, period: str = "march") -> Dict[Tuple[str, str], float]:
    """Fetch poverty lines from BPS API.

    Args:
        year: Year
        period: 'march' or 'september'

    Returns:
        Dict of (province, urban/rural) -> poverty line

    Example:
        >>> lines = fetch_poverty_lines(2024, 'march')
        >>> lines[('INDONESIA', 'urban')]
        601871
    """
    client = BPSAPIClient()
    return client.get_poverty_lines(year, period)
