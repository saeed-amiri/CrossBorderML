"""
Downloading data and updating README file
"""

import os
import typing
from datetime import datetime

import requests


INDICATORS = {
    "gdp": "NY.GDP.MKTP.CD",
    "fdi": "BX.KLT.DINV.CD.WD",
    "population": "SP.POP.TOTL",
}

BASE_URL = \
    "https://api.worldbank.org/v2/en/indicator/{code}?downloadformat=csv"

README: str = "data/README.md"


def download_indicator(name: str,
                       code: str,
                       log: typing.TextIO,
                       dest_dir: str = "data/raw"
                       ) -> None:
    """Download the data"""
    url = BASE_URL.format(code=code)
    log.write(f"- Downloading '{name}' from {url}\n")
    response = requests.get(url, timeout=60)
    if response.status_code != 200:
        print(f"+ Failed to download '{name}'", )

    zip_path = os.path.join(dest_dir, f"{name}.zip")
    with open(zip_path, "wb") as f:
        f.write(response.content)
    log.write(f"> Saved to {zip_path}\n")


if __name__ == "__main__":
    os.makedirs("data/raw", exist_ok=True)
    with open(README, 'a', encoding='utf-8') as LOG:
        LOG.write(f'\n## [{datetime.now()}]:\n')
        for NAME, CODE in INDICATORS.items():
            download_indicator(NAME, CODE, LOG)
