"""
Downloading data and updating README file
"""

import os
import typing
from datetime import datetime
import yaml
import requests


INDICATORS_PATH: str = 'src/crossborderml/conf/indicators.yaml'

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
    log.write(f"- Downloading '{name}' from:\n")
    log.write(f"> {url}  \n")
    response = requests.get(url, timeout=60)
    if response.status_code != 200:
        print(f"\nError! Failed to download '{name}'\n")

    zip_path = os.path.join(dest_dir, f"{name}.zip")
    with open(zip_path, "wb") as f:
        f.write(response.content)
    log.write(f"> Saved to {zip_path}\n")


def load_indicators(file_path: str) -> dict[str, str]:
    """Reading the YAML file"""
    with open(file_path, 'r', encoding='utf-8') as f:
        yaml_data = yaml.load(f, Loader=yaml.SafeLoader)
    return yaml_data['INDICATORS']


if __name__ == "__main__":
    os.makedirs("data/raw", exist_ok=True)
    with open(README, 'a', encoding='utf-8') as LOG:
        LOG.write(f'\n## [{datetime.now()}]:\n')
        LOG.write(f'Reading indicators file `{INDICATORS_PATH}`  \n')
        INDICATORS: dict[str, str] = load_indicators(INDICATORS_PATH)
        for NAME, CODE in INDICATORS.items():
            download_indicator(NAME, CODE, LOG)
