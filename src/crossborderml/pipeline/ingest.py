"""
Download data from the sources and unzip them if needed
"""

import os
import typing
from pathlib import Path
from datetime import datetime
import zipfile
import requests
import yaml


PROJECT_ROOT: Path = Path(__file__).resolve().parents[3]
DEFAULT_DATA_DIR: Path = PROJECT_ROOT / "data"
DEFAULT_INDICATORS_PATH: Path = \
    PROJECT_ROOT / "src/crossborderml/conf/indicators.yaml"
EXTRACTED_DIR = PROJECT_ROOT / "data"
DEFAULT_README: Path = DEFAULT_DATA_DIR / "README.md"
RAW_DIR = PROJECT_ROOT / "data" / "raw"


def _download_indicator(name: str,
                        code: str,
                        log: typing.TextIO,
                        dest_dir: Path
                        ) -> None:
    url = \
        f"https://api.worldbank.org/v2/en/indicator/{code}?downloadformat=csv"
    log.write(f"- Downloading '{name}' from:\n")
    log.write(f"> {url}  \n")
    response = requests.get(url, timeout=60)
    if response.status_code != 200:
        print(f"\nError! Failed to download '{name}'\n")

    dest_dir.mkdir(parents=True, exist_ok=True)
    zip_path = dest_dir / f"{name}.zip"
    with open(zip_path, "wb") as f:
        f.write(response.content)
    log.write(f"> Saved to {zip_path}\n")


def _load_indicators(indicators_path: Path = DEFAULT_INDICATORS_PATH
                     ) -> dict[str, str]:
    with open(indicators_path, 'r', encoding='utf-8') as f:
        yaml_data = yaml.load(f, Loader=yaml.SafeLoader)
    return yaml_data['INDICATORS']


def run_download(indicators_path: Path = DEFAULT_INDICATORS_PATH,
                 dest_dir: Path = RAW_DIR,
                 readme_path: Path = DEFAULT_README
                 ) -> None:
    """Download the files selected in indicators"""
    indicators = _load_indicators(indicators_path)

    with open(readme_path, 'a', encoding='utf-8') as log:
        log.write(f'\n## [{datetime.now()}]:\n')
        log.write(f'Reading indicators file `{indicators_path}`  \n')
        for name, code in indicators.items():
            _download_indicator(name, code, log, dest_dir)


def _unzip_all_zips(directory: Path,
                    log: typing.TextIO
                    ) -> None:
    """Get the zip files and unzip them"""
    for file in os.listdir(directory):
        if file.endswith(".zip"):
            zip_path = os.path.join(directory, file)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(EXTRACTED_DIR)
                log.write(f"> Extracted `{file}` to `{EXTRACTED_DIR}`  \n")


def run_unzip() -> None:
    """Unzip all the files"""
    with open(DEFAULT_README, 'a', encoding='utf-8') as log:
        log.write(f'\n### [{datetime.now()}]:  \n')
        log.write('Unzipping the raw files:  \n')
        os.makedirs(EXTRACTED_DIR, exist_ok=True)
        _unzip_all_zips(RAW_DIR, log)


if __name__ == '__main__':
    run_download()
    run_unzip()
