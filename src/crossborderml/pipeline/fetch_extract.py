"""
Download data from the sources and unzip them if needed
"""

from typing import TextIO
from pathlib import Path
from datetime import datetime
import zipfile
import requests
import yaml

from crossborderml.config import CFG
from crossborderml.utils.io_utils import load_yaml
from crossborderml.pipeline.downloader import IndicatorDownloader


def run_download(
        indicators_path: Path = CFG.paths.indicator_yaml,
        dest_dir: Path = CFG.paths.raw_data_dir,
        readme_path: Path = CFG.paths.data_readme
        ) -> None:
    """Download the files selected in indicators.yaml and log results."""

    success_count: int = 0
    failure_count: int = 0

    with open(readme_path, 'a', encoding='utf-8') as log:
        log.write(f'\n## [{datetime.now()}]:\n')
        log.write(f'Reading indicators file `{indicators_path}`  \n')

        try:
            indicators = load_yaml(indicators_path, 'INDICATORS')
        except (FileNotFoundError, PermissionError,
                yaml.YAMLError, KeyError) as exc:
            log.write(f"Failed to load indicators: {exc}\n")
            return
        dl = IndicatorDownloader(dest_dir=dest_dir, logger=log)
        for name, code in indicators.items():
            try:
                ok = dl.download(name, code)
                if ok:
                    success_count += 1
                else:
                    failure_count += 1
            except (requests.exceptions.RequestException, OSError) as exc:
                log.write(f"Error downloading '{name}': {exc}\n")
                failure_count += 1

        log.write(f"\nDownload complete. {success_count} succeeded, "
                  f"{failure_count} failed.\n")


def _unzip_all_zips(
        zip_dir: Path,
        log: TextIO,
        extract_dir: Path
        ) -> None:
    """
    Unzips all .zip files in the given directory to `extract_dir`.

    Args:
        zip_dir: Path to the directory containing .zip files.
        extract_dir: Destination directory for extracted content.
        log: File-like object for logging progress.
    """
    try:
        zip_files = list(zip_dir.glob("*.zip"))
    except (OSError, PermissionError) as exc:
        log.write(f"Failed to access zip directory {zip_dir}: {exc}\n")
        return

    if not zip_files:
        log.write(f"No zip files found in {zip_dir}\n")
        return

    extract_dir.mkdir(parents=True, exist_ok=True)

    for zip_path in zip_files:
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
        except zipfile.BadZipFile:
            log.write(f"Bad zip file: {zip_path}\n")
        except PermissionError:
            log.write(f"Permission denied extracting: {zip_path}\n")
        except OSError as exc:
            log.write(f"File error with `{zip_path}`: {exc}\n")
        else:
            log.write(
                f"Extracted `{zip_path.name}` to `{extract_dir}`  \n")


def run_unzip(
        zip_dir: Path = CFG.paths.raw_data_dir,
        extract_dir: Path = CFG.paths.extracted_data_dir,
        log_path: Path = CFG.paths.data_readme
        ) -> None:
    """Unzip all the files"""
    with open(log_path, 'a', encoding='utf-8') as log:
        log.write(f'\n## [{datetime.now()}]:  \n')
        log.write(f'Unzipping files from `{zip_dir}`  \n')
        _unzip_all_zips(zip_dir, log, extract_dir)


if __name__ == '__main__':
    run_download()
    run_unzip()
