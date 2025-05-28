"""
Download data from the sources and unzip them if needed
"""

import typing
from pathlib import Path
from datetime import datetime
import zipfile
import requests
import yaml

from crossborderml.config import CFG


def _download_indicator(name: str,
                        code: str,
                        log: typing.TextIO,
                        dest_dir: Path
                        ) -> bool:
    url = \
        f"https://api.worldbank.org/v2/en/indicator/{code}?downloadformat=csv"
    log.write(f"- Downloading '{name}' from:\n")
    log.write(f"> {url}  \n")

    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
    except requests.exceptions.RequestException as exc:
        log.write(f"Failed on fetch `{name}`:  \n{exc}  ")

    try:
        dest_dir.mkdir(parents=True, exist_ok=True)
        zip_path = dest_dir / f"{name}.zip"
        with open(zip_path, "wb") as f:
            f.write(response.content)
        log.write(f"> Saved to {zip_path}\n")
        return True
    except (OSError, IOError) as exc:
        log.write(f"Failed to save: `{name}`:  \n{exc}  ")
        return False


def _load_indicators(indicators_path: Path = CFG.paths.indicator_yaml
                     ) -> dict[str, str]:
    """
    Load indicator names and codes from a YAML config file.

    Returns:
        A dict of {name: code}

    Raises:
        FileNotFoundError: if the file does not exist
        yaml.YAMLError: if the YAML is invalid
        KeyError: if 'INDICATORS' section is missing
    """
    try:
        with open(indicators_path, 'r', encoding='utf-8') as f:
            yaml_data = yaml.load(f, Loader=yaml.SafeLoader)
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"Indicator file not found: {indicators_path}"
            ) from exc
    except PermissionError as exc:
        raise PermissionError(
            f"Permission denied when reading: {indicators_path}"
            ) from exc
    except yaml.YAMLError as exc:
        raise yaml.YAMLError(
            f"Invalid YAML in file: {indicators_path}\nError: {exc}"
            ) from exc

    if 'INDICATORS' not in yaml_data or \
       not isinstance(yaml_data['INDICATORS'], dict):
        raise KeyError(
            f"'INDICATORS' section missing or not a dict in: {indicators_path}"
            )

    return yaml_data['INDICATORS']


def run_download(indicators_path: Path = CFG.paths.indicator_yaml,
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
            indicators = _load_indicators(indicators_path)
        except (FileNotFoundError, PermissionError,
                yaml.YAMLError, KeyError) as exc:
            log.write(f"Failed to load indicators: {exc}\n")
            return

        for name, code in indicators.items():
            try:
                ok = _download_indicator(name, code, log, dest_dir)
                if ok:
                    success_count += 1
                else:
                    failure_count += 1
            except (requests.exceptions.RequestException, OSError) as exc:
                log.write(f"Error downloading '{name}': {exc}\n")
                failure_count += 1

        log.write(f"\nDownload complete. {success_count} succeeded, "
                  f"{failure_count} failed.\n")


def _unzip_all_zips(zip_dir: Path,
                    log: typing.TextIO,
                    extract_dir: Path = CFG.paths.data_dir
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
                log.write(f"Extracted `{zip_path.name}` to `{extract_dir}`\n")
        except zipfile.BadZipFile:
            log.write(f"Bad zip file: {zip_path}\n")
        except PermissionError:
            log.write(f"Permission denied extracting: {zip_path}\n")
        except (OSError, IOError) as exc:
            log.write(f"File error with `{zip_path}`: {exc}\n")


def run_unzip(zip_dir: Path = CFG.paths.raw_data_dir,
              extract_dir: Path = CFG.paths.data_dir,
              log_path: Path = CFG.paths.data_readme
              ) -> None:
    """Unzip all the files"""
    with open(log_path, 'a', encoding='utf-8') as log:
        log.write(f'\n## [{datetime.now()}]:\n')
        log.write(f'Unzipping files from `{zip_dir}`\n')
        _unzip_all_zips(zip_dir, log, extract_dir)


if __name__ == '__main__':
    run_download()
    run_unzip()
