"""
Check all the expected indicators keys are exist in all the
csv files
"""

from typing import TextIO
from pathlib import Path
import zipfile
import requests

from crossborderml.config import CFG
from crossborderml.utils.io_utils import load_yaml
from crossborderml.utils.io_utils import FileFinder
from crossborderml.pipeline.downloader import IndicatorDownloader


class IndicatorSpec:
    """Get desired files names"""
    # pylint: disable=too-few-public-methods
    def __init__(self, yaml_path: Path) -> None:
        self.names_to_codes: dict[str, str] = \
            load_yaml(yaml_path, "INDICATORS")
        self.expected: list[str] = sorted(self.names_to_codes.values())


class MissingFinder:
    """Finding the missing data"""
    # pylint: disable=too-few-public-methods
    def __init__(self, expected: set[str], existing: set[str]) -> None:
        self.expected = expected
        self.existing = existing

    def missing(self, named_codes: dict[str, str]) -> dict[str, str]:
        """Self explanatory"""
        return self._get_missing_dict(
            self.expected - self.existing,
            named_codes)

    def _get_missing_dict(
            self,
            missed_set: set[str],
            named_codes: dict[str, str],
            ) -> dict[str, str]:
        codes_names = {v: k for k, v in named_codes.items()}
        return {k: v for v, k in codes_names.items() if v in missed_set}


class RecoveryRunner:
    """Try to download and unzip missed files"""
    def __init__(
            self,
            missing: dict[str, str],
            names_to_codes: dict[str, str],
            raw_dir: Path,
            extract_dir: Path,
            ) -> None:
        self.missing = missing
        self.names_to_codes = names_to_codes
        self.raw_dir = raw_dir
        self.extract_dir = extract_dir

    def recover(self, readme: Path) -> None:
        """Self explanatory"""
        with open(readme, "a", encoding='utf-8') as log:
            log.write(f"Missing: {self.missing}")
            dl = IndicatorDownloader(self.raw_dir, logger=log)
            for name, code in self.missing.items():
                try:
                    dl.download(name, code)
                    file_path = self.raw_dir / (name + '.zip')

                    if file_path and str(file_path).endswith('.zip'):
                        self.unzip(name, log)
                except (requests.exceptions.RequestException, OSError) as exc:
                    log.write(
                        f"Error downloading or unzipping '{name}': {exc}\n")

    def unzip(self, name: str, log: TextIO) -> None:
        """Self explanatory"""
        zip_path = self.raw_dir / f"{name}.zip"
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(self.extract_dir)
            log.write(f"Unzipped {name}")
        except (zipfile.BadZipFile, zipfile.BadZipFile):
            log.write(f"Corrupt zip: {zip_path}")


def main():
    """Self explanatory"""

    spec = IndicatorSpec(CFG.paths.indicator_yaml)
    lister = FileFinder(
        directory=CFG.paths.extracted_data_dir,
        prefix=CFG.validd.file_prefix,
        extension=CFG.validd.file_extentions
        )
    existing = lister.get_file_names(
        CFG.validd.file_prefix,
        CFG.validd.file_suffix)
    finder = MissingFinder(set(spec.expected), existing)
    missing = finder.missing(spec.names_to_codes)

    if missing:
        runner = RecoveryRunner(
            missing,
            spec.names_to_codes,
            CFG.paths.raw_data_dir,
            CFG.paths.extracted_data_dir)
        runner.recover(CFG.paths.data_readme)


if __name__ == "__main__":
    main()
