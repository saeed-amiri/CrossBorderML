"""
Check all the expected indicators keys are exist in all the
csv files
"""

from os import listdir
from os.path import isfile, join
from pathlib import Path
import zipfile
import requests

from crossborderml.config import CFG
from crossborderml.utils.io_utils import load_yaml
from crossborderml.pipeline.downloader import IndicatorDownloader


def _get_codes(dict_in: dict[str, str]) -> list[str]:
    """Get the codes which are the values of the dict"""
    return sorted(list(dict_in.values()))


def _mk_fname(codes: list[str], prefix: str) -> list[str]:
    """Make the file name based on the World Bank convention"""
    return [prefix + item for item in codes]


def _get_files(
        data_path: Path,
        prefix: str,
        suffix: str,
        extension: str
        ) -> list[str]:
    """Get the files that are alreadx downloaded"""
    onlyfiles = [f for f in listdir(data_path) if isfile(join(data_path, f))]
    onlyfiles = [f for f in onlyfiles if f.strip().startswith(prefix)]
    onlyfiles = [f for f in onlyfiles if f.endswith(extension)]
    onlyfiles = [f.split(suffix)[0] for f in onlyfiles]
    return sorted(onlyfiles)


class CheckFilesExists:
    """Check if the all files requested in the indictor file
    are downloaded, if not try to get them.
    """

    def __init__(
            self,
            reference: list[str],
            sample: list[str],
            names_codes: dict[str, str],
            ) -> None:
        self.reference = reference
        self.sample = sample
        self.names_codes = names_codes

    def check_list_are_same(
            self,
            raw_data_dir: Path,
            extracted_dir: Path,
            readme: Path,
            prefix: str
            ) -> None:
        """Check if every item is correctly downloaded and
        extracted"""
        if not all(item in self.sample for item in self.reference):
            print('Some files are missing!\n')
            self.get_missing(raw_data_dir, extracted_dir, readme, prefix)

    def get_missing(
            self,
            raw_data_dir: Path,
            extracted_dir: Path,
            readme: Path,
            prefix: str
            ) -> None:
        """Try to get the missing files"""
        absent_from_sample = \
            list(set(self.reference).difference(set(self.sample)))
        clean_absent: list[str] = self._drop_prefix(absent_from_sample, prefix)
        absents_files: dict[str, str] = self._get_names(clean_absent)
        self.download(absents_files, raw_data_dir, extracted_dir, readme)

    @staticmethod
    def _drop_prefix(in_list: list[str], prefix: str) -> list[str]:
        return [item.removeprefix(prefix) for item in in_list]

    def _get_names(self, absents: list[str]) -> dict[str, str]:
        """Return a dict of the names and codes"""
        codes_names = {v: k for k, v in self.names_codes.items()}
        return {k: v for v, k in codes_names.items() if v in absents}

    def download(
            self,
            absent_files: dict[str, str],
            raw_data_dir: Path,
            extracted_dir: Path,
            readme: Path,
            ) -> None:
        """Downloading the missed files and unzip them"""
        with open(readme, 'a', encoding='utf-8') as log:
            log.write('\n### Downloading missing files:\n')
            log.write(f'Missed files `{absent_files}`  \n')
            dl = IndicatorDownloader(dest_dir=raw_data_dir, logger=log)
            for name, code in absent_files.items():
                try:
                    dl.download(name, code)
                    file_path = raw_data_dir / (name + '.zip')

                    if file_path and str(file_path).endswith('.zip'):

                        with zipfile.ZipFile(str(file_path), 'r') as zip_ref:
                            zip_ref.extractall(extracted_dir)
                        print(f'{name} is unzipped to {extracted_dir}!')
                except (requests.exceptions.RequestException,
                        OSError, zipfile.BadZipFile) as exc:
                    log.write(
                        f"Error downloading or unzipping '{name}': {exc}\n")


name_code: dict[str, str] = load_yaml(CFG.paths.indicator_yaml, 'INDICATORS')
wb_codes: list[str] = _get_codes(name_code)
fnames: list[str] = _mk_fname(wb_codes, CFG.validd.file_prefix)

exsit_files: list[str] = _get_files(
    CFG.paths.extracted_data_dir,
    CFG.validd.file_prefix,
    CFG.validd.file_suffix,
    CFG.validd.file_extentions)

check_files = CheckFilesExists(fnames, exsit_files, name_code,)
check_files.check_list_are_same(
    CFG.paths.raw_data_dir,
    CFG.paths.extracted_data_dir,
    CFG.paths.data_readme,
    CFG.validd.file_prefix)
