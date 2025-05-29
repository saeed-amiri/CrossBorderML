"""
Check all the expected indicators keys are exist in all the
csv files
"""

from os import listdir
from os.path import isfile, join
from pathlib import Path
import yaml

from crossborderml.config import CFG
from crossborderml.utils.io_utils import load_yaml


def _get_codes(dict_in: dict[str, str]) -> list[str]:
    """Get the codes which are the values of the dict"""
    return sorted(list(dict_in.values()))


def _mk_fname(codes: list[str], prefix: str ) -> list[str]:
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

def _check_list_are_same(reference: list[str], sample: list[str]) -> bool:
    """Check if every item"""
    not_in_b_method = list(set(reference).difference(set(sample)))
    print(not_in_b_method)
    return all(item in sample for item in reference)

name_code: dict[str, str] = load_yaml(CFG.paths.indicator_yaml, 'INDICATORS')
codes: list[str] = _get_codes(name_code)
fnames: list[str] = _mk_fname(codes, CFG.validd.file_prefix)
exsit_files: list[str] = _get_files(CFG.paths.extracted_data_dir,
                                    CFG.validd.file_prefix,
                                    CFG.validd.file_suffix,
                                    CFG.validd.file_extentions)
all_there = _check_list_are_same(fnames, exsit_files) 
