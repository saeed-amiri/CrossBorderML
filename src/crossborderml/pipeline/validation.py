"""
Check all the expected indicators keys are exist in all the
csv files
"""

from pathlib import Path
import yaml

from crossborderml.config import CFG
from crossborderml.utils.io_utils import load_yaml


def check_files() -> None:
    """Check if all the files mentioned in „indicators.yaml“
    exist.
    Since the indicator already checked before
    """
    indicators = load_yaml(CFG.paths.indicator_yaml, 'INDICATORS')