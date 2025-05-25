"""
Unzip the data in the `raw` directory
"""

import os
import typing
import zipfile
from datetime import datetime


RAW_DIR: str = "data/raw"
EXTRACTED_DIR: str = "data"
README: str = 'data/README.md'


def unzip_all_zips(directory: str,
                   log: typing.TextIO
                   ) -> None:
    """Get the zip files and unzip them"""
    for file in os.listdir(directory):
        if file.endswith(".zip"):
            zip_path = os.path.join(directory, file)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(EXTRACTED_DIR)
                log.write(f"> Extracted `{file}` to `{EXTRACTED_DIR}` \n")


if __name__ == "__main__":
    with open(README, 'a', encoding='utf-8') as LOG:
        LOG.write(f'\n### [{datetime.now()}]:  \n')
        LOG.write('Unzipping the raw files:  \n')
        os.makedirs(EXTRACTED_DIR, exist_ok=True)
        unzip_all_zips(RAW_DIR, LOG)
