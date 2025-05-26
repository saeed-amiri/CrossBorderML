"""
A script for cheking the enviroment before initiate any
actions.
"""
import os
from pathlib import Path
from typing import Callable, TextIO
from dataclasses import dataclass


ROOT: Path = Path(__file__).resolve().parents[1]
LOG: Path = ROOT / "env.log"

WARN_NUMBER: int = 0
ERROR_NUMBER: int = 0

DIRECTORIES: dict[str, Path] = {
    'DATA': ROOT / "data",
    'RAW': ROOT / "daa" / "raw",
    'PROCESSED': ROOT / "dat" / "processed",
    'CONF': ROOT / "src" / "crossborderml" / "conf",
}

FILES: dict[str, Path] = {
    'REQUIREMENT': ROOT / 'requirements.txt',
    'INDICATORS': ROOT / "src" / "conf" / 'indicators.yaml',
    'PROCESSED': ROOT / "data" / "processed" / '*.csv',
}


@dataclass
class StatusCounter:
    """Keep track of the number of warnings and errors"""
    # pylint: disable=missing-function-docstring
    warning: int = 0
    errors: int = 0

    def log_warning(self,
                    log: TextIO,
                    message: str
                    ) -> None:
        self.warning += 1
        log.write(f"[WARNNING] {message}\n")

    def log_errors(self,
                   log: TextIO,
                   message: str,
                   ) -> None:
        self.errors += 1
        log.write(f"[ERROR] {message}\n")

    def log_ok(self,
               log: TextIO,
               message: str
               ) -> None:
        log.write(f"[OK] {message}\n")



def check_directories(dirs: dict[str, Path],
                      counter: StatusCounter,
                      log: TextIO
                      ) -> None:
    """Check the directories"""
    for _, path_i in dirs.items():
        check_dir(path_i, counter, log)


def check_dir(dir_path: Path,
              counter: StatusCounter,
              log: TextIO
              ) -> None:
    """Check the dir and write info into log"""
    if dir_path.exists():
        msg = (f"Directory: `{dir_path}` exist. contains:\n"
               f"\t{os.listdir(dir_path)}\n")
        counter.log_ok(log, message=msg)
    else:
        msg = f"Directory: `{dir_path}` Does NOT exist.\n"
        counter.log_warning(log, message=msg)


if __name__ == '__main__':
    COUNTER = StatusCounter()
    with open(LOG, 'w', encoding='utf-8') as LOGGER:
        LOGGER.write("# === Environment Check Log === #\n")
        check_directories(DIRECTORIES, COUNTER, log=LOGGER)
