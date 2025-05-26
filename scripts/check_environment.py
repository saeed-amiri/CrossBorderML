"""
A script for cheking the enviroment before initiate any
actions.
"""

import importlib.metadata
from pathlib import Path
from typing import Callable, TextIO
from dataclasses import dataclass

ROOT: Path = Path(__file__).resolve().parents[1]
LOG: Path = ROOT / "env.log"


@dataclass
class StatusCounter:
    """Keep track of the number of warnings and errors"""
    # pylint: disable=missing-function-docstring
    warnings: int = 0
    errors: int = 0

    def log_warning(self,
                    log: TextIO,
                    message: str
                    ) -> None:
        self.warnings += 1
        log.write(f"[WARNNING] {message}\n")

    def log_error(self,
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


@dataclass
class CheckRule:
    """Defining the checks for each warn or error"""
    name: str
    path: Path
    check: Callable[[Path], bool]
    severity: str
    on_fail: str


def exists(path_i: Path) -> bool:
    # pylint: disable=missing-function-docstring
    return path_i.exists()


def not_empty(path_i: Path) -> bool:
    # pylint: disable=missing-function-docstring
    return path_i.exists() and any(path_i.iterdir())


def file_not_empty(path_i: Path) -> bool:
    # pylint: disable=missing-function-docstring
    return \
        path_i.exists() and path_i.is_file() and path_i.stat().st_size > 0


RULES: list[CheckRule] = [
    CheckRule("indicators.yaml",
              ROOT / "src/crossborderml/conf/indicators.yaml", file_not_empty,
              "error", "Make sure indicators are defined."),
    CheckRule("DATA directory", ROOT / "data", exists,
              "warning", "Run: `python prepare_data.py` to create it."),
    CheckRule("RAW directory", ROOT / "data/raw", exists,
              "warning", "Check your data download step."),
    CheckRule("PROCESSED directory", ROOT / "data/processed", exists,
              "warning", "Run: `python prepare_data.py`"),
    CheckRule("CONF directory", ROOT / "src/crossborderml/conf", exists,
              "error", "Check config installation or clone structure."),
    CheckRule("requirements.txt", ROOT / "requirements.txt", file_not_empty,
              "error", "This file is required to install dependencies."),
]


def run_checks(rules: list[CheckRule],
               log: TextIO,
               counter: StatusCounter
               ) -> None:
    # pylint: disable=missing-function-docstring
    for rule in rules:
        if rule.check(rule.path):
            counter.log_ok(log, f"{rule.name} exists and passed check.")
        else:
            msg = (f"{rule.name} missing or invalid at "
                   f"`{rule.path}`. {rule.on_fail}")
            if rule.severity == "error":
                counter.log_error(log, msg)
            else:
                counter.log_warning(log, msg)


def check_env_packages(req_path: Path,
                       counter: StatusCounter,
                       log: TextIO
                       ) -> None:
    """Check if the main packages are installed"""
    with req_path.open("r", encoding="utf-8") as f:
        requirements = [
            line.strip() for line in f
            if line.strip() and not line.startswith("#")
        ]

    log.write(f"\n# === Package Check from `{req_path}` === #\n")

    for req in requirements:
        pkg_name = req.split('==')[0].split('>=')[0].split('<=')[0].strip()
        try:
            version = importlib.metadata.version(pkg_name)
            log.write(f"[OK] `{pkg_name}` is installed (version {version}).\n")
        except importlib.metadata.PackageNotFoundError:
            counter.log_error(
                log,
                f"`{pkg_name}` is NOT installed. Run: `pip install {req}`")


if __name__ == '__main__':
    COUNTER = StatusCounter()
    with open(LOG, 'w', encoding='utf-8') as LOGGER:
        LOGGER.write("# === Environment Check LOG === #\n")
        run_checks(RULES, LOGGER, COUNTER)
        check_env_packages(ROOT / "requirements.txt", COUNTER, LOGGER)
        LOGGER.write("\n# === Summary === #\n")
        LOGGER.write(
            f"Warnings: {COUNTER.warnings}, Errors: {COUNTER.errors}\n")

    print(f"Environment check complete. See log at `{LOG}`.")
    if COUNTER.errors > 0:
        print(
            f"{COUNTER.errors} error(s) found. Please fix before proceeding.")
    elif COUNTER.warnings > 0:
        print(f"{COUNTER.warnings} warning(s) found. Review them if needed.")
    else:
        print("All checks passed cleanly.")
