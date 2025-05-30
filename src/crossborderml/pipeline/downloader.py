"""
Download the resuested file and save it as a zip file
"""


from typing import TextIO, Callable
from pathlib import Path
import requests

from crossborderml.config import CFG


class IndicatorDownloader:
    """Manage downloads and saving them"""

    def __init__(
            self,
            dest_dir: Path,
            logger: TextIO,
            session: requests.Session | None = None,
            ) -> None:
        self.dest_dir = dest_dir
        self.logger = logger
        self.session = session or requests.Session()

    def download(
            self,
            name: str,
            code: str,
            url_builder: Callable[[str], str] =
            CFG.urls.world_bank_indicator_url,
            timeout: float = CFG.urls.timeout,
            ) -> bool:
        """Attempt to download"""
        url = url_builder(code)
        self.logger.write(f"\nDownloading '{name}' from:  \n{url}  \n")
        try:
            resp = self.session.get(url, timeout=timeout)
            resp.raise_for_status()
        except requests.RequestException as exc:
            self.logger.write(f"Fetch failed for {name}:\n {exc}")
            return False

        return self.save_zip(name, resp.content)

    def save_zip(
            self,
            name: str,
            data: bytes
            ) -> bool:
        """Save the files to the destination path"""
        try:
            self.dest_dir.mkdir(parents=True, exist_ok=True)
            path = self.dest_dir / f"{name}.zip"
            path.write_bytes(data)
            self.logger.write(f"Saved '{name}' to {path}  \n")
            return True
        except PermissionError:
            self.logger.write(f"Permission denied saving {name}")
        except FileNotFoundError:
            self.logger.write(f"Invalid path for {name}")
        except OSError as exc:
            self.logger.write(f"OS error saving {name}: {exc}")
        return False
