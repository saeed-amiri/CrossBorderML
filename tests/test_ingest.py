"""tests for ingest in pipline
mostly by ChatGPT, but all corrected!
"""
# pylint: disable=too-few-public-methods
# pylint: disable=missing-function-docstring
# pylint: disable=missing-class-docstring

import io
import os
from pathlib import Path
import requests

from crossborderml.pipeline.ingest import IndicatorDownloader
from crossborderml.config import CFG


class DummyResponse:
    def __init__(self, content: bytes, status_code: int = 200):
        self.content = content
        self.status_code = status_code

    def raise_for_status(self):
        if not 200 <= self.status_code < 300:
            raise requests.HTTPError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self, response: DummyResponse):
        self._response = response

    def get(self, url, timeout):
        # verify that timeout is coming from CFG
        assert timeout == CFG.urls.timeout
        # simple sanity on URL
        assert "{code}" not in url
        return self._response


def test_save_zip_success(tmp_path: Path):
    log = io.StringIO()
    dl = IndicatorDownloader(dest_dir=tmp_path, logger=log)

    ok = dl.save_zip("foo", b"hello")
    assert ok is True

    # file was written
    written = (tmp_path / "foo.zip").read_bytes()
    assert written == b"hello"

    # log message contains the right name and path
    assert "Saved 'foo' to" in log.getvalue()


def test_save_zip_permission_error(tmp_path: Path):
    # create a directory and make it read-only
    ro = tmp_path / "ro_dir"
    ro.mkdir()
    os.chmod(ro, 0o400)

    log = io.StringIO()
    dl = IndicatorDownloader(dest_dir=ro, logger=log)

    ok = dl.save_zip("bar", b"data")
    assert ok is False
    assert "permission denied" in log.getvalue().lower()


def test_download_success(tmp_path: Path):
    # stub out HTTP response
    resp = DummyResponse(b"zipcontent", status_code=200)
    fake_session = FakeSession(resp)

    log = io.StringIO()
    dl = IndicatorDownloader(
        dest_dir=tmp_path, logger=log, session=fake_session)

    ok = dl.download("GDP", "NY.GDP.MKTP.CD")
    assert ok is True

    # file got written
    assert (tmp_path / "GDP.zip").read_bytes() == b"zipcontent"
    # logs contain both download and saved messages
    logs = log.getvalue()
    assert "Downloading 'GDP' from" in logs
    assert "Saved 'GDP' to" in logs


def test_download_timeout(tmp_path: Path):
    # simulate a timeout exception
    class TimeoutSession:
        def get(self, url, timeout):
            raise requests.Timeout("timed out")

    log = io.StringIO()
    dl = IndicatorDownloader(
        dest_dir=tmp_path, logger=log, session=TimeoutSession())

    ok = dl.download("TMP", "TEST.CODE")
    assert ok is False
    assert "timed out" in log.getvalue().lower()
