from pathlib import Path

import pytest

from credito_pa.config import DEFAULTS, load_settings

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def settings(tmp_path, monkeypatch):
    for key in DEFAULTS:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("REPORTS_DIR", str(tmp_path / "reports"))
    monkeypatch.setenv("SOURCE_DB_URL", f"sqlite:///{(tmp_path / 'source.db').as_posix()}")
    monkeypatch.setenv("WAREHOUSE_DB_URL", f"sqlite:///{(tmp_path / 'warehouse.db').as_posix()}")
    monkeypatch.setenv("HTTP_BACKOFF_BASE_S", "0")
    return load_settings(env_file=tmp_path / "sem.env")


class FakeResponse:
    def __init__(self, status_code=200, json_data=None, text=None, content=None):
        self.status_code = status_code
        self._json = json_data
        self.text = text if text is not None else ("" if json_data is None else str(json_data))
        self.content = content if content is not None else self.text.encode("utf-8")

    def json(self):
        if self._json is None:
            raise ValueError("not json")
        return self._json

    def iter_content(self, chunk_size=1 << 16):
        for i in range(0, len(self.content), chunk_size):
            yield self.content[i:i + chunk_size]

    def close(self):
        pass


class FakeSession:
    def __init__(self, responses=None, by_url=None):
        self.responses = list(responses or [])
        self.by_url = by_url
        self.calls = []
        self.headers = {}

    def get(self, url, params=None, timeout=None, stream=False):
        self.calls.append(url)
        if self.by_url is not None:
            for key, resp in self.by_url.items():
                if url.endswith(key):
                    if isinstance(resp, list):
                        item = resp.pop(0) if len(resp) > 1 else resp[0]
                    else:
                        item = resp
                    if isinstance(item, Exception):
                        raise item
                    return item
            return FakeResponse(404, text="not found")
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


@pytest.fixture
def fake_response():
    return FakeResponse


@pytest.fixture
def fake_session():
    return FakeSession
