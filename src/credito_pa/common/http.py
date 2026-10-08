from __future__ import annotations

import random
import time
from collections.abc import Callable
from dataclasses import dataclass

import requests

from credito_pa.common.logging import get_logger
from credito_pa.config import Settings

log = get_logger("http")

RETRYABLE_STATUS = {429, 500, 502, 503, 504}
USER_AGENT = "credito-pa-pipeline/0.1 (projeto academico; dados abertos)"


class HttpRequestError(Exception):
    def __init__(self, url: str, reason: str, status: int | None = None, detail: str = ""):
        self.url = url
        self.reason = reason
        self.status = status
        self.detail = detail
        super().__init__(f"{reason} (status={status}) em {url}: {detail}")


@dataclass
class HttpClient:
    timeout_s: float = 120.0
    max_retries: int = 5
    backoff_base_s: float = 1.0
    backoff_max_s: float = 60.0
    session: requests.Session | None = None
    sleep: Callable[[float], None] = time.sleep
    rng: random.Random | None = None

    def __post_init__(self):
        if self.session is None:
            self.session = requests.Session()
            self.session.headers["User-Agent"] = USER_AGENT
        if self.rng is None:
            self.rng = random.Random(0)
        self.last_attempts = 0

    @classmethod
    def from_settings(cls, settings: Settings, **kwargs) -> "HttpClient":
        return cls(
            timeout_s=float(settings.get("HTTP_TIMEOUT_S")),
            max_retries=int(settings.get("HTTP_MAX_RETRIES")),
            backoff_base_s=float(settings.get("HTTP_BACKOFF_BASE_S")),
            backoff_max_s=float(settings.get("HTTP_BACKOFF_MAX_S")),
            **kwargs,
        )

    def backoff(self, attempt: int) -> float:
        delay = min(self.backoff_max_s, self.backoff_base_s * (2 ** (attempt - 1)))
        return delay + self.rng.uniform(0, delay * 0.1)

    def get(self, url: str, *, params: dict | None = None, stream: bool = False) -> requests.Response:
        attempt = 0
        while True:
            attempt += 1
            self.last_attempts = attempt
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout_s, stream=stream)
            except (requests.Timeout, requests.ConnectionError) as exc:
                if attempt > self.max_retries:
                    raise HttpRequestError(url, "http_error", None, f"{type(exc).__name__}: {exc}") from exc
                wait = self.backoff(attempt)
                log.warning("Tentativa %d falhou (%s). Nova tentativa em %.1fs: %s", attempt, type(exc).__name__, wait, url)
                self.sleep(wait)
                continue

            if resp.status_code < 400:
                if attempt > 1:
                    log.info("Sucesso após %d tentativas: %s", attempt, url)
                return resp
            if resp.status_code in RETRYABLE_STATUS and attempt <= self.max_retries:
                wait = self.backoff(attempt)
                log.warning("HTTP %d na tentativa %d. Nova tentativa em %.1fs: %s", resp.status_code, attempt, wait, url)
                self.sleep(wait)
                continue
            reason = "not_found" if resp.status_code == 404 else "http_error"
            raise HttpRequestError(url, reason, resp.status_code, resp.text[:200])

    def get_json(self, url: str, *, params: dict | None = None):
        resp = self.get(url, params=params)
        try:
            return resp.json()
        except ValueError as exc:
            raise HttpRequestError(url, "invalid_json", resp.status_code, resp.text[:200]) from exc
