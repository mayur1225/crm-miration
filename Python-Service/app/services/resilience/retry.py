import asyncio
import contextvars
import logging
import random
import re
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Iterator, Optional
from urllib.parse import urlparse

import httpx

logger = logging.getLogger(__name__)

OUTCOME_UNKNOWN = "OUTCOME_UNKNOWN: "


class ApiQuotaExhausted(Exception):
    """Daily/hard API quota reached. Retrying now is pointless; the run must be checkpointed and resumed later."""


class CircuitOpen(Exception):
    """A CRM host kept failing after repeated cool-downs; the run must be checkpointed and resumed later."""


_idempotent_post: contextvars.ContextVar[bool] = contextvars.ContextVar("idempotent_post", default=False)


@contextmanager
def idempotent_posts(enabled: bool = True) -> Iterator[None]:
    """
    Within this block POSTs may be retried after ambiguous failures (read timeout, 502, 504).
    Enable only for upsert/delete traffic; never for plain inserts, and never for UPDATE mode (it is sent as an upsert
    and the CRM-created record of an unmatched row is reverted afterwards: a replayed POST would hide that record).
    """
    token = _idempotent_post.set(enabled)
    try:
        yield
    finally:
        _idempotent_post.reset(token)


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 5
    base_delay: float = 1.0
    factor: float = 2.0
    max_wait: float = 60.0
    max_total: float = 300.0

    def backoff(self, attempt: int) -> float:
        """Full jitter: uniform(0, min(cap, base * factor^(attempt-1)))."""
        return random.uniform(0.0, min(self.max_wait, self.base_delay * self.factor ** (attempt - 1)))


class CircuitBreaker:
    """
    Per-host. After `threshold` consecutive transient failures the circuit opens and callers PAUSE for
    `cooldown` seconds (the run is paused, not failed). After `max_trips` consecutive trips without a
    success, CircuitOpen is raised so the orchestrator can mark the run interrupted.
    """

    def __init__(self, threshold: int = 8, cooldown: float = 30.0, max_trips: int = 3):
        self.threshold = threshold
        self.cooldown = cooldown
        self.max_trips = max_trips
        self._failures: Dict[str, int] = {}
        self._trips: Dict[str, int] = {}
        self._open_until: Dict[str, float] = {}

    async def before(self, host: str) -> None:
        remaining = self._open_until.get(host, 0.0) - time.monotonic()
        if remaining > 0:
            logger.warning("[RETRY] Circuit open for %s; pausing %.0fs", host, remaining)
            await asyncio.sleep(remaining)

    def success(self, host: str) -> None:
        self._failures[host] = 0
        self._trips[host] = 0

    def failure(self, host: str) -> None:
        self._failures[host] = self._failures.get(host, 0) + 1
        if self._failures[host] < self.threshold:
            return
        self._failures[host] = 0
        self._trips[host] = self._trips.get(host, 0) + 1
        if self._trips[host] >= self.max_trips:
            raise CircuitOpen(f"{host} failed repeatedly; giving up for now.")
        self._open_until[host] = time.monotonic() + self.cooldown


_PRE_SEND_ERRORS = (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout)
_AMBIGUOUS_ERRORS = (
    httpx.ReadTimeout, httpx.ReadError, httpx.WriteError, httpx.WriteTimeout,
    httpx.RemoteProtocolError, httpx.CloseError,
)
_ALWAYS_SAFE_METHODS = {"GET", "HEAD", "OPTIONS", "PUT", "DELETE"}
_QUOTA_BODY_RE = re.compile(r"REQUEST_LIMIT_EXCEEDED|ApiLimitExceeded|ExceededQuota|TotalRequests Limit exceeded", re.I)


def _retry_after_seconds(res: httpx.Response) -> Optional[float]:
    raw = res.headers.get("Retry-After")
    if not raw:
        return None
    try:
        return max(0.0, float(raw))
    except ValueError:
        return None


def _is_quota_exhausted(res: httpx.Response) -> bool:
    if res.status_code in (400, 403, 429) and _QUOTA_BODY_RE.search(res.text or ""):
        return True
    if res.status_code == 429 and "hubapi.com" in str(res.url):
        try:
            return (res.json() or {}).get("policyName") == "DAILY"
        except ValueError:
            return False
    return False


class RetryingClient:
    """
    Drop-in wrapper for httpx.AsyncClient. Migrators keep calling client.get/post/put/delete unchanged;
    every call now gets backoff, Retry-After handling and a circuit breaker.

    401 is deliberately NOT retried here: migrators already refresh the token and replay the call.
    Exhausted retryable HTTP statuses return the last response so existing status checks still apply.
    """

    def __init__(self, client: httpx.AsyncClient, policy: Optional[RetryPolicy] = None,
                 breaker: Optional[CircuitBreaker] = None):
        self._client = client
        self.policy = policy or RetryPolicy()
        self.breaker = breaker or CircuitBreaker()

    async def __aenter__(self) -> "RetryingClient":
        await self._client.__aenter__()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self._client.__aexit__(*exc)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)

    async def request(self, method: str, url: Any, **kwargs: Any) -> httpx.Response:
        method_u = method.upper()
        replay_safe = method_u in _ALWAYS_SAFE_METHODS or _idempotent_post.get()
        host = urlparse(str(url)).netloc or "unknown"
        started = time.monotonic()
        attempt = 0

        while True:
            attempt += 1
            await self.breaker.before(host)
            try:
                res = await self._client.request(method_u, url, **kwargs)
            except _PRE_SEND_ERRORS as exc:
                self.breaker.failure(host)
                last_error: Any = exc
            except _AMBIGUOUS_ERRORS as exc:
                self.breaker.failure(host)
                if not replay_safe:
                    raise
                last_error = exc
            else:
                if _is_quota_exhausted(res):
                    raise ApiQuotaExhausted(f"{host}: API quota exhausted ({res.status_code}).")

                retryable = res.status_code in (429, 503) or (res.status_code in (502, 504) and replay_safe)
                if not retryable:
                    self.breaker.success(host)
                    return res

                self.breaker.failure(host)
                wait = _retry_after_seconds(res)
                if wait is not None and wait > self.policy.max_wait:
                    raise ApiQuotaExhausted(f"{host}: asked to wait {wait:.0f}s (Retry-After).")
                if attempt >= self.policy.max_attempts or time.monotonic() - started >= self.policy.max_total:
                    return res
                delay = wait if wait is not None else self.policy.backoff(attempt)
                logger.warning("[RETRY] %s %s -> %s (attempt %d); sleeping %.1fs", method_u, host, res.status_code, attempt, delay)
                await asyncio.sleep(delay)
                continue

            if attempt >= self.policy.max_attempts or time.monotonic() - started >= self.policy.max_total:
                raise last_error
            delay = self.policy.backoff(attempt)
            logger.warning("[RETRY] %s %s -> %s (attempt %d); sleeping %.1fs", method_u, host, type(last_error).__name__, attempt, delay)
            await asyncio.sleep(delay)

    async def get(self, url: Any, **kwargs: Any) -> httpx.Response:
        return await self.request("GET", url, **kwargs)

    async def post(self, url: Any, **kwargs: Any) -> httpx.Response:
        return await self.request("POST", url, **kwargs)

    async def put(self, url: Any, **kwargs: Any) -> httpx.Response:
        return await self.request("PUT", url, **kwargs)

    async def patch(self, url: Any, **kwargs: Any) -> httpx.Response:
        return await self.request("PATCH", url, **kwargs)

    async def delete(self, url: Any, **kwargs: Any) -> httpx.Response:
        return await self.request("DELETE", url, **kwargs)