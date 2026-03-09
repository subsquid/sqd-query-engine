# Python Implementation Patterns

## Table of Contents
1. [Structured Logging](#structured-logging)
2. [Metrics](#metrics)
3. [HTTP Client Wrapper](#http-client-wrapper)
4. [S3 Wrapper](#s3-wrapper)
5. [Health Checks](#health-checks)
6. [Graceful Shutdown](#graceful-shutdown)
7. [Retry with Backoff](#retry-with-backoff)
8. [Circuit Breaker](#circuit-breaker)
9. [Timeouts](#timeouts)

---

## Structured Logging

Use `structlog` or Python's `logging` with `python-json-logger`. structlog is preferred for its composability.

```python
import structlog

logger = structlog.get_logger()

# Bind context that persists across log calls for a request
log = logger.bind(request_id=request_id, user_id=user_id)

# Good error log for an API call failure
log.error(
    "downstream API call failed",
    service="payment-gateway",
    method="POST",
    path="/v1/charges",
    status_code=502,
    duration_ms=3200,
    error="Bad Gateway",
    retry_attempt=2,
)
```

**FastAPI middleware for request logging:**
```python
import time
import uuid
from starlette.middleware.base import BaseHTTPMiddleware

class RequestLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
        start = time.monotonic()

        structlog.contextvars.bind_contextvars(request_id=request_id)

        try:
            response = await call_next(request)
            duration_ms = (time.monotonic() - start) * 1000
            logger.info(
                "request completed",
                method=request.method,
                path=request.url.path,
                status_code=response.status_code,
                duration_ms=round(duration_ms, 2),
            )
            response.headers["X-Request-ID"] = request_id
            return response
        except Exception as exc:
            duration_ms = (time.monotonic() - start) * 1000
            logger.error(
                "request failed",
                method=request.method,
                path=request.url.path,
                duration_ms=round(duration_ms, 2),
                error=str(exc),
            )
            raise
        finally:
            structlog.contextvars.unbind_contextvars("request_id")
```

---

## Metrics

**Recommended library:** `prometheus_client` (works with any framework).

```python
from prometheus_client import Counter, Histogram

# External API metrics
http_client_requests = Counter(
    "http_client_requests_total",
    "Total outbound HTTP requests",
    ["service", "method", "status_class"],
)
http_client_duration = Histogram(
    "http_client_request_duration_seconds",
    "Outbound HTTP request latency",
    ["service", "method"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)

# S3 metrics
s3_requests = Counter(
    "s3_requests_total",
    "Total S3 operations",
    ["operation", "bucket", "status_class"],
)
s3_duration = Histogram(
    "s3_request_duration_seconds",
    "S3 operation latency",
    ["operation", "bucket"],
)
s3_bytes = Counter(
    "s3_bytes_transferred_total",
    "Bytes transferred to/from S3",
    ["operation", "bucket"],
)
```

**Status classification helper:**
```python
def classify_status(status_code: int | None, error: Exception | None = None) -> str:
    if error is not None:
        return "network_error"
    if status_code is None:
        return "network_error"
    if 200 <= status_code < 400:
        return "success"
    if status_code == 429:
        return "rate_limited"
    if 400 <= status_code < 500:
        return "client_error"
    return "server_error"
```

---

## HTTP Client Wrapper

Wrap `httpx` (async) or `requests` with metrics, logging, and classification:

```python
import httpx
import time
import structlog

logger = structlog.get_logger()

class InstrumentedClient:
    def __init__(self, service_name: str, base_url: str, timeout: float = 10.0):
        self.service = service_name
        self.client = httpx.AsyncClient(base_url=base_url, timeout=timeout)

    async def request(self, method: str, path: str, **kwargs) -> httpx.Response:
        start = time.monotonic()
        status_class = "network_error"
        status_code = None
        try:
            response = await self.client.request(method, path, **kwargs)
            status_code = response.status_code
            status_class = classify_status(status_code)
            return response
        except httpx.TimeoutException as exc:
            logger.error(
                "downstream request timed out",
                service=self.service, method=method, path=path,
                error=str(exc),
            )
            raise
        except httpx.HTTPError as exc:
            logger.error(
                "downstream request failed",
                service=self.service, method=method, path=path,
                error=str(exc),
            )
            raise
        finally:
            duration = time.monotonic() - start
            http_client_requests.labels(
                service=self.service, method=method, status_class=status_class,
            ).inc()
            http_client_duration.labels(
                service=self.service, method=method,
            ).observe(duration)
            if status_code is not None:
                logger.info(
                    "downstream request completed",
                    service=self.service, method=method, path=path,
                    status_code=status_code, status_class=status_class,
                    duration_ms=round(duration * 1000, 2),
                )
```

---

## S3 Wrapper

Wrap `boto3` with metrics and logging:

```python
import time
import boto3
import structlog
from botocore.exceptions import ClientError, BotoCoreError

logger = structlog.get_logger()

class InstrumentedS3:
    def __init__(self, bucket: str):
        self.bucket = bucket
        self.client = boto3.client("s3")

    def put_object(self, key: str, body: bytes, **kwargs):
        return self._instrumented_call("PutObject", "put_object",
            Bucket=self.bucket, Key=key, Body=body, **kwargs,
            _body_size=len(body))

    def get_object(self, key: str, **kwargs):
        return self._instrumented_call("GetObject", "get_object",
            Bucket=self.bucket, Key=key, **kwargs)

    def _instrumented_call(self, operation: str, method_name: str,
                           _body_size: int = 0, **kwargs):
        start = time.monotonic()
        status_class = "network_error"
        try:
            result = getattr(self.client, method_name)(**kwargs)
            status_class = "success"
            if _body_size:
                s3_bytes.labels(operation=operation, bucket=self.bucket).inc(_body_size)
            return result
        except ClientError as exc:
            code = exc.response["ResponseMetadata"]["HTTPStatusCode"]
            status_class = classify_status(code)
            logger.error(
                "S3 operation failed",
                operation=operation, bucket=self.bucket,
                key=kwargs.get("Key"), status_code=code,
                error=str(exc),
            )
            raise
        except BotoCoreError as exc:
            logger.error(
                "S3 operation failed (network)",
                operation=operation, bucket=self.bucket,
                key=kwargs.get("Key"), error=str(exc),
            )
            raise
        finally:
            duration = time.monotonic() - start
            s3_requests.labels(
                operation=operation, bucket=self.bucket, status_class=status_class,
            ).inc()
            s3_duration.labels(
                operation=operation, bucket=self.bucket,
            ).observe(duration)
```

---

## Health Checks

**FastAPI example:**
```python
from fastapi import FastAPI, Response

app = FastAPI()
_ready = False

@app.on_event("startup")
async def startup():
    global _ready
    # ... initialize DB pool, warm caches, etc.
    _ready = True

@app.get("/health/live")
async def liveness():
    return {"status": "ok"}

@app.get("/health/ready")
async def readiness(response: Response):
    if not _ready:
        response.status_code = 503
        return {"status": "not ready"}
    # optionally check DB connection, etc.
    return {"status": "ready"}
```

---

## Graceful Shutdown

```python
import signal
import asyncio
import structlog

logger = structlog.get_logger()

class GracefulShutdown:
    def __init__(self, timeout: float = 30.0):
        self.timeout = timeout
        self._shutting_down = False
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logger.info("SIGTERM received, starting graceful shutdown")
        self._shutting_down = True

    @property
    def is_shutting_down(self) -> bool:
        return self._shutting_down

    async def wait_for_shutdown(self, cleanup_coro):
        """Call this after stopping the listener."""
        try:
            await asyncio.wait_for(cleanup_coro, timeout=self.timeout)
            logger.info("graceful shutdown completed")
        except asyncio.TimeoutError:
            logger.warning(
                "graceful shutdown timed out, force exiting",
                timeout_seconds=self.timeout,
            )
```

---

## Retry with Backoff

Use `tenacity` for retry logic:

```python
from tenacity import (
    retry, stop_after_attempt, wait_exponential_jitter,
    retry_if_exception_type, before_sleep_log,
)
import structlog

logger = structlog.get_logger()

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=0.5, max=10, jitter=2),
    retry=retry_if_exception_type((httpx.TimeoutException, httpx.HTTPStatusError)),
    before_sleep=before_sleep_log(logger, structlog.stdlib.log_level),
)
async def call_with_retry(client: InstrumentedClient, method: str, path: str, **kwargs):
    response = await client.request(method, path, **kwargs)
    if response.status_code in (429, 502, 503, 504):
        raise httpx.HTTPStatusError(
            f"Retryable status {response.status_code}",
            request=response.request, response=response,
        )
    return response
```

---

## Circuit Breaker

Use `pybreaker`:

```python
import pybreaker
import structlog

logger = structlog.get_logger()

class LoggingListener(pybreaker.CircuitBreakerListener):
    def state_change(self, cb, old_state, new_state):
        logger.warning(
            "circuit breaker state changed",
            breaker=cb.name, old_state=str(old_state), new_state=str(new_state),
        )

payment_breaker = pybreaker.CircuitBreaker(
    fail_max=5,
    reset_timeout=30,
    listeners=[LoggingListener()],
    name="payment-api",
)

@payment_breaker
async def call_payment_api(...):
    ...
```

---

## Timeouts

Configure per-dependency via environment variables:

```python
import os

class TimeoutConfig:
    PAYMENT_API_TIMEOUT = float(os.getenv("PAYMENT_API_TIMEOUT_MS", "5000")) / 1000
    USER_SERVICE_TIMEOUT = float(os.getenv("USER_SERVICE_TIMEOUT_MS", "3000")) / 1000
    DB_QUERY_TIMEOUT = float(os.getenv("DB_QUERY_TIMEOUT_MS", "10000")) / 1000
    S3_TIMEOUT = float(os.getenv("S3_TIMEOUT_MS", "30000")) / 1000
```

Use these when constructing clients — never hardcode timeouts in call sites.
