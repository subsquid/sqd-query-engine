# Node.js Implementation Patterns

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

Use `pino` — it's the fastest structured logger for Node.js and outputs JSON by default.

```typescript
import pino from "pino";

const logger = pino({
  base: undefined,
  level: process.env.LOG_LEVEL ?? 'info',
  messageKey: 'message',
  timestamp: pino.stdTimeFunctions.isoTime,
  serializers: {
    error: pino.stdSerializers.errWithCause,
  },
  transport:
    process.stdout?.isTTY
      ? {
          target: 'pino-pretty',
          options: {
            messageKey: 'message',
            singleLine: true,
          },
        }
      : undefined,
})

// Create a child logger with request context
const reqLogger = logger.child({ requestId, userId });

// Good error log — single-object style with embedded context in message
reqLogger.error({
  message: 'Downstream API call failed: POST /v1/charges returned 502',
  service: 'payment-gateway',
  method: 'POST',
  path: '/v1/charges',
  statusCode: 502,
  durationMs: 3200,
  error, // serialized by errWithCause
  retryAttempt: 2,
})
```

**Express middleware for request logging:**
```typescript
import { randomUUID } from "crypto";
import { Request, Response, NextFunction } from "express";

function requestLogger(req: Request, res: Response, next: NextFunction) {
  const requestId = (req.headers["x-request-id"] as string) || randomUUID();
  const start = process.hrtime.bigint();

  // Attach logger to request for downstream use
  req.log = logger.child({ requestId });

  res.on("finish", () => {
    const durationMs =
      Number(process.hrtime.bigint() - start) / 1_000_000;
    req.log.info({
      message: `${req.method} ${req.path} completed with ${res.statusCode}`,
      method: req.method,
      path: req.path,
      statusCode: res.statusCode,
      durationMs: Math.round(durationMs * 100) / 100,
    });
  });

  res.setHeader("X-Request-ID", requestId);
  next();
}
```

---

## Metrics

**Recommended:** `prom-client` (the standard Prometheus client for Node.js).

```typescript
import client from "prom-client";

// Collect default metrics (event loop lag, heap, GC, etc.)
client.collectDefaultMetrics();

// External API metrics
const httpClientRequests = new client.Counter({
  name: "http_client_requests_total",
  help: "Total outbound HTTP requests",
  labelNames: ["service", "method", "status_class"] as const,
});

const httpClientDuration = new client.Histogram({
  name: "http_client_request_duration_seconds",
  help: "Outbound HTTP request latency",
  labelNames: ["service", "method"] as const,
  buckets: [0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
});

// S3 metrics
const s3Requests = new client.Counter({
  name: "s3_requests_total",
  help: "Total S3 operations",
  labelNames: ["operation", "bucket", "status_class"] as const,
});

const s3Duration = new client.Histogram({
  name: "s3_request_duration_seconds",
  help: "S3 operation latency",
  labelNames: ["operation", "bucket"] as const,
});

const s3Bytes = new client.Counter({
  name: "s3_bytes_transferred_total",
  help: "Bytes transferred to/from S3",
  labelNames: ["operation", "bucket"] as const,
});

// Expose metrics endpoint
app.get("/metrics", async (req, res) => {
  res.set("Content-Type", client.register.contentType);
  res.end(await client.register.metrics());
});
```

**Status classification helper:**
```typescript
function classifyStatus(
  statusCode: number | undefined,
  error?: Error
): string {
  if (error) return "network_error";
  if (statusCode === undefined) return "network_error";
  if (statusCode >= 200 && statusCode < 400) return "success";
  if (statusCode === 429) return "rate_limited";
  if (statusCode >= 400 && statusCode < 500) return "client_error";
  return "server_error";
}
```

---

## HTTP Client Wrapper

Wrap `got` or `axios` with metrics and logging:

```typescript
import got, { Options, Response, RequestError } from "got";
import pino from "pino";

interface ClientConfig {
  serviceName: string;
  baseUrl: string;
  timeoutMs?: number;
}

function createInstrumentedClient(config: ClientConfig) {
  const instance = got.extend({
    prefixUrl: config.baseUrl,
    timeout: { request: config.timeoutMs ?? 10_000 },
    hooks: {
      afterResponse: [
        (response: Response) => {
          const statusClass = classifyStatus(response.statusCode);
          httpClientRequests
            .labels(config.serviceName, response.request.options.method, statusClass)
            .inc();
          return response;
        },
      ],
      beforeError: [
        (error: RequestError) => {
          httpClientRequests
            .labels(config.serviceName, error.options?.method ?? "UNKNOWN", "network_error")
            .inc();
          logger.error({
            message: `Downstream request failed: ${error.options?.method} ${error.options?.url?.pathname}`,
            service: config.serviceName,
            method: error.options?.method,
            url: error.options?.url?.pathname,
            error,
          });
          return error;
        },
      ],
    },
  });

  return instance;
}
```

---

## S3 Wrapper

Wrap `@aws-sdk/client-s3` with metrics and logging:

```typescript
import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  PutObjectCommandInput,
  GetObjectCommandInput,
} from "@aws-sdk/client-s3";
import pino from "pino";

class InstrumentedS3 {
  private client: S3Client;
  private bucket: string;

  constructor(bucket: string) {
    this.bucket = bucket;
    this.client = new S3Client({});
  }

  async putObject(key: string, body: Buffer, extra?: Partial<PutObjectCommandInput>) {
    return this.instrumented("PutObject", () =>
      this.client.send(
        new PutObjectCommand({ Bucket: this.bucket, Key: key, Body: body, ...extra })
      ),
      body.length,
    );
  }

  async getObject(key: string, extra?: Partial<GetObjectCommandInput>) {
    return this.instrumented("GetObject", () =>
      this.client.send(
        new GetObjectCommand({ Bucket: this.bucket, Key: key, ...extra })
      ),
    );
  }

  private async instrumented<T>(
    operation: string,
    fn: () => Promise<T>,
    bodySize?: number
  ): Promise<T> {
    const start = process.hrtime.bigint();
    let statusClass = "network_error";
    try {
      const result = await fn();
      statusClass = "success";
      if (bodySize) {
        s3Bytes.labels(operation, this.bucket).inc(bodySize);
      }
      return result;
    } catch (err: any) {
      const httpStatus = err.$metadata?.httpStatusCode;
      statusClass = classifyStatus(httpStatus, err);
      logger.error({
        message: `S3 ${operation} failed on ${this.bucket}`,
        operation,
        bucket: this.bucket,
        statusCode: httpStatus,
        error: err,
      });
      throw err;
    } finally {
      const durationSec = Number(process.hrtime.bigint() - start) / 1e9;
      s3Requests.labels(operation, this.bucket, statusClass).inc();
      s3Duration.labels(operation, this.bucket).observe(durationSec);
    }
  }
}
```

---

## Health Checks

```typescript
let isReady = false;

app.get("/health/live", (req, res) => {
  res.json({ status: "ok" });
});

app.get("/health/ready", async (req, res) => {
  if (!isReady) {
    return res.status(503).json({ status: "not ready" });
  }
  // Optionally check DB, cache, etc.
  try {
    await db.query("SELECT 1");
    res.json({ status: "ready" });
  } catch {
    res.status(503).json({ status: "dependency unavailable" });
  }
});
```

---

## Graceful Shutdown

```typescript
import { Server } from "http";

function setupGracefulShutdown(server: Server, timeoutMs = 30_000) {
  let shuttingDown = false;

  async function shutdown(signal: string) {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info({ message: `Shutdown signal received: ${signal}, draining connections`, signal });

    // Stop accepting new connections
    server.close(() => {
      logger.info({ message: 'All connections drained, exiting' });
      process.exit(0);
    });

    // Force exit after timeout
    setTimeout(() => {
      logger.warn({ message: `Shutdown timed out after ${timeoutMs}ms, force exiting`, timeoutMs });
      process.exit(1);
    }, timeoutMs).unref();
  }

  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));
}
```

---

## Retry with Backoff

Use `p-retry` or implement manually:

```typescript
import pRetry, { AbortError } from "p-retry";

async function callWithRetry<T>(
  fn: () => Promise<T>,
  opts: { retries?: number; service?: string } = {}
): Promise<T> {
  return pRetry(fn, {
    retries: opts.retries ?? 3,
    minTimeout: 500,
    maxTimeout: 10_000,
    randomize: true, // adds jitter
    onFailedAttempt: (error) => {
      logger.warn({
        message: `Retrying failed request to ${opts.service} (attempt ${error.attemptNumber}, ${error.retriesLeft} left)`,
        service: opts.service,
        attempt: error.attemptNumber,
        retriesLeft: error.retriesLeft,
        error,
      });
    },
  });
}

// Usage — abort on non-retryable errors
const result = await callWithRetry(async () => {
  const response = await client.get("endpoint");
  if (response.statusCode >= 400 && response.statusCode < 500 && response.statusCode !== 429) {
    throw new AbortError(`Non-retryable: ${response.statusCode}`);
  }
  return response;
}, { service: "payment-api" });
```

---

## Circuit Breaker

Use `opossum`:

```typescript
import CircuitBreaker from "opossum";

const breaker = new CircuitBreaker(callPaymentApi, {
  timeout: 5000,
  errorThresholdPercentage: 50,
  resetTimeout: 30_000,
  volumeThreshold: 10,
});

breaker.on("open", () =>
  logger.warn({ message: 'Circuit breaker opened for payment-api', breaker: 'payment-api' })
);
breaker.on("halfOpen", () =>
  logger.warn({ message: 'Circuit breaker half-open for payment-api', breaker: 'payment-api' })
);
breaker.on("close", () =>
  logger.info({ message: 'Circuit breaker closed for payment-api', breaker: 'payment-api' })
);

// Expose state as metric
const circuitState = new client.Gauge({
  name: "circuit_breaker_state",
  help: "Circuit breaker state (0=closed, 1=half-open, 2=open)",
  labelNames: ["service"] as const,
});

breaker.on("open", () => circuitState.labels("payment-api").set(2));
breaker.on("halfOpen", () => circuitState.labels("payment-api").set(1));
breaker.on("close", () => circuitState.labels("payment-api").set(0));
```

---

## Timeouts

Configure via environment variables:

```typescript
const timeouts = {
  paymentApi: parseInt(process.env.PAYMENT_API_TIMEOUT_MS ?? "5000", 10),
  userService: parseInt(process.env.USER_SERVICE_TIMEOUT_MS ?? "3000", 10),
  dbQuery: parseInt(process.env.DB_QUERY_TIMEOUT_MS ?? "10000", 10),
  s3: parseInt(process.env.S3_TIMEOUT_MS ?? "30000", 10),
} as const;
```

**AbortController for request-scoped deadlines:**
```typescript
function withDeadline<T>(
  fn: (signal: AbortSignal) => Promise<T>,
  timeoutMs: number
): Promise<T> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  return fn(controller.signal).finally(() => clearTimeout(timer));
}

// Usage: propagate deadline to downstream calls
const result = await withDeadline(
  (signal) => got.get("endpoint", { signal }),
  timeouts.paymentApi
);
```
