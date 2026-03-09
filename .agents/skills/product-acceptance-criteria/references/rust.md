# Rust Implementation Patterns

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

Use `tracing` with `tracing-subscriber` and a JSON formatting layer. The `tracing` ecosystem is the standard for structured, context-aware logging in Rust.

```rust
use tracing::{error, info, warn, instrument, Span};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

fn init_logging() {
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer().json())
        .init();
}

// Using spans for request context
#[instrument(skip_all, fields(request_id = %request_id, user_id = %user_id))]
async fn handle_request(request_id: &str, user_id: &str) {
    // All logs within this function automatically include request_id and user_id

    // Good error log for API failure
    error!(
        service = "payment-gateway",
        method = "POST",
        path = "/v1/charges",
        status_code = 502,
        duration_ms = 3200,
        error = "Bad Gateway",
        retry_attempt = 2,
        "downstream API call failed"
    );
}
```

**Axum middleware for request logging:**
```rust
use axum::{middleware::Next, http::Request, response::Response};
use std::time::Instant;
use uuid::Uuid;

async fn request_logging<B>(req: Request<B>, next: Next<B>) -> Response {
    let request_id = req
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(String::from)
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    let start = Instant::now();

    let span = tracing::info_span!("request", %request_id, %method, %path);
    let _guard = span.enter();

    let response = next.run(req).await;

    let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
    let status = response.status().as_u16();

    info!(status_code = status, duration_ms = format!("{:.2}", duration_ms), "request completed");

    response
}
```

---

## Metrics

**Recommended:** `prometheus` crate or `metrics` crate with prometheus exporter.

```rust
use prometheus::{
    register_counter_vec, register_histogram_vec, CounterVec, HistogramVec, Encoder, TextEncoder,
};
use lazy_static::lazy_static;

lazy_static! {
    // External API metrics
    static ref HTTP_CLIENT_REQUESTS: CounterVec = register_counter_vec!(
        "http_client_requests_total",
        "Total outbound HTTP requests",
        &["service", "method", "status_class"]
    ).unwrap();

    static ref HTTP_CLIENT_DURATION: HistogramVec = register_histogram_vec!(
        "http_client_request_duration_seconds",
        "Outbound HTTP request latency",
        &["service", "method"],
        vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
    ).unwrap();

    // S3 metrics
    static ref S3_REQUESTS: CounterVec = register_counter_vec!(
        "s3_requests_total",
        "Total S3 operations",
        &["operation", "bucket", "status_class"]
    ).unwrap();

    static ref S3_DURATION: HistogramVec = register_histogram_vec!(
        "s3_request_duration_seconds",
        "S3 operation latency",
        &["operation", "bucket"]
    ).unwrap();

    static ref S3_BYTES: CounterVec = register_counter_vec!(
        "s3_bytes_transferred_total",
        "Bytes transferred to/from S3",
        &["operation", "bucket"]
    ).unwrap();
}

// Metrics endpoint handler (axum)
async fn metrics_handler() -> String {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}
```

**Status classification:**
```rust
fn classify_status(status: Option<u16>, is_network_error: bool) -> &'static str {
    if is_network_error {
        return "network_error";
    }
    match status {
        None => "network_error",
        Some(code) if (200..400).contains(&code) => "success",
        Some(429) => "rate_limited",
        Some(code) if (400..500).contains(&code) => "client_error",
        Some(_) => "server_error",
    }
}
```

---

## HTTP Client Wrapper

Wrap `reqwest` with metrics and logging:

```rust
use reqwest::{Client, Method, Response};
use std::time::Instant;

pub struct InstrumentedClient {
    client: Client,
    service: String,
    base_url: String,
}

impl InstrumentedClient {
    pub fn new(service: &str, base_url: &str, timeout: std::time::Duration) -> Self {
        let client = Client::builder()
            .timeout(timeout)
            .build()
            .expect("failed to build HTTP client");
        Self {
            client,
            service: service.to_string(),
            base_url: base_url.to_string(),
        }
    }

    pub async fn request(
        &self,
        method: Method,
        path: &str,
    ) -> Result<Response, reqwest::Error> {
        let url = format!("{}{}", self.base_url, path);
        let method_str = method.as_str();
        let start = Instant::now();
        let mut status_class = "network_error";

        let result = self.client.request(method.clone(), &url).send().await;

        let duration = start.elapsed().as_secs_f64();

        match &result {
            Ok(resp) => {
                let code = resp.status().as_u16();
                status_class = classify_status(Some(code), false);
                info!(
                    service = %self.service,
                    method = method_str,
                    path = path,
                    status_code = code,
                    status_class = status_class,
                    duration_ms = format!("{:.2}", duration * 1000.0),
                    "downstream request completed"
                );
            }
            Err(e) => {
                error!(
                    service = %self.service,
                    method = method_str,
                    path = path,
                    error = %e,
                    "downstream request failed"
                );
            }
        }

        HTTP_CLIENT_REQUESTS
            .with_label_values(&[&self.service, method_str, status_class])
            .inc();
        HTTP_CLIENT_DURATION
            .with_label_values(&[&self.service, method_str])
            .observe(duration);

        result
    }
}
```

---

## S3 Wrapper

Wrap `aws-sdk-s3` with metrics:

```rust
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::primitives::ByteStream;
use std::time::Instant;

pub struct InstrumentedS3 {
    client: S3Client,
    bucket: String,
}

impl InstrumentedS3 {
    pub fn new(client: S3Client, bucket: &str) -> Self {
        Self { client, bucket: bucket.to_string() }
    }

    pub async fn put_object(&self, key: &str, body: Vec<u8>) -> Result<(), aws_sdk_s3::Error> {
        let body_len = body.len();
        let start = Instant::now();
        let mut status_class = "network_error";

        let result = self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(body))
            .send()
            .await;

        let duration = start.elapsed().as_secs_f64();

        match &result {
            Ok(_) => {
                status_class = "success";
                S3_BYTES
                    .with_label_values(&["PutObject", &self.bucket])
                    .inc_by(body_len as f64);
            }
            Err(e) => {
                error!(
                    operation = "PutObject",
                    bucket = %self.bucket,
                    key = key,
                    error = %e,
                    "S3 operation failed"
                );
            }
        }

        S3_REQUESTS
            .with_label_values(&["PutObject", &self.bucket, status_class])
            .inc();
        S3_DURATION
            .with_label_values(&["PutObject", &self.bucket])
            .observe(duration);

        result.map(|_| ())
    }
}
```

---

## Health Checks

```rust
use axum::{routing::get, Router, Json, http::StatusCode};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

static READY: AtomicBool = AtomicBool::new(false);

async fn liveness() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}

async fn readiness() -> (StatusCode, Json<serde_json::Value>) {
    if READY.load(Ordering::Relaxed) {
        (StatusCode::OK, Json(serde_json::json!({"status": "ready"})))
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"status": "not ready"})))
    }
}

fn health_routes() -> Router {
    Router::new()
        .route("/health/live", get(liveness))
        .route("/health/ready", get(readiness))
}
```

---

## Graceful Shutdown

```rust
use tokio::signal;
use std::time::Duration;

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };

    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    tokio::select! {
        _ = ctrl_c => info!("SIGINT received"),
        _ = terminate => info!("SIGTERM received"),
    }

    info!("starting graceful shutdown");
}

// Usage with axum
let server = axum::Server::bind(&addr)
    .serve(app.into_make_service())
    .with_graceful_shutdown(shutdown_signal());

// Add a hard deadline
tokio::select! {
    result = server => {
        if let Err(e) = result { error!(error = %e, "server error"); }
    }
    _ = tokio::time::sleep(Duration::from_secs(30)) => {
        warn!("graceful shutdown timed out, force exiting");
    }
}
```

---

## Retry with Backoff

Use `backon` or `tokio-retry`:

```rust
use backon::{ExponentialBuilder, Retryable};
use std::time::Duration;

async fn call_with_retry(client: &InstrumentedClient, path: &str) -> Result<Response, reqwest::Error> {
    let backoff = ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(500))
        .with_max_delay(Duration::from_secs(10))
        .with_max_times(3)
        .with_jitter();

    (|| async { client.request(Method::GET, path).await })
        .retry(backoff)
        .notify(|err, dur| {
            warn!(
                error = %err,
                next_retry_in_ms = dur.as_millis(),
                "retrying failed request"
            );
        })
        .await
}
```

---

## Circuit Breaker

A simple implementation (or use a crate like `failsafe`):

```rust
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CircuitState { Closed, Open, HalfOpen }

pub struct CircuitBreaker {
    name: String,
    failure_count: AtomicU32,
    failure_threshold: u32,
    reset_timeout: Duration,
    last_failure_time: AtomicU64,
}

impl CircuitBreaker {
    pub fn new(name: &str, failure_threshold: u32, reset_timeout: Duration) -> Self {
        Self {
            name: name.to_string(),
            failure_count: AtomicU32::new(0),
            failure_threshold,
            reset_timeout,
            last_failure_time: AtomicU64::new(0),
        }
    }

    pub fn state(&self) -> CircuitState {
        let failures = self.failure_count.load(Ordering::Relaxed);
        if failures < self.failure_threshold {
            return CircuitState::Closed;
        }
        let last = self.last_failure_time.load(Ordering::Relaxed);
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        if now - last > self.reset_timeout.as_secs() {
            CircuitState::HalfOpen
        } else {
            CircuitState::Open
        }
    }

    pub fn record_success(&self) {
        let prev = self.failure_count.swap(0, Ordering::Relaxed);
        if prev >= self.failure_threshold {
            info!(breaker = %self.name, "circuit breaker closed");
        }
    }

    pub fn record_failure(&self) {
        let count = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        self.last_failure_time.store(now, Ordering::Relaxed);
        if count == self.failure_threshold {
            warn!(breaker = %self.name, "circuit breaker opened");
        }
    }
}
```

---

## Timeouts

Configure via environment variables using `envy` or `config`:

```rust
use std::time::Duration;

#[derive(Debug)]
pub struct TimeoutConfig {
    pub payment_api: Duration,
    pub user_service: Duration,
    pub db_query: Duration,
    pub s3: Duration,
}

impl TimeoutConfig {
    pub fn from_env() -> Self {
        Self {
            payment_api: duration_from_env("PAYMENT_API_TIMEOUT_MS", 5000),
            user_service: duration_from_env("USER_SERVICE_TIMEOUT_MS", 3000),
            db_query: duration_from_env("DB_QUERY_TIMEOUT_MS", 10000),
            s3: duration_from_env("S3_TIMEOUT_MS", 30000),
        }
    }
}

fn duration_from_env(key: &str, default_ms: u64) -> Duration {
    let ms: u64 = std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default_ms);
    Duration::from_millis(ms)
}
```

**Request-scoped deadline with tokio:**
```rust
use tokio::time::timeout;

async fn with_deadline<F, T, E>(duration: Duration, f: F) -> Result<T, E>
where
    F: std::future::Future<Output = Result<T, E>>,
    E: From<tokio::time::error::Elapsed>,
{
    timeout(duration, f).await?
}
```
