"""
Prometheus metrics definitions shared across services.
Each service imports only the metrics it needs.
Import this module after setting the service name via set_service_name().
"""

from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry

# Use the default registry — each service runs in its own process
# so there is no collision.

# ---------------------------------------------------------------------------
# API Gateway metrics
# ---------------------------------------------------------------------------

rate_limit_hits = Counter(
    "rate_limit_hits_total",
    "Number of requests rejected by the rate limiter",
    ["tenant_id"],
)

auth_failures = Counter(
    "auth_failures_total",
    "Number of authentication failures",
)

gateway_requests_total = Counter(
    "gateway_requests_total",
    "Total requests received by the API gateway",
    ["tenant_id", "tier"],
)

# ---------------------------------------------------------------------------
# Inference Gateway metrics
# ---------------------------------------------------------------------------

active_sse_streams = Gauge(
    "active_sse_streams",
    "Number of currently open SSE connections",
)

queue_enqueue_total = Counter(
    "queue_enqueue_total",
    "Total requests enqueued",
    ["tier"],
)

sse_timeout_total = Counter(
    "sse_timeout_total",
    "SSE streams that timed out waiting for worker",
    ["tenant_id"],
)

# ---------------------------------------------------------------------------
# Worker metrics
# ---------------------------------------------------------------------------

tokens_generated_total = Counter(
    "tokens_generated_total",
    "Total tokens generated",
    ["worker_id"],
)

jobs_processed_total = Counter(
    "jobs_processed_total",
    "Total inference jobs completed",
    ["worker_id", "status"],  # status: success | error | expired
)

job_processing_time = Histogram(
    "job_processing_seconds",
    "Time taken to complete a full inference job",
    ["worker_id"],
    buckets=[0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0],
)

# ---------------------------------------------------------------------------
# Shared / cross-service metrics
# ---------------------------------------------------------------------------

queue_depth = Gauge(
    "queue_depth",
    "Current number of pending jobs in the queue",
    ["tier"],
)

request_latency = Histogram(
    "request_latency_seconds",
    "End-to-end request latency",
    ["tenant_id"],
    buckets=[0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0],
)