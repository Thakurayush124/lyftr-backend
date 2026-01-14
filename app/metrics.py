from prometheus_client import Counter, Histogram

http_requests_total = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["path", "status"]
)

webhook_requests_total = Counter(
    "webhook_requests_total",
    "Webhook processing results",
    ["result"]
)

request_latency = Histogram(
    "request_latency_ms",
    "Request latency in ms",
    buckets=[100, 300, 500, 1000]
)
