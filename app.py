"""
order-service: Stateless backend.
Processes orders, burns CPU intentionally so HPA/KEDA scaling triggers are visible.
Logs every event as structured JSON to stdout.
"""
import logging
import json
import os
import time
from flask import Flask, jsonify, request, Response
import prometheus_client
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

app = Flask(__name__)

# ──────────────────────────────────────────────
# Structured JSON Logger
# ──────────────────────────────────────────────
class JSONFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "service": "order-service",
            "message": record.getMessage(),
        })

handler = logging.StreamHandler()
handler.setFormatter(JSONFormatter())
logger = logging.getLogger("order-service")
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# ──────────────────────────────────────────────
# Prometheus Metrics
# ──────────────────────────────────────────────
REQUEST_COUNT = Counter(
    "order_http_requests_total",
    "Total HTTP requests to order-service",
    ["method", "endpoint", "status"]
)
REQUEST_LATENCY = Histogram(
    "order_http_request_duration_seconds",
    "Request latency in seconds for order-service",
    ["endpoint"]
)
ORDERS_PROCESSED = Counter(
    "orders_processed_total",
    "Total orders successfully processed"
)


def burn_cpu(seconds: float = 0.1):
    """Intentionally burns CPU to trigger HPA scaling. Adjustable via env var."""
    load = float(os.getenv("CPU_BURN_SECONDS", seconds))
    deadline = time.time() + load
    x = 0
    while time.time() < deadline:
        x += 1  # tight loop burns CPU


@app.route("/health")
def health():
    return jsonify({"status": "ok", "service": "order-service"}), 200


@app.route("/metrics")
def metrics():
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)


@app.route("/process", methods=["POST"])
def process_order():
    """
    Processes an order. Burns CPU intentionally.
    HPA sees CPU > threshold and scales up pods.
    KEDA ScaledObject (SQS trigger) also targets this deployment.
    """
    start = time.time()
    payload = request.get_json(silent=True) or {}
    item = payload.get("item", "unknown-item")

    logger.info(f"Processing order for item: {item}")

    # CPU burn — this is what HPA watches (adjust CPU_BURN_SECONDS env var to control intensity)
    burn_cpu()

    order_id = f"ORD-{int(time.time() * 1000)}"
    ORDERS_PROCESSED.inc()

    duration = time.time() - start
    REQUEST_COUNT.labels("POST", "/process", "200").inc()
    REQUEST_LATENCY.labels("/process").observe(duration)

    logger.info(f"Order {order_id} completed in {duration:.3f}s for item: {item}")
    return jsonify({
        "order_id": order_id,
        "item": item,
        "status": "confirmed",
        "processed_in_seconds": round(duration, 3),
        "pod": os.getenv("HOSTNAME", "unknown"),  # Shows which pod processed it — great for canary demos!
        "version": os.getenv("APP_VERSION", "v1.0.0"),
    })


@app.route("/heavy", methods=["GET"])
def heavy_load():
    """
    Endpoint for load testing. Burns CPU for a longer duration.
    Use this with the traffic-generator to trigger HPA scaling.
    """
    start = time.time()
    burn_cpu(seconds=1.0)  # 1 full second of CPU per request
    duration = time.time() - start
    REQUEST_COUNT.labels("GET", "/heavy", "200").inc()
    REQUEST_LATENCY.labels("/heavy").observe(duration)
    logger.info(f"Heavy load request processed in {duration:.3f}s")
    return jsonify({"status": "heavy_load_complete", "duration": round(duration, 3)})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
