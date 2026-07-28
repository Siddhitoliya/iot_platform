#!/usr/bin/env python3
"""
Prometheus metrics exporter for worker
"""
import time
import threading
from prometheus_client import start_http_server, Counter, Gauge, Histogram
import redis
from rq import Queue
import os

# Initialize metrics
PROCESSED_COUNTER = Counter(
    'sensor_processed_total',
    'Total sensor readings processed',
    ['status', 'queue']
)

QUEUE_GAUGE = Gauge(
    'sensor_queue_size',
    'Current queue size',
    ['queue']
)

PROCESSING_HISTOGRAM = Histogram(
    'sensor_processing_seconds',
    'Time to process sensor data',
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0]
)

ERRORS_COUNTER = Counter(
    'sensor_errors_total',
    'Total processing errors',
    ['type']
)

def update_queue_metrics():
    """Update queue size metrics"""
    redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
    try:
        redis_conn = redis.from_url(redis_url)
        queues = ['high', 'default', 'low']
        for q in queues:
            size = Queue(q, connection=redis_conn).count
            QUEUE_GAUGE.labels(queue=q).set(size)
    except Exception as e:
        ERRORS_COUNTER.labels(type='queue_metric').inc()

def start_metrics_server(port=8000):
    """Start Prometheus metrics server"""
    start_http_server(port)
    print(f"✅ Metrics server started on port {port}")
    
    # Update queue metrics periodically
    while True:
        update_queue_metrics()
        time.sleep(10)

if __name__ == "__main__":
    start_metrics_server()