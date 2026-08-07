#!/usr/bin/env python3
"""
CloudWatch-style monitoring script
"""
import json
import time
import logging
import threading
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
import paho.mqtt.client as mqtt
import redis
import psycopg2
import requests
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class Metric:
    name: str
    value: float
    timestamp: datetime
    dimensions: dict
    unit: str = "Count"

class MetricStore:
    def __init__(self, retention_minutes=60):
        self.metrics = {}
        self.retention = timedelta(minutes=retention_minutes)
        self.lock = threading.Lock()
    
    def add_metric(self, metric: Metric):
        key = f"{metric.name}|{'_'.join(f'{k}={v}' for k,v in sorted(metric.dimensions.items()))}"
        with self.lock:
            if key not in self.metrics:
                self.metrics[key] = []
            self.metrics[key].append(metric)
            cutoff = datetime.utcnow() - self.retention
            self.metrics[key] = [m for m in self.metrics[key] if m.timestamp > cutoff]
    
    def get_metrics(self, name: str, dimensions: dict, period: int, statistic: str = "avg"):
        key = f"{name}|{'_'.join(f'{k}={v}' for k,v in sorted(dimensions.items()))}"
        if key not in self.metrics:
            return None
        cutoff = datetime.utcnow() - timedelta(seconds=period)
        values = [m.value for m in self.metrics[key] if m.timestamp > cutoff]
        if not values:
            return None
        if statistic == "avg":
            return sum(values) / len(values)
        elif statistic == "sum":
            return sum(values)
        elif statistic == "max":
            return max(values)
        elif statistic == "min":
            return min(values)
        return values[-1]

class Monitor:
    def __init__(self, collect_interval=10, alarm_interval=30):
        self.store = MetricStore()
        self.running = False
        self.collect_interval = collect_interval
        self.alarm_interval = alarm_interval
        
        self.collectors = [
            self.collect_mqtt,
            self.collect_redis,
            self.collect_postgres
        ]
    
    def collect_mqtt(self):
        try:
            client = mqtt.Client()
            start = time.time()
            connected = False
            try:
                client.connect('localhost', 1883, timeout=2)
                connected = True
                client.disconnect()
            except:
                pass
            self.store.add_metric(Metric('mqtt_health', 1.0 if connected else 0.0, 
                                         datetime.utcnow(), {'component': 'mqtt'}))
        except Exception as e:
            logger.error(f"MQTT collector error: {e}")
    
    def collect_redis(self):
        try:
            r = redis.Redis(host='localhost', port=6379, socket_timeout=2)
            try:
                r.ping()
                connected = True
                for q in ['high', 'default', 'low']:
                    size = r.llen(f'rq:queue:{q}')
                    self.store.add_metric(Metric('redis_queue_size', size, datetime.utcnow(), 
                                                 {'queue': q}))
                total = sum(r.llen(f'rq:queue:{q}') for q in ['high', 'default', 'low'])
                self.store.add_metric(Metric('redis_total_queue_size', total, datetime.utcnow(),
                                             {'component': 'redis'}))
            except:
                connected = False
            self.store.add_metric(Metric('redis_health', 1.0 if connected else 0.0,
                                         datetime.utcnow(), {'component': 'redis'}))
        except Exception as e:
            logger.error(f"Redis collector error: {e}")
    
    def collect_postgres(self):
        try:
            conn = psycopg2.connect(host='localhost', port=5432, database='oxygen_data',
                                     user='pipeline', password='pipeline123', connect_timeout=2)
            connected = True
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM oxygen_readings")
            count = cur.fetchone()[0]
            self.store.add_metric(Metric('postgres_record_count', count, datetime.utcnow(),
                                         {'component': 'postgres'}))
            cur.close()
            conn.close()
        except:
            connected = False
        self.store.add_metric(Metric('postgres_health', 1.0 if connected else 0.0,
                                     datetime.utcnow(), {'component': 'postgres'}))
    
    def evaluate_alarms(self):
        # Simple alarms
        alarms = [
            ('mqtt_down', 'mqtt_health', 60, 0.5, 'lt'),
            ('redis_down', 'redis_health', 60, 0.5, 'lt'),
            ('postgres_down', 'postgres_health', 60, 0.5, 'lt'),
            ('queue_backlog', 'redis_total_queue_size', 60, 100, 'gt'),
        ]
        
        for name, metric, period, threshold, comparison in alarms:
            value = self.store.get_metrics(metric, {}, period, 'avg')
            if value is None:
                continue
            triggered = (comparison == 'lt' and value < threshold) or (comparison == 'gt' and value > threshold)
            state = "ALARM" if triggered else "OK"
            if triggered:
                logger.error(f"🚨 ALARM: {name} - {metric}={value:.2f} (threshold: {threshold})")
            else:
                logger.info(f"✅ OK: {name} - {metric}={value:.2f}")
    
    def run(self):
        self.running = True
        logger.info("🚀 Starting monitor")
        
        while self.running:
            for collector in self.collectors:
                try:
                    collector()
                except Exception as e:
                    logger.error(f"Collector error: {e}")
            
            self.evaluate_alarms()
            
            # Print summary
            mqtt = self.store.get_metrics('mqtt_health', {}, 10, 'avg')
            redis = self.store.get_metrics('redis_health', {}, 10, 'avg')
            postgres = self.store.get_metrics('postgres_health', {}, 10, 'avg')
            queue = self.store.get_metrics('redis_total_queue_size', {}, 10, 'avg')
            
            status = [
                f"MQTT: {'✅ UP' if mqtt and mqtt > 0 else '❌ DOWN'}",
                f"Redis: {'✅ UP' if redis and redis > 0 else '❌ DOWN'}",
                f"Postgres: {'✅ UP' if postgres and postgres > 0 else '❌ DOWN'}",
                f"Queue: {int(queue or 0)} jobs"
            ]
            logger.info("📊 " + " | ".join(status))
            
            time.sleep(self.collect_interval)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--collect-interval", type=int, default=10)
    parser.add_argument("--alarm-interval", type=int, default=30)
    args = parser.parse_args()
    
    Monitor(args.collect_interval, args.alarm_interval).run()