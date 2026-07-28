#!/usr/bin/env python3
"""
CloudWatch-style monitoring for self-healing pipeline
Collects metrics, evaluates rules, triggers alerts
"""
import json
import time
import logging
import threading
import socket
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict
import paho.mqtt.client as mqtt
import redis
import psycopg2
import requests
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/var/log/monitor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# Data Models
# ============================================================================

@dataclass
class Metric:
    """Single metric data point"""
    name: str
    value: float
    timestamp: datetime
    dimensions: Dict[str, str]
    unit: str = "Count"

@dataclass
class Alarm:
    """Alarm configuration"""
    name: str
    metric: str
    statistic: str  # avg, sum, max, min, count
    period: int  # seconds
    threshold: float
    comparison: str  # gt, lt, gte, lte, eq
    dimensions: Dict[str, str]
    actions: List[str]
    current_state: str = "OK"
    last_evaluation: Optional[datetime] = None
    state_changed: bool = False

@dataclass
class Notification:
    """Alert notification"""
    alarm_name: str
    state: str
    metric: str
    value: float
    threshold: float
    timestamp: datetime
    dimensions: Dict[str, str]

# ============================================================================
# Metric Storage
# ============================================================================

class MetricStore:
    """In-memory metric storage with retention"""
    
    def __init__(self, retention_minutes=60):
        self.metrics: Dict[str, List[Metric]] = {}
        self.retention = timedelta(minutes=retention_minutes)
        self.lock = threading.Lock()
    
    def add_metric(self, metric: Metric):
        """Add a metric data point"""
        key = self._get_key(metric.name, metric.dimensions)
        with self.lock:
            if key not in self.metrics:
                self.metrics[key] = []
            self.metrics[key].append(metric)
            
            # Cleanup old data
            cutoff = datetime.utcnow() - self.retention
            self.metrics[key] = [
                m for m in self.metrics[key]
                if m.timestamp > cutoff
            ]
    
    def get_metrics(self, name: str, dimensions: Dict[str, str], 
                    period: int, statistic: str = "avg") -> Optional[float]:
        """Get aggregated metric value"""
        key = self._get_key(name, dimensions)
        if key not in self.metrics:
            return None
        
        with self.lock:
            # Get metrics within period
            cutoff = datetime.utcnow() - timedelta(seconds=period)
            values = [
                m.value for m in self.metrics[key]
                if m.timestamp > cutoff
            ]
            
            if not values:
                return None
            
            # Apply statistic
            if statistic == "avg":
                return sum(values) / len(values)
            elif statistic == "sum":
                return sum(values)
            elif statistic == "max":
                return max(values)
            elif statistic == "min":
                return min(values)
            elif statistic == "count":
                return len(values)
            else:
                return values[-1]  # latest
    
    def _get_key(self, name: str, dimensions: Dict[str, str]) -> str:
        """Create unique key for metric+dimensions"""
        dims = "_".join(f"{k}={v}" for k, v in sorted(dimensions.items()))
        return f"{name}|{dims}" if dims else name

# ============================================================================
# Metric Collectors
# ============================================================================

class MetricCollector:
    """Base collector class"""
    
    def __init__(self, store: MetricStore):
        self.store = store
        self.running = False
    
    def collect(self):
        """Collect metrics - to be overridden"""
        raise NotImplementedError
    
    def run(self, interval: int = 10):
        """Run collector in loop"""
        self.running = True
        logger.info(f"Starting {self.__class__.__name__} (interval={interval}s)")
        
        while self.running:
            try:
                self.collect()
            except Exception as e:
                logger.error(f"Collector error: {e}")
            time.sleep(interval)
    
    def stop(self):
        self.running = False

class MQTTCollector(MetricCollector):
    """Collect MQTT metrics"""
    
    def __init__(self, store: MetricStore, host='localhost', port=1883):
        super().__init__(store)
        self.host = host
        self.port = port
    
    def collect(self):
        try:
            # Check if MQTT is up
            client = mqtt.Client()
            start = time.time()
            connected = False
            
            try:
                client.connect(self.host, self.port, timeout=2)
                connected = True
                client.disconnect()
            except Exception:
                pass
            
            response_time = time.time() - start
            
            # Store metrics
            self.store.add_metric(Metric(
                name="mqtt_health",
                value=1.0 if connected else 0.0,
                timestamp=datetime.utcnow(),
                dimensions={"component": "mqtt", "host": self.host}
            ))
            
            self.store.add_metric(Metric(
                name="mqtt_response_time",
                value=response_time,
                timestamp=datetime.utcnow(),
                dimensions={"component": "mqtt", "host": self.host},
                unit="Seconds"
            ))
            
            logger.debug(f"MQTT: {'UP' if connected else 'DOWN'} (response: {response_time:.3f}s)")
            
        except Exception as e:
            logger.error(f"MQTT collector error: {e}")

class RedisCollector(MetricCollector):
    """Collect Redis metrics"""
    
    def __init__(self, store: MetricStore, host='localhost', port=6379):
        super().__init__(store)
        self.host = host
        self.port = port
    
    def collect(self):
        try:
            r = redis.Redis(host=self.host, port=self.port, socket_timeout=2)
            
            # Check connection
            try:
                r.ping()
                connected = True
            except Exception:
                connected = False
            
            self.store.add_metric(Metric(
                name="redis_health",
                value=1.0 if connected else 0.0,
                timestamp=datetime.utcnow(),
                dimensions={"component": "redis", "host": self.host}
            ))
            
            if connected:
                # Get queue sizes
                queues = ['high', 'default', 'low']
                for queue in queues:
                    size = r.llen(f'rq:queue:{queue}')
                    self.store.add_metric(Metric(
                        name="redis_queue_size",
                        value=size,
                        timestamp=datetime.utcnow(),
                        dimensions={"queue": queue},
                        unit="Count"
                    ))
                
                # Total queue size
                total = sum(r.llen(f'rq:queue:{q}') for q in queues)
                self.store.add_metric(Metric(
                    name="redis_total_queue_size",
                    value=total,
                    timestamp=datetime.utcnow(),
                    dimensions={"component": "redis"},
                    unit="Count"
                ))
                
                logger.debug(f"Redis: UP (queue: {total})")
            else:
                logger.warning("Redis: DOWN")
                
        except Exception as e:
            logger.error(f"Redis collector error: {e}")

class PostgresCollector(MetricCollector):
    """Collect PostgreSQL metrics"""
    
    def __init__(self, store: MetricStore, host='localhost', port=5432,
                 db='oxygen_data', user='pipeline', password='pipeline123'):
        super().__init__(store)
        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.password = password
    
    def collect(self):
        try:
            conn = None
            try:
                conn = psycopg2.connect(
                    host=self.host, port=self.port, database=self.db,
                    user=self.user, password=self.password,
                    connect_timeout=2
                )
                connected = True
            except Exception:
                connected = False
            
            self.store.add_metric(Metric(
                name="postgres_health",
                value=1.0 if connected else 0.0,
                timestamp=datetime.utcnow(),
                dimensions={"component": "postgres", "host": self.host}
            ))
            
            if connected:
                cur = conn.cursor()
                
                # Get record count
                cur.execute("SELECT COUNT(*) FROM oxygen_readings")
                count = cur.fetchone()[0]
                self.store.add_metric(Metric(
                    name="postgres_record_count",
                    value=count,
                    timestamp=datetime.utcnow(),
                    dimensions={"component": "postgres"},
                    unit="Count"
                ))
                
                # Get recent anomalies
                cur.execute("""
                    SELECT COUNT(*) FROM oxygen_readings 
                    WHERE is_anomaly = true 
                    AND received_at > NOW() - INTERVAL '5 minutes'
                """)
                anomalies = cur.fetchone()[0]
                self.store.add_metric(Metric(
                    name="postgres_recent_anomalies",
                    value=anomalies,
                    timestamp=datetime.utcnow(),
                    dimensions={"component": "postgres"},
                    unit="Count"
                ))
                
                cur.close()
                conn.close()
                logger.debug(f"PostgreSQL: UP (records: {count}, anomalies: {anomalies})")
            else:
                logger.warning("PostgreSQL: DOWN")
                
        except Exception as e:
            logger.error(f"PostgreSQL collector error: {e}")

class GeneratorCollector(MetricCollector):
    """Collect generator metrics via MQTT"""
    
    def __init__(self, store: MetricStore, host='localhost', port=1883):
        super().__init__(store)
        self.host = host
        self.port = port
        self.last_heartbeat = datetime.min
    
    def collect(self):
        try:
            # Check if generator is publishing
            client = mqtt.Client()
            client.connect(self.host, self.port, timeout=2)
            
            # Subscribe and check for messages
            received = threading.Event()
            
            def on_message(client, userdata, msg):
                received.set()
            
            client.on_message = on_message
            client.subscribe('oxygen/sensors/data')
            client.loop_start()
            
            # Wait for message
            if received.wait(timeout=2):
                self.last_heartbeat = datetime.utcnow()
                health = 1.0
            else:
                health = 0.0 if (datetime.utcnow() - self.last_heartbeat) > timedelta(seconds=30) else 1.0
            
            client.loop_stop()
            client.disconnect()
            
            self.store.add_metric(Metric(
                name="generator_health",
                value=health,
                timestamp=datetime.utcnow(),
                dimensions={"component": "generator"},
                unit="Boolean"
            ))
            
            logger.debug(f"Generator: {'UP' if health > 0 else 'DOWN'}")
            
        except Exception as e:
            logger.error(f"Generator collector error: {e}")

class WorkerCollector(MetricCollector):
    """Collect worker metrics via HTTP"""
    
    def __init__(self, store: MetricStore, host='localhost', port=8000):
        super().__init__(store)
        self.host = host
        self.port = port
        self.metrics_url = f'http://{host}:{port}/metrics'
    
    def collect(self):
        try:
            response = requests.get(self.metrics_url, timeout=2)
            
            if response.status_code == 200:
                connected = 1.0
                
                # Parse metrics (simple text format)
                for line in response.text.split('\n'):
                    if line.startswith('sensor_processed_total'):
                        # Example: sensor_processed_total{status="success"} 123
                        parts = line.split()
                        if len(parts) == 2:
                            self.store.add_metric(Metric(
                                name="worker_processed_total",
                                value=float(parts[1]),
                                timestamp=datetime.utcnow(),
                                dimensions={"component": "worker"},
                                unit="Count"
                            ))
            else:
                connected = 0.0
            
            self.store.add_metric(Metric(
                name="worker_health",
                value=connected,
                timestamp=datetime.utcnow(),
                dimensions={"component": "worker"},
                unit="Boolean"
            ))
            
            logger.debug(f"Worker: {'UP' if connected > 0 else 'DOWN'}")
            
        except Exception as e:
            logger.error(f"Worker collector error: {e}")

# ============================================================================
# Alarm Engine
# ============================================================================

class AlarmEngine:
    """Evaluate metrics and trigger alarms"""
    
    def __init__(self, store: MetricStore, notifier):
        self.store = store
        self.notifier = notifier
        self.alarms: Dict[str, Alarm] = {}
        self.alarm_history: Dict[str, List[datetime]] = {}
    
    def add_alarm(self, alarm: Alarm):
        """Add an alarm configuration"""
        self.alarms[alarm.name] = alarm
        self.alarm_history[alarm.name] = []
        logger.info(f"Added alarm: {alarm.name}")
    
    def evaluate(self):
        """Evaluate all alarms"""
        now = datetime.utcnow()
        
        for name, alarm in self.alarms.items():
            # Get metric value
            value = self.store.get_metrics(
                name=alarm.metric,
                dimensions=alarm.dimensions,
                period=alarm.period,
                statistic=alarm.statistic
            )
            
            if value is None:
                logger.debug(f"Alarm {name}: No data for {alarm.metric}")
                continue
            
            # Check threshold
            triggered = self._compare(value, alarm.threshold, alarm.comparison)
            
            # Update state
            old_state = alarm.current_state
            alarm.current_state = "ALARM" if triggered else "OK"
            alarm.last_evaluation = now
            
            # Track history
            if triggered:
                self.alarm_history[name].append(now)
            
            # Handle state change
            if old_state != alarm.current_state:
                alarm.state_changed = True
                logger.info(f"Alarm {name}: {old_state} -> {alarm.current_state}")
                
                # Trigger actions
                if alarm.current_state == "ALARM":
                    self._handle_alarm(alarm, value)
                else:
                    self._handle_ok(alarm, value)
            else:
                alarm.state_changed = False
            
            # Check if alarm has been firing too long
            if alarm.current_state == "ALARM":
                recent_fires = [
                    t for t in self.alarm_history[name]
                    if t > now - timedelta(minutes=5)
                ]
                if len(recent_fires) > 3:
                    logger.warning(f"Alarm {name}: Frequent firing")
    
    def _compare(self, value: float, threshold: float, comparison: str) -> bool:
        """Compare value against threshold"""
        if comparison == "gt":
            return value > threshold
        elif comparison == "lt":
            return value < threshold
        elif comparison == "gte":
            return value >= threshold
        elif comparison == "lte":
            return value <= threshold
        elif comparison == "eq":
            return value == threshold
        return False
    
    def _handle_alarm(self, alarm: Alarm, value: float):
        """Handle alarm trigger"""
        # Create notification
        notification = Notification(
            alarm_name=alarm.name,
            state="ALARM",
            metric=alarm.metric,
            value=value,
            threshold=alarm.threshold,
            timestamp=datetime.utcnow(),
            dimensions=alarm.dimensions
        )
        
        # Send notifications
        self.notifier.notify(notification)
    
    def _handle_ok(self, alarm: Alarm, value: float):
        """Handle alarm resolution"""
        notification = Notification(
            alarm_name=alarm.name,
            state="OK",
            metric=alarm.metric,
            value=value,
            threshold=alarm.threshold,
            timestamp=datetime.utcnow(),
            dimensions=alarm.dimensions
        )
        
        self.notifier.notify(notification)

# ============================================================================
# Notifiers
# ============================================================================

class Notifier:
    """Base notifier class"""
    
    def notify(self, notification: Notification):
        """Send notification - to be overridden"""
        raise NotImplementedError

class ConsoleNotifier(Notifier):
    """Print to console"""
    
    def notify(self, notification: Notification):
        msg = f"""
╔═══════════════════════════════════════════════════════════╗
║ ALARM: {notification.alarm_name}
║ STATE: {notification.state}
║ METRIC: {notification.metric} = {notification.value:.2f}
║ THRESHOLD: {notification.threshold}
║ TIME: {notification.timestamp.isoformat()}
╚═══════════════════════════════════════════════════════════╝
        """
        if notification.state == "ALARM":
            logger.error(msg)
        else:
            logger.info(msg)

class MQTTNotifier(Notifier):
    """Send via MQTT"""
    
    def __init__(self, host='localhost', port=1883):
        self.client = mqtt.Client()
        self.client.connect(host, port, 60)
    
    def notify(self, notification: Notification):
        payload = json.dumps(asdict(notification), default=str)
        self.client.publish('alerts/notifications', payload)

class WebhookNotifier(Notifier):
    """Send via webhook"""
    
    def __init__(self, url: str, headers: Dict = None):
        self.url = url
        self.headers = headers or {}
    
    def notify(self, notification: Notification):
        try:
            requests.post(
                self.url,
                json=asdict(notification, default=str),
                headers=self.headers,
                timeout=2
            )
        except Exception as e:
            logger.error(f"Webhook notification failed: {e}")

class EmailNotifier(Notifier):
    """Send via email (using SMTP)"""
    
    def __init__(self, smtp_host='localhost', port=25, from_addr='monitor@local',
                 to_addrs=None):
        self.smtp_host = smtp_host
        self.port = port
        self.from_addr = from_addr
        self.to_addrs = to_addrs or []
    
    def notify(self, notification: Notification):
        try:
            import smtplib
            from email.mime.text import MIMEText
            
            subject = f"[{notification.state}] {notification.alarm_name}"
            body = f"""
Alarm: {notification.alarm_name}
State: {notification.state}
Metric: {notification.metric}
Value: {notification.value:.2f}
Threshold: {notification.threshold}
Time: {notification.timestamp.isoformat()}
Dimensions: {notification.dimensions}
            """
            
            msg = MIMEText(body)
            msg['Subject'] = subject
            msg['From'] = self.from_addr
            msg['To'] = ', '.join(self.to_addrs)
            
            with smtplib.SMTP(self.smtp_host, self.port) as server:
                server.send_message(msg)
                
            logger.info(f"Email notification sent to {self.to_addrs}")
            
        except Exception as e:
            logger.error(f"Email notification failed: {e}")

class SlackNotifier(Notifier):
    """Send via Slack webhook"""
    
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
        self.colors = {
            'ALARM': 'danger',
            'OK': 'good'
        }
    
    def notify(self, notification: Notification):
        try:
            payload = {
                'attachments': [{
                    'color': self.colors.get(notification.state, 'warning'),
                    'title': notification.alarm_name,
                    'fields': [
                        {'title': 'State', 'value': notification.state, 'short': True},
                        {'title': 'Metric', 'value': notification.metric, 'short': True},
                        {'title': 'Value', 'value': f"{notification.value:.2f}", 'short': True},
                        {'title': 'Threshold', 'value': str(notification.threshold), 'short': True},
                        {'title': 'Dimensions', 'value': str(notification.dimensions), 'short': False},
                        {'title': 'Time', 'value': notification.timestamp.isoformat(), 'short': False}
                    ]
                }]
            }
            
            requests.post(self.webhook_url, json=payload, timeout=2)
            logger.info(f"Slack notification sent")
            
        except Exception as e:
            logger.error(f"Slack notification failed: {e}")

# ============================================================================
# Multi-Notifier
# ============================================================================

class MultiNotifier(Notifier):
    """Send to multiple notifiers"""
    
    def __init__(self, notifiers: List[Notifier]):
        self.notifiers = notifiers
    
    def notify(self, notification: Notification):
        for notifier in self.notifiers:
            try:
                notifier.notify(notification)
            except Exception as e:
                logger.error(f"Notifier {type(notifier).__name__} failed: {e}")

# ============================================================================
# Main Monitor
# ============================================================================

class Monitor:
    """Main monitoring service"""
    
    def __init__(self, collection_interval: int = 10,
                 alarm_interval: int = 30):
        self.collection_interval = collection_interval
        self.alarm_interval = alarm_interval
        
        # Setup store
        self.store = MetricStore(retention_minutes=60)
        
        # Setup notifiers
        console = ConsoleNotifier()
        mqtt = MQTTNotifier()
        self.notifier = MultiNotifier([console, mqtt])
        
        # Setup collectors
        self.collectors = [
            MQTTCollector(self.store),
            RedisCollector(self.store),
            PostgresCollector(self.store),
            GeneratorCollector(self.store),
            WorkerCollector(self.store),
        ]
        
        # Setup alarm engine
        self.alarm_engine = AlarmEngine(self.store, self.notifier)
        self._setup_alarms()
        
        self.running = False
        self.collector_threads = []
    
    def _setup_alarms(self):
        """Configure default alarms"""
        alarms = [
            # Component health alarms
            Alarm(
                name="mqtt_down",
                metric="mqtt_health",
                statistic="avg",
                period=60,
                threshold=0.5,
                comparison="lt",
                dimensions={"component": "mqtt"},
                actions=["notify"]
            ),
            Alarm(
                name="redis_down",
                metric="redis_health",
                statistic="avg",
                period=60,
                threshold=0.5,
                comparison="lt",
                dimensions={"component": "redis"},
                actions=["notify"]
            ),
            Alarm(
                name="postgres_down",
                metric="postgres_health",
                statistic="avg",
                period=60,
                threshold=0.5,
                comparison="lt",
                dimensions={"component": "postgres"},
                actions=["notify"]
            ),
            Alarm(
                name="generator_down",
                metric="generator_health",
                statistic="avg",
                period=60,
                threshold=0.5,
                comparison="lt",
                dimensions={"component": "generator"},
                actions=["notify"]
            ),
            Alarm(
                name="worker_down",
                metric="worker_health",
                statistic="avg",
                period=60,
                threshold=0.5,
                comparison="lt",
                dimensions={"component": "worker"},
                actions=["notify"]
            ),
            
            # Queue alerts
            Alarm(
                name="queue_backlog",
                metric="redis_total_queue_size",
                statistic="avg",
                period=60,
                threshold=100,
                comparison="gt",
                dimensions={"component": "redis"},
                actions=["notify"]
            ),
            
            # Data alerts
            Alarm(
                name="high_anomaly_rate",
                metric="postgres_recent_anomalies",
                statistic="sum",
                period=300,
                threshold=10,
                comparison="gt",
                dimensions={"component": "postgres"},
                actions=["notify"]
            ),
            
            # Performance alerts
            Alarm(
                name="slow_mqtt",
                metric="mqtt_response_time",
                statistic="avg",
                period=60,
                threshold=0.5,
                comparison="gt",
                dimensions={"component": "mqtt"},
                actions=["notify"]
            ),
        ]
        
        for alarm in alarms:
            self.alarm_engine.add_alarm(alarm)
    
    def run(self):
        """Run the monitor"""
        self.running = True
        logger.info("🚀 Starting monitor service")
        logger.info(f"   Collection interval: {self.collection_interval}s")
        logger.info(f"   Alarm evaluation: {self.alarm_interval}s")
        
        # Start collectors
        logger.info(f"Starting {len(self.collectors)} collectors...")
        for collector in self.collectors:
            t = threading.Thread(
                target=collector.run,
                args=(self.collection_interval,),
                daemon=True
            )
            t.start()
            self.collector_threads.append(t)
            time.sleep(0.5)  # Stagger start
        
        # Main loop
        last_alarm_eval = datetime.min
        
        try:
            while self.running:
                # Evaluate alarms at specified interval
                now = datetime.utcnow()
                if (now - last_alarm_eval).total_seconds() >= self.alarm_interval:
                    self.alarm_engine.evaluate()
                    last_alarm_eval = now
                
                # Print summary
                self._print_summary()
                
                time.sleep(5)
                
        except KeyboardInterrupt:
            logger.info("🛑 Stopping monitor...")
        finally:
            self.stop()
    
    def _print_summary(self):
        """Print current status"""
        # Get key metrics
        mqtt = self.store.get_metrics("mqtt_health", {"component": "mqtt"}, 10, "avg")
        redis = self.store.get_metrics("redis_health", {"component": "redis"}, 10, "avg")
        postgres = self.store.get_metrics("postgres_health", {"component": "postgres"}, 10, "avg")
        queue = self.store.get_metrics("redis_total_queue_size", {"component": "redis"}, 10, "avg")
        anomalies = self.store.get_metrics("postgres_recent_anomalies", {"component": "postgres"}, 300, "sum")
        
        status = [
            f"MQTT: {'✅ UP' if mqtt and mqtt > 0 else '❌ DOWN'}",
            f"Redis: {'✅ UP' if redis and redis > 0 else '❌ DOWN'}",
            f"Postgres: {'✅ UP' if postgres and postgres > 0 else '❌ DOWN'}",
            f"Queue: {int(queue or 0)} jobs",
            f"Anomalies: {int(anomalies or 0)} in 5min"
        ]
        
        logger.info("📊 " + " | ".join(status))
    
    def stop(self):
        """Stop monitor and all collectors"""
        self.running = False
        for collector in self.collectors:
            collector.stop()
        
        # Wait for threads
        for t in self.collector_threads:
            t.join(timeout=2)
        
        logger.info("✅ Monitor stopped")

# ============================================================================
# CLI Entry Point
# ============================================================================

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="CloudWatch-style monitor")
    parser.add_argument("--collect-interval", type=int, default=10,
                       help="Metric collection interval (seconds)")
    parser.add_argument("--alarm-interval", type=int, default=30,
                       help="Alarm evaluation interval (seconds)")
    parser.add_argument("--config", help="Config file path")
    
    args = parser.parse_args()
    
    monitor = Monitor(
        collection_interval=args.collect_interval,
        alarm_interval=args.alarm_interval
    )
    
    try:
        monitor.run()
    except KeyboardInterrupt:
        logger.info("Shutting down...")

if __name__ == "__main__":
    main()