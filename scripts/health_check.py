#!/usr/bin/env python3
"""
Health check for all pipeline components
"""
import socket
import paho.mqtt.client as mqtt
import redis
import psycopg2
import json
import os
from datetime import datetime
import requests

def check_mqtt(host='localhost', port=1883):
    """Check MQTT broker health"""
    try:
        client = mqtt.Client()
        client.connect(host, port, timeout=2)
        client.disconnect()
        return True, "MQTT broker is up"
    except Exception as e:
        return False, f"MQTT broker down: {e}"

def check_redis(host='localhost', port=6379):
    """Check Redis health"""
    try:
        r = redis.Redis(host=host, port=port, socket_timeout=2)
        r.ping()
        # Check queue length
        queue_length = r.llen('rq:queue:high')
        return True, f"Redis is up, queue length: {queue_length}"
    except Exception as e:
        return False, f"Redis down: {e}"

def check_postgres(host='localhost', port=5432, db='oxygen_data', user='pipeline', password='pipeline123'):
    """Check PostgreSQL health"""
    try:
        conn = psycopg2.connect(
            host=host, port=port, database=db,
            user=user, password=password, connect_timeout=2
        )
        cur = conn.cursor()
        cur.execute('SELECT 1')
        cur.close()
        conn.close()
        return True, "PostgreSQL is up"
    except Exception as e:
        return False, f"PostgreSQL down: {e}"

def check_prometheus(host='localhost', port=9090):
    """Check Prometheus health"""
    try:
        response = requests.get(f'http://{host}:{port}/-/healthy', timeout=2)
        if response.status_code == 200:
            return True, "Prometheus is up"
        return False, f"Prometheus returned: {response.status_code}"
    except Exception as e:
        return False, f"Prometheus down: {e}"

def check_grafana(host='localhost', port=3000):
    """Check Grafana health"""
    try:
        response = requests.get(f'http://{host}:{port}/api/health', timeout=2)
        if response.status_code == 200:
            return True, "Grafana is up"
        return False, f"Grafana returned: {response.status_code}"
    except Exception as e:
        return False, f"Grafana down: {e}"

def main():
    """Run all health checks"""
    status = {
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'components': {}
    }
    
    checks = [
        ('mqtt', check_mqtt),
        ('redis', check_redis),
        ('postgres', check_postgres),
        ('prometheus', check_prometheus),
        ('grafana', check_grafana)
    ]
    
    all_healthy = True
    for name, check_func in checks:
        healthy, message = check_func()
        status['components'][name] = {
            'healthy': healthy,
            'message': message
        }
        if not healthy:
            all_healthy = False
    
    status['overall_health'] = all_healthy
    
    # Print results
    print(json.dumps(status, indent=2))
    
    # Exit with appropriate code
    exit(0 if all_healthy else 1)

if __name__ == "__main__":
    main()