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

def check_mqtt(host='localhost', port=1883):
    try:
        client = mqtt.Client()
        client.connect(host, port, timeout=2)
        client.disconnect()
        return True, "MQTT is up"
    except Exception as e:
        return False, f"MQTT down: {e}"

def check_redis(host='localhost', port=6379):
    try:
        r = redis.Redis(host=host, port=port, socket_timeout=2)
        r.ping()
        return True, "Redis is up"
    except Exception as e:
        return False, f"Redis down: {e}"

def check_postgres(host='localhost', port=5432, db='baby_oil_data', user='pipeline', password='pipeline123'):
    try:
        conn = psycopg2.connect(host=host, port=port, database=db, user=user, password=password, connect_timeout=2)
        cur = conn.cursor()
        cur.execute('SELECT 1')
        cur.close()
        conn.close()
        return True, "PostgreSQL is up"
    except Exception as e:
        return False, f"PostgreSQL down: {e}"

def main():
    status = {'timestamp': datetime.utcnow().isoformat() + 'Z', 'components': {}}
    all_healthy = True
    
    for name, check_func in [('mqtt', check_mqtt), ('redis', check_redis), ('postgres', check_postgres)]:
        healthy, message = check_func()
        status['components'][name] = {'healthy': healthy, 'message': message}
        if not healthy:
            all_healthy = False
    
    status['overall_health'] = all_healthy
    print(json.dumps(status, indent=2))
    exit(0 if all_healthy else 1)

if __name__ == "__main__":
    main()