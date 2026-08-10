#!/usr/bin/env python3
"""
Monitor Redis Streams and DLQ
"""
import json
import time
from redis import Redis
from datetime import datetime
import os

REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379')
STREAM_NAME = os.getenv('STREAM_NAME', 'sensor:stream')
DLQ_STREAM = os.getenv('DLQ_STREAM', 'sensor:dlq')
CONSUMER_GROUP = os.getenv('CONSUMER_GROUP', 'sensor-group')

def monitor():
    r = Redis.from_url(REDIS_URL, decode_responses=True)
    
    print("\n" + "=" * 70)
    print(f"{'Queue Monitor':^70}")
    print("=" * 70)
    
    try:
        info = r.xinfo_stream(STREAM_NAME)
        print(f"\n📌 Main Stream: {STREAM_NAME}")
        print(f"   Length: {info.get('length', 0):,}")
        print(f"   Pending: {info.get('pending', 0):,}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    try:
        dlq_len = r.xlen(DLQ_STREAM)
        print(f"\n📌 DLQ: {DLQ_STREAM}")
        print(f"   Length: {dlq_len:,}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    try:
        groups = r.xinfo_groups(STREAM_NAME)
        for group in groups:
            print(f"\n📌 Consumer Group: {group.get('name')}")
            print(f"   Consumers: {group.get('consumers', 0)}")
            print(f"   Pending: {group.get('pending', 0):,}")
    except Exception:
        pass
    
    print("\n" + "=" * 70)

if __name__ == "__main__":
    while True:
        monitor()
        time.sleep(10)