#!/usr/bin/env python3
"""
Monitor Redis Streams and DLQ
"""
import json
import time
from redis import Redis
from datetime import datetime, timedelta
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
    
    # Main stream info
    print(f"\n📌 Main Stream: {STREAM_NAME}")
    try:
        info = r.xinfo_stream(STREAM_NAME)
        print(f"   Length: {info.get('length', 0):,}")
        print(f"   Pending: {info.get('pending', 0):,}")
        print(f"   Last Generated: {info.get('last-generated-id', 'N/A')}")
        print(f"   First Entry: {info.get('first-entry', ['N/A'])[0] if info.get('first-entry') else 'N/A'}")
        print(f"   Last Entry: {info.get('last-entry', ['N/A'])[0] if info.get('last-entry') else 'N/A'}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    # DLQ info
    print(f"\n📌 DLQ: {DLQ_STREAM}")
    try:
        dlq_len = r.xlen(DLQ_STREAM)
        print(f"   Length: {dlq_len:,}")
        
        # Show oldest DLQ message
        if dlq_len > 0:
            entries = r.xrevrange(DLQ_STREAM, '+', '-', count=1)
            if entries:
                msg_id, fields = entries[0]
                data = json.loads(fields.get('data', '{}'))
                print(f"   Oldest Message: {msg_id}")
                print(f"   Error: {fields.get('error', 'N/A')}")
                print(f"   Failed At: {fields.get('failed_at', 'N/A')}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    # Consumer group info
    print(f"\n📌 Consumer Group: {CONSUMER_GROUP}")
    try:
        groups = r.xinfo_groups(STREAM_NAME)
        for group in groups:
            print(f"   Consumers: {group.get('consumers', 0)}")
            print(f"   Pending: {group.get('pending', 0):,}")
            print(f"   Last Delivered: {group.get('last-delivered-id', 'N/A')}")
            
            # Show pending messages age
            if group.get('pending', 0) > 0:
                pending = r.xpending(STREAM_NAME, CONSUMER_GROUP, '-', '+', 5)
                if pending:
                    print(f"   Pending Sample:")
                    for p in pending:
                        print(f"      - {p['message_id']} (IDLE: {p.get('idle', 0)}ms)")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    # Processing rate
    print(f"\n📊 Processing Stats:")
    try:
        # Count messages processed in last minute
        minute_ago = datetime.utcnow() - timedelta(minutes=1)
        # This is a rough estimate - you might want more sophisticated tracking
        print(f"   To track rates, use Prometheus metrics")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    print("\n" + "=" * 70)

if __name__ == "__main__":
    while True:
        monitor()
        time.sleep(10)