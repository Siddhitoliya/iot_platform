#!/usr/bin/env python3
"""
Generator that sends data directly to Redis Stream
"""
import json
import time
import random
import argparse
import os
from datetime import datetime
from redis import Redis
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StreamGenerator:
    def __init__(self, redis_url="redis://localhost:6379", stream="sensor:stream"):
        self.redis = Redis.from_url(redis_url, decode_responses=True)
        self.stream = stream
    
    def generate_payload(self, device_id=None):
        if not device_id:
            device_id = f"O2P-{random.choice(['VIE','MIA'])}-ICU-{random.randint(1000,9999)}"
        
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "system_metadata": {
                "device_id": device_id,
                "hospital_id": f"HOSP-{random.choice(['FL','CA'])}-{random.randint(1000,9999)}",
                "facility_zone": "Main-ICU",
                "firmware_version": "v3.4.2"
            },
            "tank_metrics": {
                "liquid_level_percent": round(random.uniform(15, 95), 1),
                "liquid_volume_liters": round(random.uniform(1000, 10000), 1),
                "head_pressure_bar": round(random.uniform(10, 15), 1),
                "tank_temperature_celsius": round(random.uniform(-190, -180), 1)
            },
            "pipeline_distribution": {
                "output_flow_rate_m3_per_hour": round(random.uniform(50, 300), 1),
                "pipeline_pressure_bar": round(random.uniform(3.5, 5.5), 1),
                "gas_purity_percentage": round(random.uniform(99.2, 99.9), 1)
            },
            "system_health": {
                "battery_backup_percent": round(random.uniform(80, 100), 1),
                "power_source": "MAINS",
                "network_signal_dbm": random.randint(-80, -50),
                "active_error_codes": []
            }
        }
    
    def publish(self, message):
        try:
            msg_id = self.redis.xadd(
                self.stream,
                {
                    'data': json.dumps(message),
                    'retry_count': '0',
                    'first_seen': datetime.utcnow().isoformat()
                }
            )
            return msg_id
        except Exception as e:
            logger.error(f"Failed to publish: {e}")
            return None
    
    def run(self, interval=5, count=0):
        device_id = f"O2P-{random.choice(['VIE','MIA'])}-ICU-{random.randint(1000,9999)}"
        published = 0
        
        logger.info(f"📡 Starting stream generator")
        logger.info(f"   Stream: {self.stream}")
        print("Press Ctrl+C to stop")
        
        try:
            while True:
                if count > 0 and published >= count:
                    break
                
                payload = self.generate_payload(device_id)
                self.publish(payload)
                published += 1
                
                if published % 10 == 0:
                    logger.info(f"📊 Published {published} messages")
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("🛑 Stopping generator")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-url", default="redis://localhost:6379")
    parser.add_argument("--stream", default="sensor:stream")
    parser.add_argument("--interval", type=int, default=5)
    parser.add_argument("--count", type=int, default=0)
    args = parser.parse_args()
    
    generator = StreamGenerator(redis_url=args.redis_url, stream=args.stream)
    generator.run(interval=args.interval, count=args.count)