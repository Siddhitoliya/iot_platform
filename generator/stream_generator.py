#!/usr/bin/env python3
"""
Stream Generator for Baby Oil Dispenser
Publishes directly to Redis Stream
"""
import json
import time
import random
import argparse
import os
from datetime import datetime
from redis import Redis
import logging

from generator import BabyOilSimulator, generate_telemetry_payload

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StreamGenerator:
    def __init__(self, redis_url="redis://localhost:6379", stream="babyoil:stream"):
        self.redis = Redis.from_url(redis_url, decode_responses=True)
        self.stream = stream
        self.device_id = f"BOD-{random.randint(1000, 9999)}"
        self.user_id = f"USER-{random.randint(1000, 9999)}"
        self.sim = BabyOilSimulator(self.device_id, self.user_id)
    
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
        published = 0
        logger.info(f"📡 Starting stream generator")
        logger.info(f"   Stream: {self.stream}")
        logger.info(f"   Device: {self.device_id}")
        
        try:
            while True:
                if count > 0 and published >= count:
                    break
                
                # Generate various payloads
                events = ["weight_measured", "warming", "heating_tcc", "ready", "dispensing"]
                event = random.choice(events)
                payload = generate_telemetry_payload(event, self.device_id, self.user_id)
                
                # Add heartbeat occasionally
                if published % 6 == 0:
                    heartbeat = self.sim.generate_heartbeat()
                    self.publish(heartbeat)
                    logger.info(f"💓 Heartbeat: {heartbeat['heartbeat_id']}")
                
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
    parser.add_argument("--stream", default="babyoil:stream")
    parser.add_argument("--interval", type=int, default=5)
    parser.add_argument("--count", type=int, default=0)
    args = parser.parse_args()
    
    generator = StreamGenerator(redis_url=args.redis_url, stream=args.stream)
    generator.run(interval=args.interval, count=args.count)