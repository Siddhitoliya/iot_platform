#!/usr/bin/env python3
"""
Redis Streams processor with DLQ support
"""
import os
import json
import time
import logging
import signal
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import redis
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# Configuration
# ============================================================================

REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379')
STREAM_NAME = os.getenv('STREAM_NAME', 'sensor:stream')
DLQ_STREAM = os.getenv('DLQ_STREAM', 'sensor:dlq')
CONSUMER_GROUP = os.getenv('CONSUMER_GROUP', 'sensor-group')
CONSUMER_NAME = os.getenv('CONSUMER_NAME', f'worker-{os.getpid()}')
MAX_RETRIES = int(os.getenv('MAX_RETRIES', 3))
RETRY_DELAY = int(os.getenv('RETRY_DELAY', 30))
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 10))
BLOCK_TIMEOUT = int(os.getenv('BLOCK_TIMEOUT', 5000))

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'oxygen_data'),
    'user': os.getenv('DB_USER', 'pipeline'),
    'password': os.getenv('DB_PASSWORD', 'pipeline123')
}

# ============================================================================
# Redis Streams Processor
# ============================================================================

class StreamProcessor:
    def __init__(self, redis_url: str, stream: str, dlq: str, consumer_group: str,
                 consumer_name: str):
        self.redis = redis.from_url(redis_url, decode_responses=True)
        self.stream = stream
        self.dlq = dlq
        self.consumer_group = consumer_group
        self.consumer_name = consumer_name
        self.running = False
        self.stats = {'processed': 0, 'errors': 0, 'dlq_sent': 0, 'retries': 0}
        self._create_consumer_group()
    
    def _create_consumer_group(self):
        try:
            if not self.redis.exists(self.stream):
                self.redis.xadd(self.stream, {'init': 'true'})
                self.redis.xdel(self.stream, ['0-1'])
            
            self.redis.xgroup_create(
                self.stream,
                self.consumer_group,
                id='0',
                mkstream=True
            )
            logger.info(f"✅ Created consumer group: {self.consumer_group}")
        except redis.exceptions.ResponseError as e:
            if 'BUSYGROUP' in str(e):
                logger.info(f"✅ Consumer group already exists: {self.consumer_group}")
            else:
                logger.error(f"❌ Failed to create consumer group: {e}")
    
    def read_messages(self, count: int = 10, block: int = 5000) -> list:
        try:
            results = self.redis.xreadgroup(
                self.consumer_group,
                self.consumer_name,
                {self.stream: '>'},
                count=count,
                block=block
            )
            
            messages = []
            if results:
                for stream_name, entries in results:
                    for msg_id, fields in entries:
                        messages.append({
                            'id': msg_id,
                            'stream': stream_name,
                            'data': json.loads(fields.get('data', '{}')),
                            'retry_count': int(fields.get('retry_count', '0')),
                            'first_seen': fields.get('first_seen', '')
                        })
            return messages
        except Exception as e:
            logger.error(f"❌ Failed to read messages: {e}")
            return []
    
    def acknowledge(self, msg_id: str):
        try:
            self.redis.xack(self.stream, self.consumer_group, msg_id)
        except Exception as e:
            logger.error(f"❌ Failed to acknowledge: {e}")
    
    def move_to_dlq(self, msg_id: str, message: Dict[str, Any], 
                    error: str, retry_count: int):
        try:
            self.redis.xadd(
                self.dlq,
                {
                    'data': json.dumps(message),
                    'original_id': msg_id,
                    'retry_count': str(retry_count),
                    'error': error,
                    'failed_at': datetime.utcnow().isoformat(),
                    'status': 'failed'
                }
            )
            self.redis.xack(self.stream, self.consumer_group, msg_id)
            self.stats['dlq_sent'] += 1
            logger.info(f"📦 Moved to DLQ: {msg_id} (retries: {retry_count})")
        except Exception as e:
            logger.error(f"❌ Failed to move to DLQ: {e}")
    
    def get_dlq_size(self) -> int:
        try:
            return self.redis.xlen(self.dlq)
        except Exception:
            return 0

# ============================================================================
# Processing Functions
# ============================================================================

def get_db_connection():
    try:
        return psycopg2.connect(**DB_CONFIG, connect_timeout=10)
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        raise

def process_message(message: Dict[str, Any]) -> bool:
    try:
        data = message.get('data', {})
        
        conn = get_db_connection()
        try:
            cur = conn.cursor()
            
            sql = """
                INSERT INTO oxygen_readings (
                    device_id, hospital_id, facility_zone,
                    liquid_level_percent, liquid_volume_liters,
                    pipeline_pressure_bar, gas_purity_percentage,
                    active_error_codes, event_time, is_anomaly
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            values = (
                data.get('system_metadata', {}).get('device_id', 'unknown'),
                data.get('system_metadata', {}).get('hospital_id', 'unknown'),
                data.get('system_metadata', {}).get('facility_zone', ''),
                data.get('tank_metrics', {}).get('liquid_level_percent', 0),
                data.get('tank_metrics', {}).get('liquid_volume_liters', 0),
                data.get('pipeline_distribution', {}).get('pipeline_pressure_bar', 0),
                data.get('pipeline_distribution', {}).get('gas_purity_percentage', 0),
                json.dumps(data.get('system_health', {}).get('active_error_codes', [])),
                data.get('timestamp', datetime.utcnow().isoformat()),
                len(data.get('system_health', {}).get('active_error_codes', [])) > 0
            )
            
            cur.execute(sql, values)
            conn.commit()
            cur.close()
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"❌ Database error: {e}")
            if conn:
                conn.rollback()
                conn.close()
            return False
            
    except Exception as e:
        logger.error(f"❌ Processing error: {e}")
        return False

# ============================================================================
# Worker
# ============================================================================

class Worker:
    def __init__(self):
        self.processor = StreamProcessor(
            redis_url=REDIS_URL,
            stream=STREAM_NAME,
            dlq=DLQ_STREAM,
            consumer_group=CONSUMER_GROUP,
            consumer_name=CONSUMER_NAME
        )
        self.running = False
    
    def _process_batch(self, messages: list):
        for msg in messages:
            msg_id = msg['id']
            msg_data = msg['data']
            retry_count = msg['retry_count']
            
            success = process_message(msg_data)
            
            if success:
                self.processor.acknowledge(msg_id)
                self.processor.stats['processed'] += 1
                logger.info(f"✅ Processed: {msg_id}")
            else:
                retry_count += 1
                if retry_count >= MAX_RETRIES:
                    self.processor.move_to_dlq(msg_id, msg_data, "Max retries exceeded", retry_count)
                else:
                    try:
                        entries = self.processor.redis.xrange(STREAM_NAME, msg_id, msg_id)
                        if entries:
                            _, fields = entries[0]
                            fields['retry_count'] = str(retry_count)
                            self.processor.redis.xadd(STREAM_NAME, fields, id=msg_id)
                            self.processor.stats['retries'] += 1
                            logger.info(f"🔄 Retry: {msg_id} (attempt {retry_count})")
                    except Exception as e:
                        logger.error(f"❌ Failed to update retry: {e}")
    
    def run(self):
        self.running = True
        logger.info(f"🚀 Starting worker: {CONSUMER_NAME}")
        logger.info(f"   Stream: {STREAM_NAME}")
        logger.info(f"   Consumer Group: {CONSUMER_GROUP}")
        logger.info(f"   Max Retries: {MAX_RETRIES}")
        
        while self.running:
            try:
                messages = self.processor.read_messages(count=BATCH_SIZE, block=BLOCK_TIMEOUT)
                if messages:
                    self._process_batch(messages)
                    logger.info(f"📊 Stats: {self.processor.stats}")
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"❌ Worker error: {e}")
                time.sleep(1)

# ============================================================================
# Replay Service
# ============================================================================

class ReplayService:
    def __init__(self, redis_url: str, dlq_stream: str, main_stream: str):
        self.redis = redis.from_url(redis_url, decode_responses=True)
        self.dlq = dlq_stream
        self.main_stream = main_stream
        self.running = False
    
    def check_health(self) -> bool:
        try:
            conn = psycopg2.connect(**DB_CONFIG, connect_timeout=2)
            cur = conn.cursor()
            cur.execute('SELECT 1')
            cur.close()
            conn.close()
            return True
        except Exception:
            return False
    
    def replay_one(self) -> bool:
        try:
            entries = self.redis.xrevrange(self.dlq, '+', '-', count=1)
            if not entries:
                return False
            
            msg_id, fields = entries[0]
            data = fields.get('data')
            if data:
                self.redis.xadd(
                    self.main_stream,
                    {
                        'data': data,
                        'retry_count': '0',
                        'first_seen': datetime.utcnow().isoformat()
                    }
                )
                self.redis.xdel(self.dlq, msg_id)
                logger.info(f"🔄 Replayed from DLQ: {msg_id}")
                return True
            return False
        except Exception as e:
            logger.error(f"❌ Replay failed: {e}")
            return False
    
    def run(self, interval: int = 30, max_per_cycle: int = 10):
        self.running = True
        logger.info(f"🚀 Starting replay service (interval={interval}s)")
        
        while self.running:
            try:
                dlq_size = self.redis.xlen(self.dlq)
                if dlq_size == 0:
                    time.sleep(interval)
                    continue
                
                logger.info(f"📦 DLQ size: {dlq_size}")
                
                if not self.check_health():
                    logger.warning("⚠️ Downstream services unhealthy, waiting...")
                    time.sleep(interval)
                    continue
                
                replayed = 0
                while replayed < max_per_cycle and self.replay_one():
                    replayed += 1
                
                if replayed > 0:
                    logger.info(f"✅ Replayed {replayed} messages")
                
                time.sleep(interval)
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"❌ Replay service error: {e}")
                time.sleep(interval)

# ============================================================================
# CLI
# ============================================================================

def show_stats():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    
    print("\n📊 Queue Statistics")
    print("-" * 60)
    
    stream_info = r.xinfo_stream(STREAM_NAME)
    print(f"📌 Main Stream: {STREAM_NAME}")
    print(f"   Messages: {stream_info.get('length', 0)}")
    print(f"   Pending: {stream_info.get('pending', 0)}")
    
    dlq_info = r.xinfo_stream(DLQ_STREAM) if r.exists(DLQ_STREAM) else None
    print(f"\n📌 DLQ: {DLQ_STREAM}")
    if dlq_info:
        print(f"   Messages: {dlq_info.get('length', 0)}")
    else:
        print("   Empty")
    
    try:
        group_info = r.xinfo_groups(STREAM_NAME)
        for group in group_info:
            print(f"\n📌 Consumer Group: {group.get('name')}")
            print(f"   Consumers: {group.get('consumers', 0)}")
            print(f"   Pending: {group.get('pending', 0)}")
    except Exception:
        pass

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['worker', 'replay', 'stats'],
                        default='worker')
    args = parser.parse_args()
    
    if args.mode == 'worker':
        Worker().run()
    elif args.mode == 'replay':
        ReplayService(REDIS_URL, DLQ_STREAM, STREAM_NAME).run()
    else:
        show_stats()