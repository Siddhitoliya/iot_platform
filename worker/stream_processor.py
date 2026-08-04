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

# Setup logging
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
RETRY_DELAY = int(os.getenv('RETRY_DELAY', 30))  # seconds
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 10))
BLOCK_TIMEOUT = int(os.getenv('BLOCK_TIMEOUT', 5000))  # milliseconds

# Database config
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'oxygen_data'),
    'user': os.getenv('DB_USER', 'pipeline'),
    'password': os.getenv('DB_PASSWORD', 'pipeline123')
}

# ============================================================================
# Redis Streams Wrapper
# ============================================================================

class StreamProcessor:
    """Redis Streams processor with DLQ support"""
    
    def __init__(self, redis_url: str, stream: str, dlq: str, consumer_group: str,
                 consumer_name: str):
        self.redis = redis.from_url(redis_url, decode_responses=True)
        self.stream = stream
        self.dlq = dlq
        self.consumer_group = consumer_group
        self.consumer_name = consumer_name
        self.running = False
        self.stats = {
            'processed': 0,
            'errors': 0,
            'dlq_sent': 0,
            'retries': 0
        }
        
        # Create consumer group if it doesn't exist
        self._create_consumer_group()
    
    def _create_consumer_group(self):
        """Create consumer group if not exists"""
        try:
            # Check if stream exists
            if not self.redis.exists(self.stream):
                # Create stream by adding a dummy message and deleting it
                self.redis.xadd(self.stream, {'init': 'true'})
                self.redis.xdel(self.stream, ['0-1'])
            
            # Try to create consumer group
            self.redis.xgroup_create(
                self.stream,
                self.consumer_group,
                id='0',  # Start from beginning
                mkstream=True
            )
            logger.info(f"✅ Created consumer group: {self.consumer_group}")
        except redis.exceptions.ResponseError as e:
            if 'BUSYGROUP' in str(e):
                logger.info(f"✅ Consumer group already exists: {self.consumer_group}")
            else:
                logger.error(f"❌ Failed to create consumer group: {e}")
                raise
    
    def add_message(self, message: Dict[str, Any]) -> str:
        """Add message to stream"""
        try:
            msg_id = self.redis.xadd(
                self.stream,
                {
                    'data': json.dumps(message),
                    'retry_count': '0',
                    'first_seen': datetime.utcnow().isoformat()
                }
            )
            logger.debug(f"📤 Message added to stream: {msg_id}")
            return msg_id
        except Exception as e:
            logger.error(f"❌ Failed to add message: {e}")
            raise
    
    def read_messages(self, count: int = 10, block: int = 5000) -> list:
        """Read messages from stream"""
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
        """Acknowledge successful processing"""
        try:
            self.redis.xack(self.stream, self.consumer_group, msg_id)
            logger.debug(f"✅ Acknowledged: {msg_id}")
        except Exception as e:
            logger.error(f"❌ Failed to acknowledge: {e}")
    
    def claim_pending(self, min_idle_ms: int = 30000, count: int = 10) -> list:
        """Claim pending messages from other dead consumers"""
        try:
            pending = self.redis.xpending(
                self.stream,
                self.consumer_group,
                '-',
                '+',
                count
            )
            
            if not pending:
                return []
            
            # Claim messages
            claimed = self.redis.xclaim(
                self.stream,
                self.consumer_group,
                self.consumer_name,
                min_idle_ms,
                [p['message_id'] for p in pending],
                {
                    'justid': False,
                    'count': count
                }
            )
            
            messages = []
            for msg_id, fields in claimed:
                messages.append({
                    'id': msg_id,
                    'stream': self.stream,
                    'data': json.loads(fields.get('data', '{}')),
                    'retry_count': int(fields.get('retry_count', '0')),
                    'first_seen': fields.get('first_seen', '')
                })
            
            return messages
        except Exception as e:
            logger.error(f"❌ Failed to claim pending: {e}")
            return []
    
    def move_to_dlq(self, msg_id: str, message: Dict[str, Any], 
                    error: str, retry_count: int):
        """Move failed message to DLQ"""
        try:
            # Add to DLQ stream
            dlq_id = self.redis.xadd(
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
            
            # Acknowledge and remove from main stream
            self.redis.xack(self.stream, self.consumer_group, msg_id)
            
            self.stats['dlq_sent'] += 1
            logger.info(f"📦 Moved to DLQ: {msg_id} -> {dlq_id} (retries: {retry_count})")
            
        except Exception as e:
            logger.error(f"❌ Failed to move to DLQ: {e}")
    
    def requeue_from_dlq(self, msg_id: str):
        """Requeue a message from DLQ back to main stream"""
        try:
            # Get message from DLQ
            entries = self.redis.xrange(self.dlq, msg_id, msg_id)
            if not entries:
                logger.error(f"❌ Message not found in DLQ: {msg_id}")
                return
            
            for _, fields in entries:
                # Re-add to main stream
                self.add_message(json.loads(fields.get('data', '{}')))
                
                # Remove from DLQ
                self.redis.xdel(self.dlq, msg_id)
                
                logger.info(f"🔄 Requeued from DLQ: {msg_id}")
                
        except Exception as e:
            logger.error(f"❌ Failed to requeue: {e}")
    
    def get_pending_count(self) -> int:
        """Get count of pending messages"""
        try:
            info = self.redis.xinfo_stream(self.stream)
            return info.get('pending', 0)
        except Exception:
            return 0
    
    def get_dlq_size(self) -> int:
        """Get DLQ size"""
        try:
            return self.redis.xlen(self.dlq)
        except Exception:
            return 0

# ============================================================================
# Processing Functions
# ============================================================================

def get_db_connection():
    """Get PostgreSQL connection"""
    try:
        return psycopg2.connect(**DB_CONFIG, connect_timeout=10)
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        raise

def process_message(message: Dict[str, Any]) -> bool:
    """
    Process a sensor reading
    Returns True on success, False on failure
    """
    try:
        # Extract payload
        data = message.get('data', {})
        
        # Flatten and insert into PostgreSQL
        conn = get_db_connection()
        try:
            cur = conn.cursor()
            
            # Simple insert (you can use the full schema from before)
            sql = """
                INSERT INTO oxygen_readings (
                    device_id, hospital_id, facility_zone,
                    liquid_level_percent, liquid_volume_liters,
                    pipeline_pressure_bar, gas_purity_percentage,
                    active_error_codes, event_time, is_anomaly
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            # Extract fields (simplified for example)
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
            
            logger.debug(f"✅ Processed: {data.get('system_metadata', {}).get('device_id')}")
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
# Replay Service
# ============================================================================

class ReplayService:
    """Service to replay messages from DLQ"""
    
    def __init__(self, redis_url: str, dlq_stream: str, main_stream: str):
        self.redis = redis.from_url(redis_url, decode_responses=True)
        self.dlq = dlq_stream
        self.main_stream = main_stream
        self.running = False
    
    def check_health(self) -> bool:
        """Check if downstream services are healthy"""
        # Check PostgreSQL
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
        """Replay one message from DLQ"""
        try:
            # Get oldest message from DLQ
            entries = self.redis.xrevrange(self.dlq, '+', '-', count=1)
            if not entries:
                return False
            
            msg_id, fields = entries[0]
            
            # Re-add to main stream
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
                
                # Remove from DLQ
                self.redis.xdel(self.dlq, msg_id)
                
                logger.info(f"🔄 Replayed from DLQ: {msg_id}")
                return True
                
        except Exception as e:
            logger.error(f"❌ Replay failed: {e}")
            return False
    
    def run(self, interval: int = 30, max_per_cycle: int = 10):
        """Main replay loop"""
        self.running = True
        logger.info(f"🚀 Starting replay service (interval={interval}s)")
        
        while self.running:
            try:
                # Check if DLQ has messages
                dlq_size = self.redis.xlen(self.dlq)
                if dlq_size == 0:
                    time.sleep(interval)
                    continue
                
                logger.info(f"📦 DLQ size: {dlq_size}")
                
                # Check health before replaying
                if not self.check_health():
                    logger.warning("⚠️ Downstream services unhealthy, waiting...")
                    time.sleep(interval)
                    continue
                
                # Replay messages
                replayed = 0
                while replayed < max_per_cycle and self.replay_one():
                    replayed += 1
                
                if replayed > 0:
                    logger.info(f"✅ Replayed {replayed} messages")
                
                time.sleep(interval)
                
            except KeyboardInterrupt:
                logger.info("🛑 Stopping replay service...")
                break
            except Exception as e:
                logger.error(f"❌ Replay service error: {e}")
                time.sleep(interval)

# ============================================================================
# Main Worker
# ============================================================================

class Worker:
    """Main worker that processes stream messages"""
    
    def __init__(self):
        self.processor = StreamProcessor(
            redis_url=REDIS_URL,
            stream=STREAM_NAME,
            dlq=DLQ_STREAM,
            consumer_group=CONSUMER_GROUP,
            consumer_name=CONSUMER_NAME
        )
        self.running = False
        self.stats = {
            'processed': 0,
            'errors': 0,
            'retries': 0
        }
    
    def _process_batch(self, messages: list):
        """Process a batch of messages"""
        for msg in messages:
            msg_id = msg['id']
            msg_data = msg['data']
            retry_count = msg['retry_count']
            
            logger.debug(f"📥 Processing: {msg_id} (retry: {retry_count})")
            
            # Process the message
            success = process_message(msg_data)
            
            if success:
                # Acknowledge successful processing
                self.processor.acknowledge(msg_id)
                self.stats['processed'] += 1
                logger.info(f"✅ Processed: {msg_id}")
                
            else:
                # Increment retry count
                retry_count += 1
                
                # Check if max retries reached
                if retry_count >= MAX_RETRIES:
                    # Move to DLQ
                    error = "Max retries exceeded"
                    self.processor.move_to_dlq(msg_id, msg_data, error, retry_count)
                    self.stats['dlq_sent'] += 1
                    logger.warning(f"📦 Moved to DLQ: {msg_id} (max retries)")
                    
                else:
                    # Update retry count in stream
                    try:
                        # Get current fields
                        entries = self.processor.redis.xrange(
                            STREAM_NAME, msg_id, msg_id
                        )
                        if entries:
                            _, fields = entries[0]
                            fields['retry_count'] = str(retry_count)
                            # Re-add with updated retry count
                            self.processor.redis.xadd(
                                STREAM_NAME,
                                fields,
                                id=msg_id
                            )
                            self.stats['retries'] += 1
                            logger.info(f"🔄 Retry: {msg_id} (attempt {retry_count})")
                    except Exception as e:
                        logger.error(f"❌ Failed to update retry: {e}")
    
    def run(self):
        """Main worker loop"""
        self.running = True
        logger.info(f"🚀 Starting worker: {CONSUMER_NAME}")
        logger.info(f"   Stream: {STREAM_NAME}")
        logger.info(f"   Consumer Group: {CONSUMER_GROUP}")
        logger.info(f"   Max Retries: {MAX_RETRIES}")
        logger.info(f"   Batch Size: {BATCH_SIZE}")
        logger.info("-" * 60)
        
        while self.running:
            try:
                # Read messages
                messages = self.processor.read_messages(
                    count=BATCH_SIZE,
                    block=BLOCK_TIMEOUT
                )
                
                if messages:
                    self._process_batch(messages)
                    
                    # Log stats
                    logger.info(f"📊 Stats: {self.stats['processed']} processed, "
                              f"{self.stats['errors']} errors, "
                              f"{self.stats['retries']} retries, "
                              f"{self.stats['dlq_sent']} in DLQ")
                
            except KeyboardInterrupt:
                logger.info("🛑 Stopping worker...")
                break
            except Exception as e:
                logger.error(f"❌ Worker error: {e}")
                time.sleep(1)
    
    def stop(self):
        self.running = False

# ============================================================================
# CLI Entry Points
# ============================================================================

def run_worker():
    """Run the main worker"""
    worker = Worker()
    worker.run()

def run_replay():
    """Run the replay service"""
    replay = ReplayService(
        redis_url=REDIS_URL,
        dlq_stream=DLQ_STREAM,
        main_stream=STREAM_NAME
    )
    replay.run()

def show_stats():
    """Show queue statistics"""
    r = redis.from_url(REDIS_URL, decode_responses=True)
    
    print("\n📊 Queue Statistics")
    print("-" * 60)
    
    # Main stream info
    stream_info = r.xinfo_stream(STREAM_NAME)
    print(f"📌 Main Stream: {STREAM_NAME}")
    print(f"   Messages: {stream_info.get('length', 0)}")
    print(f"   Pending: {stream_info.get('pending', 0)}")
    print(f"   Last Generated: {stream_info.get('last-generated-id', 'N/A')}")
    
    # DLQ info
    dlq_info = r.xinfo_stream(DLQ_STREAM) if r.exists(DLQ_STREAM) else None
    print(f"\n📌 DLQ: {DLQ_STREAM}")
    if dlq_info:
        print(f"   Messages: {dlq_info.get('length', 0)}")
    else:
        print("   Empty")
    
    # Consumer group info
    try:
        group_info = r.xinfo_groups(STREAM_NAME)
        for group in group_info:
            print(f"\n📌 Consumer Group: {group.get('name')}")
            print(f"   Consumers: {group.get('consumers', 0)}")
            print(f"   Pending: {group.get('pending', 0)}")
            print(f"   Last Delivered: {group.get('last-delivered-id', 'N/A')}")
    except Exception:
        pass

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['worker', 'replay', 'stats'],
                        default='worker', help='Run mode')
    parser.add_argument('--stream', default=STREAM_NAME, help='Stream name')
    parser.add_argument('--dlq', default=DLQ_STREAM, help='DLQ name')
    parser.add_argument('--group', default=CONSUMER_GROUP, help='Consumer group')
    
    args = parser.parse_args()
    
    if args.mode == 'worker':
        run_worker()
    elif args.mode == 'replay':
        run_replay()
    else:
        show_stats()