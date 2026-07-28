#!/usr/bin/env python3
"""
Load test for MQTT and Redis queue
"""
import json
import time
import threading
import statistics
import paho.mqtt.client as mqtt
from redis import Redis
from rq import Queue
from datetime import datetime
import random

class MQTTLoadTest:
    def __init__(self, broker='localhost', port=1883):
        self.broker = broker
        self.port = port
        self.results = []
        self.lock = threading.Lock()
    
    def publisher_thread(self, client_id, messages=100, topic='test'):
        """Single publisher thread"""
        client = mqtt.Client(f'{client_id}')
        try:
            client.connect(self.broker, self.port, 60)
            start = time.time()
            
            for i in range(messages):
                payload = json.dumps({
                    'client': client_id,
                    'seq': i,
                    'timestamp': datetime.utcnow().isoformat()
                })
                client.publish(topic, payload)
                time.sleep(0.001)  # Small delay
            
            duration = time.time() - start
            with self.lock:
                self.results.append({
                    'client': client_id,
                    'messages': messages,
                    'duration': duration,
                    'rate': messages / duration
                })
            client.disconnect()
        except Exception as e:
            with self.lock:
                self.results.append({
                    'client': client_id,
                    'error': str(e)
                })
    
    def run_test(self, num_clients=10, messages_per_client=100):
        """Run load test with multiple publishers"""
        print(f"🚀 Starting load test: {num_clients} clients, {messages_per_client} messages each")
        print("-" * 60)
        
        threads = []
        for i in range(num_clients):
            t = threading.Thread(
                target=self.publisher_thread,
                args=(f'p{i}', messages_per_client)
            )
            threads.append(t)
            t.start()
        
        # Wait for all threads
        for t in threads:
            t.join()
        
        # Analyze results
        successful = [r for r in self.results if 'error' not in r]
        failed = [r for r in self.results if 'error' in r]
        
        print(f"\n📊 Results:")
        print(f"   Successful: {len(successful)}/{len(self.results)}")
        print(f"   Failed: {len(failed)}")
        
        if successful:
            rates = [r['rate'] for r in successful]
            print(f"   Avg rate: {statistics.mean(rates):.2f} msg/s")
            print(f"   Min rate: {min(rates):.2f} msg/s")
            print(f"   Max rate: {max(rates):.2f} msg/s")
            print(f"   Total throughput: {sum(rates):.2f} msg/s")
        
        print(f"\n   Total messages sent: {len(successful) * messages_per_client}")

class RedisQueueLoadTest:
    def __init__(self, redis_url='redis://localhost:6379'):
        self.redis = Redis.from_url(redis_url)
        self.queue = Queue('high', connection=self.redis)
    
    def enqueue_batch(self, num_jobs=1000):
        """Enqueue many jobs at once"""
        start = time.time()
        
        for i in range(num_jobs):
            self.queue.enqueue(
                'worker.processor.process_sensor_data',
                {
                    'system_metadata': {'device_id': f'test-{i}'},
                    'tank_metrics': {},
                    'valve_and_vaporizer_states': {},
                    'pipeline_distribution': {},
                    'system_health': {'active_error_codes': []},
                    'timestamp': datetime.utcnow().isoformat()
                }
            )
        
        duration = time.time() - start
        print(f"📦 Enqueued {num_jobs} jobs in {duration:.2f}s")
        print(f"   Rate: {num_jobs/duration:.2f} jobs/s")
        
        # Check queue size
        size = self.queue.count
        print(f"   Queue size: {size}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--clients', type=int, default=10, help='Number of clients')
    parser.add_argument('--messages', type=int, default=100, help='Messages per client')
    parser.add_argument('--mode', choices=['mqtt', 'redis'], default='mqtt', help='Test mode')
    args = parser.parse_args()
    
    if args.mode == 'mqtt':
        test = MQTTLoadTest()
        test.run_test(args.clients, args.messages)
    else:
        test = RedisQueueLoadTest()
        test.enqueue_batch(args.clients * args.messages)