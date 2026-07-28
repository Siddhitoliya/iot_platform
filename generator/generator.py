"""
Medical Oxygen Sensor Generator
Simulates oxygen tank sensors publishing to MQTT
"""
import json
import time
import random
import argparse
import os
from datetime import datetime
from typing import Dict, Any
import paho.mqtt.client as mqtt
from redis import Redis
from rq import Queue
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class OxygenSensorPayload:
    """Generate realistic medical oxygen tank sensor data"""
    
    @staticmethod
    def generate(device_id: str = None, hospital_id: str = None) -> Dict[str, Any]:
        """Generate a complete sensor payload"""
        
        if not device_id:
            device_id = f"O2P-{random.choice(['VIE','MIA','NYC','LAX'])}-{random.choice(['ICU','ER','OR'])}-{random.randint(1000,9999):04d}"
        
        if not hospital_id:
            hospital_id = f"HOSP-{random.choice(['FL','CA','NY','TX'])}-{random.randint(1000,9999):04d}"
        
        liquid_level = round(random.uniform(15.0, 95.0), 1)
        tank_volume_liters = round((liquid_level / 100) * 11000, 1)
        
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "system_metadata": {
                "device_id": device_id,
                "hospital_id": hospital_id,
                "facility_zone": random.choice(["Main-ICU-Supply-Yard", "ER-Backup", "Surgical-Wing"]),
                "firmware_version": f"v{random.randint(3,5)}.{random.randint(0,9)}.{random.randint(0,9)}"
            },
            "tank_metrics": {
                "liquid_level_percent": liquid_level,
                "liquid_volume_liters": tank_volume_liters,
                "current_weight_kg": round((tank_volume_liters * 1.141) + 800, 1),
                "head_pressure_bar": round(random.uniform(10.0, 15.0), 1),
                "tank_temperature_celsius": round(random.uniform(-190.0, -180.0), 1),
                "vacuum_jacket_pressure_millibar": round(random.uniform(0.001, 0.005), 3)
            },
            "valve_and_vaporizer_states": {
                "main_isolation_valve": random.choice(["OPEN", "CLOSED"]),
                "bypass_valve": random.choice(["OPEN", "CLOSED"]),
                "economizer_valve_position_percent": round(random.uniform(0, 100), 1),
                "pressure_building_valve": random.choice(["OPEN", "CLOSED"]),
                "vaporizer_outlet_temperature_celsius": round(random.uniform(15.0, 25.0), 1),
                "vaporizer_frost_index": round(random.uniform(0.0, 0.5), 2)
            },
            "pipeline_distribution": {
                "output_flow_rate_m3_per_hour": round(random.uniform(50.0, 300.0), 1),
                "pipeline_pressure_bar": round(random.uniform(3.5, 5.5), 1),
                "gas_purity_percentage": round(random.uniform(99.2, 99.9), 1)
            },
            "system_health": {
                "battery_backup_percent": round(random.uniform(80.0, 100.0), 1),
                "power_source": random.choice(["MAINS", "BATTERY", "GENERATOR"]),
                "network_signal_dbm": random.randint(-80, -50),
                "active_error_codes": []
            }
        }
    
    @staticmethod
    def generate_anomaly() -> Dict[str, Any]:
        """Generate an anomalous reading for testing"""
        payload = OxygenSensorPayload.generate()
        
        anomaly_type = random.choice(['low_pressure', 'high_temp', 'low_purity', 'valve_mismatch'])
        
        if anomaly_type == 'low_pressure':
            payload['pipeline_distribution']['pipeline_pressure_bar'] = round(random.uniform(0.5, 1.5), 1)
            payload['system_health']['active_error_codes'].append("PRESSURE_LOW")
        elif anomaly_type == 'high_temp':
            payload['tank_metrics']['tank_temperature_celsius'] = round(random.uniform(-100.0, -50.0), 1)
            payload['system_health']['active_error_codes'].append("TEMP_ABNORMAL")
        elif anomaly_type == 'low_purity':
            payload['pipeline_distribution']['gas_purity_percentage'] = round(random.uniform(90.0, 97.0), 1)
            payload['system_health']['active_error_codes'].append("PURITY_LOW")
        
        return payload


class SensorGenerator:
    """Main generator class"""
    
    def __init__(self, broker="localhost", port=1883, use_queue=True, 
                 redis_url="redis://localhost:6379"):
        self.broker = broker
        self.port = port
        self.use_queue = use_queue
        self.topic = "oxygen/sensors/data"
        self.client = None
        self.running = False
        self.stats = {"total": 0, "anomalies": 0, "errors": 0}
        
        if use_queue:
            self.redis = Redis.from_url(redis_url)
            self.queue = Queue('high', connection=self.redis)
            logger.info("✅ Redis queue enabled")
        
        self.devices = self._generate_devices()
    
    def _generate_devices(self):
        """Generate list of devices"""
        hospitals = [
            "HOSP-FL-7721", "HOSP-CA-3342", "HOSP-NY-1190", 
            "HOSP-TX-5582", "HOSP-IL-4410", "HOSP-PA-8833"
        ]
        zones = ["Main-ICU-Supply-Yard", "ER-Backup", "Surgical-Wing"]
        
        devices = []
        for hospital in hospitals:
            for i in range(2): 
                device_id = f"O2P-{hospital.split('-')[1]}-{zones[i % len(zones)]}-{random.randint(1000,9999):04d}"
                devices.append({
                    "device_id": device_id,
                    "hospital_id": hospital,
                    "zone": zones[i % len(zones)]
                })
        return devices
    
    def connect_mqtt(self):
        """Connect to MQTT broker"""
        self.client = mqtt.Client()
        try:
            self.client.connect(self.broker, self.port, 60)
            logger.info(f"✅ Connected to MQTT at {self.broker}:{self.port}")
            return True
        except Exception as e:
            logger.error(f"❌ MQTT connection failed: {e}")
            return False
    
    def publish(self, payload: Dict[str, Any], anomalous: bool = False):
        """Publish to MQTT or queue"""
        message = json.dumps(payload)
        
        if self.use_queue:
            self.queue.enqueue(
                'worker.processor.process_sensor_data',
                payload,
                retry_delay=5,
                retry_count=3
            )
            logger.debug(f"📦 Queued: {payload['system_metadata']['device_id']}")
        else:
            if not self.client:
                if not self.connect_mqtt():
                    return False
            self.client.publish(self.topic, message)
            logger.debug(f"📤 Published: {payload['system_metadata']['device_id']}")
        
        self.stats["total"] += 1
        if anomalous:
            self.stats["anomalies"] += 1
        
        return True
    
    def run(self, interval=5, anomaly_rate=0.05):
        """Main loop"""
        if not self.use_queue and not self.connect_mqtt():
            return
        
        self.running = True
        logger.info(f"📡 Starting generator: {len(self.devices)} devices")
        logger.info(f"   Anomaly rate: {anomaly_rate*100}%")
        logger.info(f"   Queue mode: {'ENABLED' if self.use_queue else 'DISABLED'}")
        print("   Press Ctrl+C to stop")
        print("-" * 60)
        
        try:
            while self.running:
                for device in self.devices:
                    is_anomaly = random.random() < anomaly_rate
                    
                    if is_anomaly:
                        payload = OxygenSensorPayload.generate_anomaly()
                    else:
                        payload = OxygenSensorPayload.generate(
                            device_id=device["device_id"],
                            hospital_id=device["hospital_id"]
                        )
                    
                    payload["system_metadata"]["device_id"] = device["device_id"]
                    payload["system_metadata"]["hospital_id"] = device["hospital_id"]
                    payload["system_metadata"]["facility_zone"] = device["zone"]
                    
                    self.publish(payload, is_anomaly)
                    time.sleep(0.1) 
                
                if self.stats["total"] % len(self.devices) == 0:
                    logger.info(f"📊 Published {self.stats['total']} | "
                              f"Anomalies: {self.stats['anomalies']} "
                              f"({self.stats['anomalies']/max(1,self.stats['total'])*100:.1f}%)")
                
                time.sleep(max(0, interval - (len(self.devices) * 0.1)))
                
        except KeyboardInterrupt:
            logger.info("🛑 Stopping generator")
        finally:
            self.running = False
            if self.client:
                self.client.disconnect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", default=os.getenv("MQTT_BROKER", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.getenv("MQTT_PORT", 1883)))
    parser.add_argument("--interval", type=int, default=int(os.getenv("PUBLISH_INTERVAL", 5)))
    parser.add_argument("--anomaly-rate", type=float, default=float(os.getenv("ANOMALY_RATE", 0.05)))
    parser.add_argument("--no-queue", action="store_true", help="Disable Redis queue")
    args = parser.parse_args()
    
    generator = SensorGenerator(
        broker=args.broker,
        port=args.port,
        use_queue=not args.no_queue,
        redis_url=os.getenv("REDIS_URL", "redis://localhost:6379")
    )
    generator.run(interval=args.interval, anomaly_rate=args.anomaly_rate)