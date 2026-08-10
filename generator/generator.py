#!/usr/bin/env python3
"""
Medical Oxygen Sensor Generator - MQTT version
"""
import json
import time
import random
import argparse
import os
from datetime import datetime
from typing import Dict, Any
import paho.mqtt.client as mqtt
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OxygenSensorPayload:
    @staticmethod
    def generate(device_id: str = None, hospital_id: str = None) -> Dict[str, Any]:
        if not device_id:
            device_id = f"O2P-{random.choice(['VIE','MIA'])}-ICU-{random.randint(1000,9999)}"
        if not hospital_id:
            hospital_id = f"HOSP-{random.choice(['FL','CA'])}-{random.randint(1000,9999)}"
        
        liquid_level = round(random.uniform(15.0, 95.0), 1)
        
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "system_metadata": {
                "device_id": device_id,
                "hospital_id": hospital_id,
                "facility_zone": random.choice(["Main-ICU", "ER-Backup", "Surgical-Wing"]),
                "firmware_version": f"v{random.randint(3,5)}.{random.randint(0,9)}.{random.randint(0,9)}"
            },
            "tank_metrics": {
                "liquid_level_percent": liquid_level,
                "liquid_volume_liters": round((liquid_level / 100) * 11000, 1),
                "current_weight_kg": round(((liquid_level / 100) * 11000 * 1.141) + 800, 1),
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

class SensorGenerator:
    def __init__(self, broker="localhost", port=1883):
        self.broker = broker
        self.port = port
        self.topic = "oxygen/sensors/data"
        self.client = None
    
    def connect_mqtt(self):
        self.client = mqtt.Client()
        try:
            self.client.connect(self.broker, self.port, 60)
            logger.info(f"✅ Connected to MQTT at {self.broker}:{self.port}")
            return True
        except Exception as e:
            logger.error(f"❌ MQTT connection failed: {e}")
            return False
    
    def run(self, interval=5, count=0):
        if not self.connect_mqtt():
            return
        
        device_id = f"O2P-{random.choice(['VIE','MIA'])}-ICU-{random.randint(1000,9999)}"
        published = 0
        
        logger.info(f"📡 Starting generator")
        print("Press Ctrl+C to stop")
        
        try:
            while True:
                if count > 0 and published >= count:
                    break
                
                payload = OxygenSensorPayload.generate(device_id)
                self.client.publish(self.topic, json.dumps(payload))
                published += 1
                
                if published % 10 == 0:
                    logger.info(f"📊 Published {published} messages")
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("🛑 Stopping generator")
        finally:
            self.client.disconnect()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", default="localhost")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--interval", type=int, default=5)
    parser.add_argument("--count", type=int, default=0)
    args = parser.parse_args()
    
    generator = SensorGenerator(broker=args.broker, port=args.port)
    generator.run(interval=args.interval, count=args.count)