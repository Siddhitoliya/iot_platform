"""
RQ Worker - Processes queued sensor data jobs
"""
import os
import json
import logging
import time
from datetime import datetime
from typing import Dict, Any
import psycopg2
from psycopg2.extras import execute_values
from redis import Redis
from rq import Worker, Queue, Connection
import prometheus_client

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

COUNTER = prometheus_client.Counter(
    'sensor_processed_total',
    'Total sensor readings processed',
    ['status']
)
GAUGE = prometheus_client.Gauge(
    'sensor_queue_size',
    'Current queue size'
)
HISTOGRAM = prometheus_client.Histogram(
    'sensor_processing_seconds',
    'Time to process sensor data'
)

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'oxygen_data'),
    'user': os.getenv('DB_USER', 'pipeline'),
    'password': os.getenv('DB_PASSWORD', 'pipeline123')
}

def get_db_connection():
    """Get PostgreSQL connection"""
    try:
        return psycopg2.connect(**DB_CONFIG, connect_timeout=10)
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

def process_sensor_data(payload: Dict[str, Any]) -> bool:
    """
    Process a sensor reading.
    This is the main job function called by RQ worker.
    """
    with HISTOGRAM.time():
        try:
            data = flatten_payload(payload)
            
            conn = get_db_connection()
            try:
                cur = conn.cursor()
                
                sql = """
                    INSERT INTO oxygen_readings (
                        device_id, hospital_id, facility_zone, firmware_version,
                        liquid_level_percent, liquid_volume_liters, current_weight_kg,
                        head_pressure_bar, tank_temperature_celsius, vacuum_jacket_pressure_millibar,
                        main_isolation_valve, bypass_valve, economizer_valve_position_percent,
                        pressure_building_valve, vaporizer_outlet_temperature_celsius, vaporizer_frost_index,
                        output_flow_rate_m3_per_hour, pipeline_pressure_bar, gas_purity_percentage,
                        battery_backup_percent, power_source, network_signal_dbm, active_error_codes,
                        event_time, is_anomaly
                    ) VALUES (
                        %(device_id)s, %(hospital_id)s, %(facility_zone)s, %(firmware_version)s,
                        %(liquid_level_percent)s, %(liquid_volume_liters)s, %(current_weight_kg)s,
                        %(head_pressure_bar)s, %(tank_temperature_celsius)s, %(vacuum_jacket_pressure_millibar)s,
                        %(main_isolation_valve)s, %(bypass_valve)s, %(economizer_valve_position_percent)s,
                        %(pressure_building_valve)s, %(vaporizer_outlet_temperature_celsius)s, %(vaporizer_frost_index)s,
                        %(output_flow_rate_m3_per_hour)s, %(pipeline_pressure_bar)s, %(gas_purity_percentage)s,
                        %(battery_backup_percent)s, %(power_source)s, %(network_signal_dbm)s, %(active_error_codes)s::jsonb,
                        %(event_time)s::timestamptz, %(is_anomaly)s
                    )
                """
                
                cur.execute(sql, data)
                conn.commit()
                cur.close()
                
                COUNTER.labels(status='success').inc()
                logger.info(f"✅ Inserted: {data['device_id']} (anomaly: {data['is_anomaly']})")
                return True
                
            except Exception as e:
                logger.error(f"❌ Insert failed: {e}")
                COUNTER.labels(status='error').inc()
                raise
            finally:
                conn.close()
                
        except Exception as e:
            logger.error(f"❌ Processing failed: {e}")
            raise

def flatten_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten nested JSON payload"""
    return {
        'device_id': payload['system_metadata']['device_id'],
        'hospital_id': payload['system_metadata']['hospital_id'],
        'facility_zone': payload['system_metadata']['facility_zone'],
        'firmware_version': payload['system_metadata']['firmware_version'],
        
        'liquid_level_percent': payload['tank_metrics']['liquid_level_percent'],
        'liquid_volume_liters': payload['tank_metrics']['liquid_volume_liters'],
        'current_weight_kg': payload['tank_metrics']['current_weight_kg'],
        'head_pressure_bar': payload['tank_metrics']['head_pressure_bar'],
        'tank_temperature_celsius': payload['tank_metrics']['tank_temperature_celsius'],
        'vacuum_jacket_pressure_millibar': payload['tank_metrics']['vacuum_jacket_pressure_millibar'],
        
        'main_isolation_valve': payload['valve_and_vaporizer_states']['main_isolation_valve'],
        'bypass_valve': payload['valve_and_vaporizer_states']['bypass_valve'],
        'economizer_valve_position_percent': payload['valve_and_vaporizer_states']['economizer_valve_position_percent'],
        'pressure_building_valve': payload['valve_and_vaporizer_states']['pressure_building_valve'],
        'vaporizer_outlet_temperature_celsius': payload['valve_and_vaporizer_states']['vaporizer_outlet_temperature_celsius'],
        'vaporizer_frost_index': payload['valve_and_vaporizer_states']['vaporizer_frost_index'],
        
        'output_flow_rate_m3_per_hour': payload['pipeline_distribution']['output_flow_rate_m3_per_hour'],
        'pipeline_pressure_bar': payload['pipeline_distribution']['pipeline_pressure_bar'],
        'gas_purity_percentage': payload['pipeline_distribution']['gas_purity_percentage'],
        
        'battery_backup_percent': payload['system_health']['battery_backup_percent'],
        'power_source': payload['system_health']['power_source'],
        'network_signal_dbm': payload['system_health']['network_signal_dbm'],
        'active_error_codes': json.dumps(payload['system_health']['active_error_codes']),
        
        'event_time': payload['timestamp'],
        
        'is_anomaly': len(payload['system_health']['active_error_codes']) > 0
    }

def update_queue_metrics():
    """Update queue size metric"""
    try:
        redis_conn = Redis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6379'))
        for queue_name in ['high', 'default', 'low']:
            queue = Queue(queue_name, connection=redis_conn)
            size = queue.count
            GAUGE.set(size)
    except Exception as e:
        logger.error(f"Failed to get queue metrics: {e}")

if __name__ == "__main__":
    prometheus_client.start_http_server(8000)
    
    import threading
    def metrics_updater():
        while True:
            update_queue_metrics()
            time.sleep(10)
    threading.Thread(target=metrics_updater, daemon=True).start()
    
    redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
    redis_conn = Redis.from_url(redis_url)
    
    with Connection(redis_conn):
        worker = Worker(['high', 'default', 'low'])
        logger.info("🚀 Starting RQ worker...")
        worker.work()