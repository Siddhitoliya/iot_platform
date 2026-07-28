"""
AWS Lambda function for processing oxygen sensor data
"""
import json
import os
import logging
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Database config from environment
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT', '5432')
DB_NAME = os.environ.get('DB_NAME', 'oxygen_data')
DB_USER = os.environ.get('DB_USER', 'pipeline')
DB_PASSWORD = os.environ.get('DB_PASSWORD')

# SQS config
QUEUE_URL = os.environ.get('QUEUE_URL')
DLQ_URL = os.environ.get('DLQ_URL')

# Initialize AWS clients
sqs = boto3.client('sqs')

def get_db_connection():
    """Create PostgreSQL connection"""
    try:
        return psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            connect_timeout=10
        )
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

def flatten_payload(payload):
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

def insert_reading(conn, data):
    """Insert sensor reading into database"""
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
    cur = conn.cursor()
    cur.execute(sql, data)
    conn.commit()
    cur.close()

def lambda_handler(event, context):
    """Main Lambda handler"""
    logger.info(f"📨 Processing {len(event['Records'])} records")
    
    conn = None
    success_count = 0
    error_count = 0
    
    try:
        conn = get_db_connection()
        
        for record in event['Records']:
            try:
                # Parse SQS message
                body = json.loads(record['body'])
                logger.info(f"Processing: {body.get('system_metadata', {}).get('device_id', 'unknown')}")
                
                # Flatten and insert
                data = flatten_payload(body)
                insert_reading(conn, data)
                success_count += 1
                
            except Exception as e:
                logger.error(f"❌ Error processing record: {e}")
                error_count += 1
                
                # Send to DLQ
                if DLQ_URL:
                    sqs.send_message(
                        QueueUrl=DLQ_URL,
                        MessageBody=record['body'],
                        MessageAttributes={
                            'Error': {
                                'DataType': 'String',
                                'StringValue': str(e)
                            }
                        }
                    )
        
    except Exception as e:
        logger.error(f"❌ Lambda error: {e}")
        raise
    finally:
        if conn:
            conn.close()
    
    logger.info(f"📊 Summary: {success_count} success, {error_count} failed")
    return {
        'statusCode': 200,
        'body': json.dumps({
            'successful': success_count,
            'failed': error_count
        })
    }