"""
Baby Oil Dispenser - Complete IoT Generator/Simulator
Generates all payloads as per Payloads.xlsx
"""
import json
import random
import time
import argparse
import os
import signal
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from enum import Enum
import paho.mqtt.client as mqtt
import redis
from redis import Redis
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# Enums
# ============================================================================

class DeviceState(Enum):
    SLEEP = "sleep"
    WAKE = "wake"
    WEIGHT_MEASURE = "weight_measure"
    NECK_RING_WARM = "neck_ring_warm"
    HEAT = "heat"
    READY = "ready_to_despense"
    DISPENSE = "dispense"
    PROCESSING = "processing"
    IDLE = "idle"
    FAULT = "fault"
    SAFE_MODE = "safe_mode"
    OTA_UPDATING = "ota_updating"

class SessionStatus(Enum):
    IDLE = "idle"
    PREPARING = "preparing"
    WARMING = "warming"
    HEATING = "heating"
    READY = "ready"
    DISPENSING = "dispensing"
    COMPLETE = "complete"
    PROCESSING = "processing"
    CANCELLED = "cancelled"
    FAULT = "fault"
    SAFETY = "safety"

class BabyOilSimulator:
    """Complete simulator generating payloads as per Payloads.xlsx"""
    
    def __init__(self, device_id: str = None, user_id: str = None, profile_id: str = None):
        self.device_id = device_id or f"BOD-{random.randint(1000, 9999)}"
        self.user_id = user_id or f"USER-{random.randint(1000, 9999)}"
        self.profile_id = profile_id or f"BABY-{random.randint(100, 999)}"
        self.firmware_version = f"v{random.randint(2,3)}.{random.randint(0,9)}.{random.randint(0,9)}"
        
        self.state = DeviceState.SLEEP
        self.session_status = SessionStatus.IDLE
        self.session_active = False
        self.session_id = None
        self.uptime_seconds = random.randint(0, 86400)

        self.heartbeat_interval = 30
        self.heartbeat_timeout = 180
        self.heartbeat_missed = 0
        self.last_heartbeat = datetime.utcnow()
        self.sequence_number = 0
        
        self.oil_level = random.uniform(20.0, 95.0)
        self.oil_volume_ml = (self.oil_level / 100) * 500
        self.remaining_sessions = int(self.oil_volume_ml / 5)
        self.total_uses = 0
        self.total_volume_dispensed_ml = 0.0
        self.load_cell_reading_g = self.oil_volume_ml * 1.0
        
        self.tcc_temp = random.uniform(20.0, 25.0)
        self.neck_ring_temp = random.uniform(20.0, 25.0)
        self.ambient_temp = 25.0
        self.target_temp = 37.0
        
        self.tcc_sensor_ok = True
        self.neck_ring_sensor_ok = True
        self.sensors_agree = True
        self.sensor_anomaly_count = 0
        self.last_sensor_anomaly = None
        self.anomaly_details = {}
        
        self.neck_ring_active = False
        self.heating_chamber_active = False
        self.heater_power_w = 0.0
        self.pid_output = 0.0
        self.pid_integral = 0.0
        self.pid_last_error = 0.0
        
        self.battery_percent = random.uniform(60.0, 100.0)
        self.battery_voltage = 3.2 + (self.battery_percent / 100) * 1.0
        self.charging_status = "NOT_CHARGING"
        self.is_charging = False
        
        self.child_lock = True
        self.hard_stop_count = 0
        self.hard_stop_state = "normal"
        self.safe_mode = False
        self.overheating_protection = "NORMAL"
        self.error_codes = []
        self.warning_codes = []
        
        self.wifi_status = "CONNECTED"
        self.ble_status = "CONNECTED"
        self.cloud_sync_status = "SYNCED"
        self.signal_strength = random.randint(-70, -40)
        self.offline_logs = []
        self.mode = "online"
        self.connection = "wi-fi"
        
        self.baby_profiles = [
            {
                "profile_id": f"BABY-{random.randint(100, 999)}",
                "name": "Emma",
                "target_temp": 37.0,
                "oil_volume": 5.0,
                "oil_type": "Coconut",
                "age_months": 18,
                "weight_kg": 12.3
            },
            {
                "profile_id": f"BABY-{random.randint(100, 999)}",
                "name": "Noah",
                "target_temp": 36.5,
                "oil_volume": 6.0,
                "oil_type": "Olive",
                "age_months": 6,
                "weight_kg": 7.8
            }
        ]
        self.active_baby = self.baby_profiles[0]
        self.pending_profile = None
        
        self.session_start_time = None
        self.dose_count = 0
        self.max_doses = 5
        self.session_progress = 0
        self.estimated_time_remaining = "45s"
        
        self.oled_state = "sleep"
        self.oled_messages = []
        
        self.ota_status = "IDLE"
        self.ota_progress = 0
        self.available_firmware = None
        self.ota_phase = ""
        self.ota_changelog = ""
        
        self.mqtt_connected = False
        self.mqtt_client = None
        
        self.event_count = 0
        
        logger.info(f"✅ Simulator initialized: {self.device_id}")
    
    def power_on(self):
        """User powers on device via physical button"""
        self.state = DeviceState.WAKE
        self.oled_state = "wake"
        self._log_event("Device powered on")
        return self._create_status_payload("power_on", "Device powered on")
    
    def check_oil_volume(self):
        """Device checks oil volume via load sensor"""
        self.state = DeviceState.WEIGHT_MEASURE
        self.oled_state = "weight_measure"
        
        self.oil_level = (self.oil_volume_ml / 500) * 100
        self.remaining_sessions = int(self.oil_volume_ml / 5)
        self.load_cell_reading_g = self.oil_volume_ml * 1.0
        
        if self.oil_level < 10:
            self.warning_codes.append("LOW_OIL")
            self._send_notification("low_oil", "Low Oil Warning", 
                                   f"Oil level at {self.oil_level:.1f}%. Please replace cartridge soon.",
                                   {"oil_level_percent": self.oil_level, 
                                    "remaining_sessions": self.remaining_sessions,
                                    "oil_volume_ml": self.oil_volume_ml})
        
        return self._create_telemetry_payload("weight_measured", 
            f"Oil volume checked: {self.remaining_sessions} sessions remaining")
    
    def pre_warm_neck_ring(self):
        """Neck ring pre-warms oil"""
        self.state = DeviceState.NECK_RING_WARM
        self.oled_state = "warming"
        self.neck_ring_active = True
        self.heater_power_w = 150.0
        self.session_status = SessionStatus.WARMING
        self.session_active = True
        
        self.neck_ring_temp += random.uniform(2.0, 5.0)
        self.tcc_temp += random.uniform(0.5, 1.0)
        
        return self._create_telemetry_payload("warming", "Pre-warming oil to reduce viscosity")
    
    def heat_to_target(self):
        """Heat oil to target temperature"""
        self.state = DeviceState.HEAT
        self.oled_state = "heating"
        self.heating_chamber_active = True
        self.heater_power_w = 300.0
        self.target_temp = self.active_baby["target_temp"]
        self.session_status = SessionStatus.HEATING
        self.session_active = True
        
        progress = 0
        while self.tcc_temp < self.target_temp:
            heat_increment = random.uniform(0.5, 1.0)
            self.tcc_temp += heat_increment
            self.heating_chamber_temp = self.tcc_temp
            progress += 5
            
            error = self.target_temp - self.tcc_temp
            self.pid_integral += error * 0.1
            derivative = (error - self.pid_last_error) / 0.1
            self.pid_output = (2.0 * error + 0.5 * self.pid_integral + 0.1 * derivative)
            self.pid_output = max(0, min(100, self.pid_output))
            self.pid_last_error = error
            
            delta = abs(self.tcc_temp - self.neck_ring_temp)
            if delta > 2.0:
                self.sensors_agree = False
                self.sensor_anomaly_count += 1
                self.last_sensor_anomaly = datetime.utcnow()
                self.anomaly_details = {
                    "type": "SENSOR_MISMATCH",
                    "message": f"Sensor readings differ by {delta:.1f}°C",
                    "delta": delta,
                    "threshold": 2.0
                }
                self.error_codes.append("TEMP_SENSOR_MISMATCH")
                self.state = DeviceState.FAULT
                self.session_status = SessionStatus.FAULT
                return self._create_fault_payload("sensor_mismatch", "Temperature sensor mismatch detected")
            
            self.session_progress = progress
            time.sleep(0.01)
        
        self.state = DeviceState.READY
        self.oled_state = "ready"
        self.heating_chamber_active = False
        self.heater_power_w = 0.0
        self.session_status = SessionStatus.READY
        
        self._send_notification("oil_ready", "Oil Ready to Dispense", 
                               f"Oil has reached {self.target_temp}°C. Place hand below nozzle.",
                               {"session_id": self.session_id, "temperature": self.target_temp})
        
        return self._create_telemetry_payload("ready", "Ready to dispense")
    
    def dispense(self):
        """Dispense oil"""
        self.state = DeviceState.DISPENSE
        self.oled_state = "dispensing"
        self.session_status = SessionStatus.DISPENSING
        
        dose_ml = self.active_baby["oil_volume"]
        
        if self.oil_volume_ml < dose_ml:
            self.error_codes.append("INSUFFICIENT_OIL")
            return {"status": "error", "error": "Insufficient oil"}
        
        self.oil_volume_ml -= dose_ml
        self.oil_level = (self.oil_volume_ml / 500) * 100
        self.remaining_sessions = int(self.oil_volume_ml / 5)
        self.total_uses += 1
        self.total_volume_dispensed_ml += dose_ml
        self.dose_count += 1
        
        return self._create_telemetry_payload("dispensing", f"Hand Detected - Dispensing {dose_ml}ml")
    
    def return_to_sleep(self):
        """Return to sleep state"""
        self.state = DeviceState.SLEEP
        self.oled_state = "sleep"
        self.neck_ring_active = False
        self.heating_chamber_active = False
        self.heater_power_w = 0.0
        self.session_active = False
        self.dose_count = 0
        self.session_status = SessionStatus.COMPLETE
        
        return self._create_telemetry_payload("session_complete", "Session completed - Device in sleep mode")

    def _create_status_payload(self, event: str, message: str) -> Dict:
        """Create status payload (bod/device/status)"""
        return {
            "device_id": self.device_id,
            "user_id": self.user_id,
            "profile_id": self.active_baby.get("profile_id", self.profile_id),
            "firmware_version": self.firmware_version,
            "event": event,
            "state": self.state.value,
            "session_status": self.session_status.value if hasattr(self.session_status, 'value') else self.session_status,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "battery_percent": round(self.battery_percent, 1),
            "message": message
        }
    
    def _create_connectivity_payload(self) -> Dict:
        """Create connectivity payload (bod/device/connectivity)"""
        now = datetime.utcnow()
        return {
            "device_id": self.device_id,
            "user_id": self.user_id,
            "firmware_version": self.firmware_version,
            "event": "connectivity_status",
            "timestamp": now.isoformat() + "Z",
            "connectivity": {
                "connection": self.connection,
                "cloud_sync": self.cloud_sync_status,
                "signal_strength_dbm": self.signal_strength,
                "last_cloud_sync": now.isoformat() + "Z",
                "heartbeat_missed": self.heartbeat_missed,
                "offline_logs": len(self.offline_logs)
            },
            "mode": self.mode,
            "message": f"{'Wi-Fi' if self.connection == 'wi-fi' else 'BLE'} connected - operating in {'online' if self.mode == 'online' else 'offline/travel'} mode."
        }
    
    def _create_heartbeat_payload(self) -> Dict:
        """Create heartbeat payload (bod/device/heartbeat)"""
        now = datetime.utcnow()
        self.sequence_number += 1
        self.last_heartbeat = now
        
        payload = {
            "device_id": self.device_id,
            "user_id": self.user_id,
            "firmware_version": self.firmware_version,
            "heartbeat_id": f"hb_{now.strftime('%Y%m%d_%H%M%S')}_{self.sequence_number:04d}",
            "timestamp": now.isoformat() + "Z",
            "status": "alive",
            "uptime_seconds": self.uptime_seconds,
            "state": self.state.value,
            "session_active": self.session_active,
            "battery": {
                "percent": round(self.battery_percent, 1),
                "voltage": round(self.battery_voltage, 2),
                "charging": self.charging_status
            },
            "connectivity": {
                "wifi": self.wifi_status,
                "signal_strength_dbm": self.signal_strength,
                "rssi": self.signal_strength
            },
            "health": {
                "cpu_usage": round(random.uniform(5.0, 20.0), 1),
                "memory_usage": round(random.uniform(20.0, 50.0), 1),
                "temperature": round(self.tcc_temp, 1),
                "errors": self.error_codes,
                "warnings": self.warning_codes
            },
            "sequence_number": self.sequence_number
        }
        
        if self.session_active:
            payload["session_id"] = self.session_id
            payload["session_progress"] = self.session_progress
        
        return payload
    
    def _create_telemetry_payload(self, event: str, message: str) -> Dict:
        """Create telemetry payload (bod/device/telemetry/*)"""
        now = datetime.utcnow()
        
        payload = {
            "device_id": self.device_id,
            "user_id": self.user_id,
            "profile_id": self.active_baby.get("profile_id", self.profile_id),
            "firmware_version": self.firmware_version,
            "event": event,
            "state": self.state.value,
            "session_status": self.session_status.value if hasattr(self.session_status, 'value') else self.session_status,
            "timestamp": now.isoformat() + "Z",
            "temperature": {
                "tcc_temp": round(self.tcc_temp, 1),
                "neck_ring_temp": round(self.neck_ring_temp, 1),
                "target_temp": self.target_temp if self.state.value in ["heat", "ready_to_despense", "dispense"] else None,
                "delta": round(abs(self.tcc_temp - self.neck_ring_temp), 1)
            },
            "sensor_health": {
                "tcc_sensor_ok": self.tcc_sensor_ok,
                "neck_ring_sensor_ok": self.neck_ring_sensor_ok,
                "sensors_agree": self.sensors_agree
            },
            "battery_percent": round(self.battery_percent, 1),
            "message": message
        }
        
        if self.session_active:
            payload["session_active"] = True
            payload["session_id"] = self.session_id
        
        payload["baby_profile"] = {
            "profile_id": self.active_baby.get("profile_id", self.profile_id),
            "target_temp": self.active_baby["target_temp"],
            "oil_volume_per_session": self.active_baby["oil_volume"],
            "oil_type": self.active_baby["oil_type"]
        }
        
        if event == "weight_measured":
            payload["dispenser_metrics"] = {
                "oil_level_percent": round(self.oil_level, 1),
                "oil_volume_ml": round(self.oil_volume_ml, 1),
                "remaining_sessions": self.remaining_sessions,
                "load_cell_reading_g": round(self.load_cell_reading_g, 1)
            }
        
        elif event in ["warming", "heating_tcc", "ready", "dispensing", "session_complete"]:
            payload["dispenser_metrics"] = {
                "oil_level_percent": round(self.oil_level, 1),
                "oil_volume_ml": round(self.oil_volume_ml, 1),
                "remaining_sessions": self.remaining_sessions
            }
        
        if event == "warming":
            payload["heating"] = {
                "neck_ring_active": self.neck_ring_active,
                "heating_chamber_active": self.heating_chamber_active
            }
        
        elif event == "heating_tcc":
            payload["heating"] = {
                "neck_ring_active": self.neck_ring_active,
                "heating_chamber_active": self.heating_chamber_active,
                "heater_power_w": round(self.heater_power_w, 1),
                "pid_output": round(self.pid_output, 1),
                "pid_integral": round(self.pid_integral, 1),
                "pid_last_error": round(self.pid_last_error, 1)
            }
            payload["progress"] = self.session_progress
            payload["estimated_time_remaining"] = self.estimated_time_remaining
        
        elif event in ["ready", "dispensing"]:
            payload["heating"] = {
                "neck_ring_active": self.neck_ring_active,
                "heating_chamber_active": self.heating_chamber_active,
                "heater_power_w": round(self.heater_power_w, 1)
            }
        
        elif event == "dispensing":
            payload["dispensing"] = {
                "ir_hand_detected": True,
                "dose_ml": self.active_baby["oil_volume"],
                "dose_number": self.dose_count,
                "pump_flow_rate_ml_per_sec": round(random.uniform(2.0, 3.0), 1)
            }
        
        elif event == "session_complete":
            payload["session_summary"] = {
                "oil_used_ml": self.active_baby["oil_volume"],
                "remaining_oil_ml": round(self.oil_volume_ml, 1),
                "remaining_sessions": self.remaining_sessions,
                "total_doses": self.dose_count,
                "duration_seconds": random.randint(60, 180)
            }
            payload["temperature"]["target_temp"] = None
        
        return payload
    
    def _create_fault_payload(self, fault_type: str, message: str) -> Dict:
        """Create fault payload (bod/device/fault/*)"""
        now = datetime.utcnow()
        
        payload = {
            "device_id": self.device_id,
            "user_id": self.user_id,
            "firmware_version": self.firmware_version,
            "event": fault_type,
            "state": self.state.value,
            "session_status": self.session_status.value if hasattr(self.session_status, 'value') else self.session_status,
            "timestamp": now.isoformat() + "Z",
            "temperature": {
                "tcc_temp": round(self.tcc_temp, 1),
                "neck_ring_temp": round(self.neck_ring_temp, 1),
                "target_temp": self.target_temp,
                "delta": round(abs(self.tcc_temp - self.neck_ring_temp), 1)
            },
            "sensor_health": {
                "tcc_sensor_ok": self.tcc_sensor_ok,
                "neck_ring_sensor_ok": self.neck_ring_sensor_ok,
                "sensors_agree": self.sensors_agree,
                "anomaly_count": self.sensor_anomaly_count,
                "last_anomaly": self.last_sensor_anomaly.isoformat() + "Z" if self.last_sensor_anomaly else None,
                "anomaly_details": self.anomaly_details
            },
            "battery_percent": round(self.battery_percent, 1),
            "message": message
        }
        
        if fault_type == "sensor_mismatch":
            payload["error"] = {
                "code": "TEMP_SENSOR_MISMATCH",
                "severity": "CRITICAL",
                "message": "Temperature sensors disagree - Check sensors",
                "action": "Entering safe mode"
            }
        
        elif fault_type == "sensor_stuck":
            payload["heating"] = {
                "neck_ring_active": self.neck_ring_active,
                "heating_chamber_active": self.heating_chamber_active,
                "heater_power_w": round(self.heater_power_w, 1)
            }
            payload["error"] = {
                "code": "TEMP_SENSOR_STUCK",
                "severity": "CRITICAL",
                "message": "Temperature sensor stuck - Device entering safe mode",
                "action": "Check temperature sensor"
            }
        
        elif fault_type == "sensor_out_of_range":
            payload["error"] = {
                "code": "TEMP_SENSOR_OUT_OF_RANGE",
                "severity": "CRITICAL",
                "message": "TCC sensor reading out of range",
                "action": "Entering safe mode"
            }
        
        return payload
    
    def _create_safety_payload(self, event: str, message: str) -> Dict:
        """Create safety payload (bod/device/safety)"""
        now = datetime.utcnow()
        
        payload = {
            "device_id": self.device_id,
            "user_id": self.user_id,
            "firmware_version": self.firmware_version,
            "event": event,
            "state": self.state.value,
            "session_status": self.session_status.value if hasattr(self.session_status, 'value') else self.session_status,
            "timestamp": now.isoformat() + "Z",
            "safety": {
                "hard_stop_count": self.hard_stop_count,
                "hard_stop_state": self.hard_stop_state,
                "safe_mode": self.safe_mode,
                "child_lock_active": self.child_lock,
                "overheating_protection": self.overheating_protection
            },
            "battery_percent": round(self.battery_percent, 1),
            "message": message
        }
        
        if event == "hard_stop_protected":
            payload["error"] = {
                "code": "HARD_STOP_PROTECTED",
                "severity": "CRITICAL",
                "message": "Protected fault state entered after 3 hard stops",
                "action": "Acknowledge through mobile app"
            }
        elif event == "fault_acknowledged":
            payload["safety"]["error_codes"] = []
        
        return payload
    
    def _create_ota_payload(self, ota_event: str, message: str) -> Dict:
        """Create OTA payload (bod/device/ota/*)"""
        now = datetime.utcnow()
        
        payload = {
            "device_id": self.device_id,
            "user_id": self.user_id,
            "firmware_version": self.firmware_version,
            "event": ota_event,
            "timestamp": now.isoformat() + "Z",
            "battery_percent": round(self.battery_percent, 1),
            "message": message
        }
        
        if ota_event == "ota_available":
            payload["ota"] = {
                "current_version": self.firmware_version,
                "available_version": self.available_firmware or "v2.6.0",
                "changelog": "• Improved PID control\n• Better offline logging\n• Enhanced safety features\n• Fixed sensor calibration",
                "update_available": True
            }
        
        elif ota_event == "ota_in_progress":
            payload["state"] = "ota_updating"
            payload["session_status"] = "idle"
            payload["ota"] = {
                "status": self.ota_status,
                "progress": self.ota_progress,
                "current_version": self.firmware_version,
                "target_version": self.available_firmware or "v2.6.0",
                "phase": self.ota_phase or "Installing firmware"
            }
        
        elif ota_event == "ota_complete":
            payload["state"] = "idle"
            payload["session_status"] = "idle"
            payload["ota"] = {
                "status": "COMPLETE",
                "progress": 100,
                "old_version": self.firmware_version,
                "new_version": self.available_firmware or "v2.6.0"
            }
            payload["firmware_version"] = self.available_firmware or "v2.6.0"
        
        elif ota_event == "ota_failed":
            payload["state"] = "idle"
            payload["session_status"] = "idle"
            payload["ota"] = {
                "status": "FAILED",
                "progress": self.ota_progress,
                "current_version": self.firmware_version,
                "target_version": self.available_firmware or "v2.6.0",
                "error": "Download verification failed",
                "rollback_available": True
            }
        
        return payload
    
    def _create_profile_payload(self, profile_event: str, message: str) -> Dict:
        """Create profile payload (bod/device/profile/*)"""
        now = datetime.utcnow()
        
        payload = {
            "device_id": self.device_id,
            "user_id": self.user_id,
            "firmware_version": self.firmware_version,
            "event": profile_event,
            "timestamp": now.isoformat() + "Z",
            "message": message
        }
        
        if profile_event == "profile_configured":
            payload["profile"] = {
                "baby_name": self.active_baby["name"],
                "age_months": self.active_baby.get("age_months", 18),
                "weight_kg": self.active_baby.get("weight_kg", 12.3),
                "target_temperature": self.active_baby["target_temp"],
                "oil_volume_per_session": self.active_baby["oil_volume"],
                "oil_type": self.active_baby["oil_type"]
            }
        
        elif profile_event == "profile_fetched":
            payload["profile"] = {
                "profile_id": self.active_baby.get("profile_id", self.profile_id),
                "description": {
                    "name": self.active_baby["name"],
                    "target_temp": self.active_baby["target_temp"],
                    "oil_volume_per_session": self.active_baby["oil_volume"],
                    "oil_type": self.active_baby["oil_type"]
                }
            }
        
        elif profile_event == "profile_selected":
            payload["available_profiles"] = [
                {
                    "profile_id": p.get("profile_id", f"BABY-{i}"),
                    "name": p["name"],
                    "target_temp": p["target_temp"],
                    "oil_volume": p["oil_volume"]
                }
                for i, p in enumerate(self.baby_profiles)
            ]
            payload["selected_profile"] = {
                "profile_id": self.active_baby.get("profile_id", self.profile_id),
                "name": self.active_baby["name"],
                "target_temp": self.active_baby["target_temp"],
                "oil_volume": self.active_baby["oil_volume"]
            }
        
        elif profile_event == "profile_update":
            payload["state"] = "processing"
            payload["profile"] = {
                "profile_id": self.active_baby.get("profile_id", self.profile_id),
                "name": self.active_baby["name"],
                "target_temp": self.active_baby["target_temp"],
                "oil_volume_per_session": self.active_baby["oil_volume"],
                "oil_type": self.active_baby["oil_type"]
            }
            payload["reconfiguration"] = {
                "processing_state": True,
                "oil_reheating": True,
                "dose_volume_reconfigured": True
            }
        
        return payload
    
    def _send_notification(self, notif_type: str, title: str, body: str, data: Dict):
        """Send notification payload"""
        now = datetime.utcnow()
        return {
            "device_id": self.device_id,
            "user_id": self.user_id,
            "timestamp": now.isoformat() + "Z",
            "notification_type": notif_type,
            "title": title,
            "body": body,
            "data": data,
            "priority": "high" if notif_type in ["low_oil", "battery_critical", "battery_low_warning"] else "normal"
        }
    
    def _log_event(self, event: str):
        """Log event for offline storage"""
        self.offline_logs.append({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "event": event,
            "device_id": self.device_id
        })
        if len(self.offline_logs) > 100:
            self.offline_logs = self.offline_logs[-100:]

    def generate_heartbeat(self) -> Dict:
        """Generate heartbeat payload"""
        return self._create_heartbeat_payload()

    def go_offline(self):
        """Go offline"""
        self.wifi_status = "DISCONNECTED"
        self.cloud_sync_status = "PENDING"
        self.mode = "offline"
        self.connection = "ble"
        self.signal_strength = -90
        self.warning_codes.append("WIFI_OFFLINE")
        self.heartbeat_missed += 1
        return self._create_connectivity_payload()
    
    def go_online(self):
        """Go online"""
        self.wifi_status = "CONNECTED"
        self.cloud_sync_status = "SYNCED"
        self.mode = "online"
        self.connection = "wi-fi"
        self.signal_strength = random.randint(-70, -40)
        self.heartbeat_missed = 0
        if "WIFI_OFFLINE" in self.warning_codes:
            self.warning_codes.remove("WIFI_OFFLINE")
        return self._create_connectivity_payload()

    def check_battery(self) -> Dict:
        """Check battery level and trigger alerts"""
        now = datetime.utcnow()
        
        if self.battery_percent < 5 and self.battery_percent > 0:
            self.safe_mode = True
            self.state = DeviceState.SAFE_MODE
            self.error_codes.append("BATTERY_CRITICAL_SAFE_MODE")
            self._send_notification("battery_critical", "Critical Battery Alert",
                                   f"Battery at {self.battery_percent:.1f}%. Device entering safe mode.",
                                   {"battery_percent": self.battery_percent, 
                                    "battery_voltage": self.battery_voltage,
                                    "safe_mode_triggered": True})
            return {"status": "safe_mode", "battery_percent": self.battery_percent}
        
        if self.battery_percent < 15 and self.battery_percent >= 5:
            self.warning_codes.append("BATTERY_CRITICAL")
            self._send_notification("battery_critical", "Critical Battery Alert",
                                   f"Battery at {self.battery_percent:.1f}%. Please charge immediately!",
                                   {"battery_percent": self.battery_percent, 
                                    "battery_voltage": self.battery_voltage,
                                    "estimated_remaining_uses": int(self.battery_percent / 5),
                                    "estimated_time_left_minutes": int(self.battery_percent * 1.5),
                                    "action_required": True,
                                    "will_enter_safe_mode": True,
                                    "time_to_safe_mode_minutes": 5})
            return {"status": "critical", "battery_percent": self.battery_percent}
        
        if self.battery_percent < 30 and self.battery_percent >= 15:
            self.warning_codes.append("LOW_BATTERY")
            self._send_notification("battery_low_warning", "Low Battery Warning",
                                   f"Battery at {self.battery_percent:.1f}%. Please charge your Baby Oil Dispenser soon.",
                                   {"battery_percent": self.battery_percent, 
                                    "battery_voltage": self.battery_voltage,
                                    "estimated_remaining_uses": int(self.battery_percent / 5),
                                    "estimated_time_left_minutes": int(self.battery_percent * 1.5),
                                    "action_required": False,
                                    "recommended_action": "Plug in charger"})
            return {"status": "warning", "battery_percent": self.battery_percent}
        
        if "LOW_BATTERY" in self.warning_codes:
            self.warning_codes.remove("LOW_BATTERY")
        if "BATTERY_CRITICAL" in self.warning_codes:
            self.warning_codes.remove("BATTERY_CRITICAL")
        
        return {"status": "normal", "battery_percent": self.battery_percent}

    def check_ota_update(self):
        """Check for OTA update"""
        if self.state not in [DeviceState.IDLE, DeviceState.SLEEP]:
            return {"status": "not_available", "current_state": self.state.value}
        
        if random.random() < 0.3:
            self.available_firmware = f"v{random.randint(2,3)}.{random.randint(0,9)}.{random.randint(0,9)}"
            return self._create_ota_payload("ota_available", "Firmware update available")
        return {"status": "no_update"}
    
    def perform_ota_update(self):
        """Perform OTA update"""
        if self.state not in [DeviceState.IDLE, DeviceState.SLEEP]:
            return {"status": "error", "message": "OTA only allowed in idle state"}
        
        if not self.available_firmware:
            self.available_firmware = f"v{random.randint(2,3)}.{random.randint(0,9)}.{random.randint(0,9)}"
        
        self.state = DeviceState.OTA_UPDATING
        self.ota_status = "DOWNLOADING"
        self.ota_progress = 0
        
        phases = [
            {"phase": "DOWNLOADING", "progress": 25},
            {"phase": "VALIDATING", "progress": 45},
            {"phase": "INSTALLING", "progress": 75},
            {"phase": "REBOOTING", "progress": 90},
            {"phase": "COMPLETE", "progress": 100}
        ]
        
        # Simulate failure occasionally
        if random.random() < 0.1:
            self.ota_progress = 45
            self.ota_status = "FAILED"
            return self._create_ota_payload("ota_failed", "OTA update failed - Rolling back")
        
        for phase in phases:
            self.ota_status = phase["phase"]
            self.ota_progress = phase["progress"]
            self.ota_phase = phase["phase"]
            time.sleep(0.3)
        
        old_version = self.firmware_version
        self.firmware_version = self.available_firmware
        self.ota_status = "COMPLETE"
        self.ota_progress = 100
        self.state = DeviceState.IDLE
        
        return self._create_ota_payload("ota_complete", "OTA update completed successfully")
    
    # =========================================================================
    # Fault Detection (2 Sensors)
    # =========================================================================
    
    def detect_sensor_faults(self):
        """Detect temperature sensor faults using 2 sensors"""
        now = datetime.utcnow()
        
        # 1. Sensor mismatch
        delta = abs(self.tcc_temp - self.neck_ring_temp)
        if delta > 2.0:
            self.sensors_agree = False
            self.sensor_anomaly_count += 1
            self.last_sensor_anomaly = now
            self.anomaly_details = {
                "type": "SENSOR_MISMATCH",
                "message": f"Sensor readings differ by {delta:.1f}°C",
                "delta": delta,
                "threshold": 2.0
            }
            self.error_codes.append("TEMP_SENSOR_MISMATCH")
            self.state = DeviceState.FAULT
            self.session_status = SessionStatus.FAULT
            return self._create_fault_payload("sensor_mismatch", "Temperature sensor mismatch detected")
        
        # 2. Stuck sensor detection
        if self.tcc_temp < 20 and self.heating_chamber_active:
            self.sensor_anomaly_count += 1
            self.last_sensor_anomaly = now
            self.anomaly_details = {
                "type": "SENSOR_STUCK",
                "sensor": "TCC",
                "reading": self.tcc_temp,
                "message": f"TCC sensor reading stuck at {self.tcc_temp}°C",
                "duration": 60
            }
            self.error_codes.append("TEMP_SENSOR_STUCK")
            self.state = DeviceState.SAFE_MODE
            self.session_status = SessionStatus.FAULT
            return self._create_fault_payload("sensor_stuck", "Device entered safe mode")
        
        # 3. Out of range
        if not (20.0 <= self.tcc_temp <= 50.0):
            self.sensor_anomaly_count += 1
            self.last_sensor_anomaly = now
            self.anomaly_details = {
                "type": "OUT_OF_RANGE",
                "sensor": "TCC",
                "reading": self.tcc_temp,
                "range": "20-50°C"
            }
            self.error_codes.append("TEMP_SENSOR_OUT_OF_RANGE")
            self.state = DeviceState.FAULT
            self.session_status = SessionStatus.FAULT
            return self._create_fault_payload("sensor_out_of_range", "Temperature sensor out of range")
        
        return None
    
    # =========================================================================
    # Hard Stop
    # =========================================================================
    
    def trigger_hard_stop(self):
        """Trigger hard stop"""
        self.hard_stop_count += 1
        
        if self.hard_stop_count >= 3:
            self.hard_stop_state = "protected_fault"
            self.safe_mode = True
            self.state = DeviceState.SAFE_MODE
            self.session_status = SessionStatus.SAFETY
            self.error_codes.append("HARD_STOP_PROTECTED")
            return self._create_safety_payload("hard_stop_protected", "Hard stop protection triggered")
        
        return {"status": "hard_stop", "count": self.hard_stop_count}
    
    def acknowledge_fault(self):
        """Acknowledge fault"""
        self.hard_stop_count = 0
        self.hard_stop_state = "normal"
        self.safe_mode = False
        self.state = DeviceState.IDLE
        self.session_status = SessionStatus.IDLE
        self.error_codes = []
        return self._create_safety_payload("fault_acknowledged", "Fault acknowledged - Operation can resume")
    
    # =========================================================================
    # Profile Management
    # =========================================================================
    
    def configure_baby_profile(self, name: str, target_temp: float, oil_volume: float, oil_type: str = "Coconut"):
        """Configure baby profile"""
        profile = {
            "profile_id": f"BABY-{random.randint(100, 999)}",
            "name": name,
            "target_temp": target_temp,
            "oil_volume": oil_volume,
            "oil_type": oil_type,
            "age_months": 18,
            "weight_kg": 12.3
        }
        
        for i, p in enumerate(self.baby_profiles):
            if p["name"] == name:
                self.baby_profiles[i] = profile
                self.active_baby = profile
                break
        else:
            self.baby_profiles.append(profile)
            self.active_baby = profile
        
        return self._create_profile_payload("profile_configured", "Baby profile configured and synced to cloud")
    
    def select_profile(self, name: str):
        """Select baby profile"""
        for profile in self.baby_profiles:
            if profile["name"] == name:
                self.active_baby = profile
                return self._create_profile_payload("profile_selected", f"Active profile set to {name}")
        return {"status": "error", "message": f"Profile {name} not found"}
    
    def update_profile(self, name: str, target_temp: float = None, oil_volume: float = None):
        """Update profile configuration"""
        for profile in self.baby_profiles:
            if profile["name"] == name:
                if target_temp:
                    profile["target_temp"] = target_temp
                if oil_volume:
                    profile["oil_volume"] = oil_volume
                self.active_baby = profile
                self.state = DeviceState.PROCESSING
                return self._create_profile_payload("profile_update", "Profile reconfiguration - Processing reconfiguration")
        return {"status": "error", "message": f"Profile {name} not found"}
    
    def fetch_profile(self):
        """Fetch profile from cloud"""
        return self._create_profile_payload("profile_fetched", "Profile fetched from cloud")
    
    # =========================================================================
    # Generate Complete Payload
    # =========================================================================
    
    def generate_payload(self) -> Dict:
        """Generate complete device payload"""
        now = datetime.utcnow()
        self.uptime_seconds += 1
        
        # Update battery
        if self.battery_percent > 0 and not self.is_charging:
            self.battery_percent -= random.uniform(0.001, 0.005)
            self.battery_percent = max(0, self.battery_percent)
            self.battery_voltage = 3.2 + (self.battery_percent / 100) * 1.0
        
        # Check battery
        battery_status = self.check_battery()
        
        # Detect sensor faults
        fault = self.detect_sensor_faults()
        
        # Generate appropriate payload based on state
        if self.state == DeviceState.SLEEP or self.state == DeviceState.IDLE:
            return self._create_status_payload("status_update", f"Device in {self.state.value} mode")
        else:
            return self._create_telemetry_payload("state_update", f"Device in {self.state.value} state")
    
    # =========================================================================
    # Run Simulator
    # =========================================================================
    
    def run_continuous(self, interval: int = 5, publish_mqtt: bool = False, 
                       mqtt_client: mqtt.Client = None, publish_redis: bool = False,
                       redis_client: Redis = None):
        """Run simulator in continuous mode"""
        logger.info(f"🔄 Running continuous mode (interval={interval}s)")
        logger.info(f"Device: {self.device_id}, User: {self.user_id}")
        
        heartbeat_timer = 0
        heartbeat_interval = 30
        
        try:
            while True:
                # Simulate device behavior
                if self.state == DeviceState.SLEEP and random.random() < 0.05:
                    self.power_on()
                elif self.state == DeviceState.WAKE:
                    self.check_oil_volume()
                elif self.state == DeviceState.WEIGHT_MEASURE:
                    self.pre_warm_neck_ring()
                elif self.state == DeviceState.NECK_RING_WARM and random.random() < 0.3:
                    self.heat_to_target()
                elif self.state == DeviceState.READY and random.random() < 0.2:
                    self.dispense()
                elif self.state == DeviceState.DISPENSE and (self.dose_count >= self.max_doses or random.random() < 0.3):
                    self.return_to_sleep()
                
                # Random events
                if random.random() < 0.005:
                    self.go_offline()
                if random.random() < 0.01 and self.mode == "offline":
                    self.go_online()
                
                # Generate payload
                payload = self.generate_payload()
                
                # Publish
                if publish_mqtt and mqtt_client:
                    topic = "bod/device/telemetry/state"
                    mqtt_client.publish(topic, json.dumps(payload))
                    logger.info(f"📤 MQTT: {topic} - {payload['device_id']}")
                
                if publish_redis and redis_client:
                    msg_id = redis_client.xadd(
                        "babyoil:stream",
                        {
                            'data': json.dumps(payload),
                            'retry_count': '0',
                            'first_seen': datetime.utcnow().isoformat()
                        }
                    )
                    logger.info(f"📤 Redis: {msg_id}")
                
                if not publish_mqtt and not publish_redis:
                    print(json.dumps(payload, indent=2))
                    print("-" * 40)
                
                # Send heartbeat
                heartbeat_timer += interval
                if heartbeat_timer >= heartbeat_interval:
                    heartbeat = self.generate_heartbeat()
                    if publish_mqtt and mqtt_client:
                        mqtt_client.publish("bod/device/heartbeat", json.dumps(heartbeat))
                    elif publish_redis and redis_client:
                        redis_client.xadd("babyoil:heartbeat", {
                            'data': json.dumps(heartbeat),
                            'timestamp': datetime.utcnow().isoformat()
                        })
                    else:
                        print("💓 Heartbeat:")
                        print(json.dumps(heartbeat, indent=2))
                    heartbeat_timer = 0
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("🛑 Stopping simulator")
        except Exception as e:
            logger.error(f"Error: {e}")


# ============================================================================
# Payload Generators (For Testing)
# ============================================================================

def generate_status_payload(device_id: str = None, user_id: str = None) -> Dict:
    sim = BabyOilSimulator(device_id, user_id)
    sim.state = DeviceState.WAKE
    return sim._create_status_payload("power_on", "Device powered on")

def generate_connectivity_payload(device_id: str = None, user_id: str = None, offline: bool = False) -> Dict:
    sim = BabyOilSimulator(device_id, user_id)
    if offline:
        sim.go_offline()
    else:
        sim.go_online()
    return sim._create_connectivity_payload()

def generate_heartbeat_payload(device_id: str = None, user_id: str = None) -> Dict:
    sim = BabyOilSimulator(device_id, user_id)
    return sim.generate_heartbeat()

def generate_telemetry_payload(event: str, device_id: str = None, user_id: str = None) -> Dict:
    sim = BabyOilSimulator(device_id, user_id)
    if event == "weight_measured":
        sim.check_oil_volume()
    elif event == "warming":
        sim.pre_warm_neck_ring()
    elif event == "heating_tcc":
        sim.heat_to_target()
    elif event == "ready":
        sim.target_temp = 37.0
        sim.tcc_temp = 37.0
        sim.neck_ring_temp = 36.5
        sim.state = DeviceState.READY
        sim.session_status = SessionStatus.READY
    elif event == "dispensing":
        sim.dispense()
    elif event == "session_complete":
        sim.return_to_sleep()
    return sim._create_telemetry_payload(event, "Telemetry update")

def generate_fault_payload(fault_type: str, device_id: str = None, user_id: str = None) -> Dict:
    sim = BabyOilSimulator(device_id, user_id)
    if fault_type == "sensor_mismatch":
        sim.tcc_temp = 37.0
        sim.neck_ring_temp = 24.5
        sim.sensors_agree = False
        sim.sensor_anomaly_count = 1
        sim.anomaly_details = {"type": "SENSOR_MISMATCH", "message": "Sensor readings differ by 12.5°C", "delta": 12.5, "threshold": 2.0}
        sim.error_codes.append("TEMP_SENSOR_MISMATCH")
        sim.state = DeviceState.FAULT
    elif fault_type == "sensor_stuck":
        sim.tcc_temp = 20.0
        sim.neck_ring_temp = 20.0
        sim.heating_chamber_active = True
        sim.anomaly_details = {"type": "SENSOR_STUCK", "sensor": "NECK_RING", "message": "Neck ring sensor reading stuck at 24.0°C for 60 seconds", "duration": 60}
        sim.error_codes.append("TEMP_SENSOR_STUCK")
        sim.state = DeviceState.SAFE_MODE
    elif fault_type == "sensor_out_of_range":
        sim.tcc_temp = 55.0
        sim.neck_ring_temp = 24.0
        sim.anomaly_details = {"type": "OUT_OF_RANGE", "sensor": "TCC", "reading": 55.0, "range": "20-50°C"}
        sim.error_codes.append("TEMP_SENSOR_OUT_OF_RANGE")
        sim.state = DeviceState.FAULT
    return sim._create_fault_payload(fault_type, f"{fault_type.replace('_', ' ').title()} detected")

def generate_safety_payload(device_id: str = None, user_id: str = None) -> Dict:
    sim = BabyOilSimulator(device_id, user_id)
    sim.hard_stop_count = 3
    sim.hard_stop_state = "protected_fault"
    sim.safe_mode = True
    sim.state = DeviceState.SAFE_MODE
    sim.session_status = SessionStatus.SAFETY
    sim.error_codes.append("HARD_STOP_PROTECTED")
    return sim._create_safety_payload("hard_stop_protected", "Hard stop protection triggered")

def generate_ota_payload(ota_event: str, device_id: str = None, user_id: str = None) -> Dict:
    sim = BabyOilSimulator(device_id, user_id)
    sim.available_firmware = "v2.6.0"
    if ota_event == "ota_available":
        return sim._create_ota_payload("ota_available", "Firmware update available")
    elif ota_event == "ota_in_progress":
        sim.ota_status = "INSTALLING"
        sim.ota_progress = 70
        sim.state = DeviceState.OTA_UPDATING
        sim.ota_phase = "Installing firmware"
        return sim._create_ota_payload("ota_in_progress", "OTA update 70% complete")
    elif ota_event == "ota_complete":
        sim.firmware_version = "v2.6.0"
        return sim._create_ota_payload("ota_complete", "OTA update completed successfully")
    elif ota_event == "ota_failed":
        sim.ota_progress = 45
        sim.ota_status = "FAILED"
        return sim._create_ota_payload("ota_failed", "OTA update failed - Rolling back")
    return None

def generate_notification_payload(notif_type: str, device_id: str = None, user_id: str = None) -> Dict:
    sim = BabyOilSimulator(device_id, user_id)
    if notif_type == "low_oil":
        sim.oil_level = 8.2
        sim.remaining_sessions = 4
        return sim._send_notification("low_oil", "Low Oil Warning", 
                                      "Oil level at 8.2%. Please replace cartridge soon.",
                                      {"oil_level_percent": 8.2, "remaining_sessions": 4, "oil_volume_ml": 20.0})
    elif notif_type == "battery_low_warning":
        sim.battery_percent = 22.0
        return sim.check_battery()
    elif notif_type == "battery_critical":
        sim.battery_percent = 8.0
        return sim.check_battery()
    return None

def generate_profile_payload(profile_event: str, device_id: str = None, user_id: str = None) -> Dict:
    sim = BabyOilSimulator(device_id, user_id)
    if profile_event == "profile_configured":
        sim.active_baby["name"] = "Emma"
        sim.active_baby["target_temp"] = 37.0
        sim.active_baby["oil_volume"] = 5.0
        sim.active_baby["oil_type"] = "Coconut"
        return sim._create_profile_payload("profile_configured", "Baby profile configured and synced to cloud")
    elif profile_event == "profile_fetched":
        return sim._create_profile_payload("profile_fetched", "Profile fetched from cloud")
    elif profile_event == "profile_selected":
        sim.active_baby = sim.baby_profiles[0]
        return sim._create_profile_payload("profile_selected", f"Active profile set to {sim.active_baby['name']}")
    elif profile_event == "profile_update":
        sim.state = DeviceState.PROCESSING
        return sim._create_profile_payload("profile_update", "Profile reconfiguration - Processing reconfiguration")
    return None


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--device-id", help="Device ID")
    parser.add_argument("--user-id", help="User ID")
    parser.add_argument("--continuous", action="store_true", help="Run continuous mode")
    parser.add_argument("--interval", type=int, default=5, help="Interval in seconds")
    parser.add_argument("--mqtt", action="store_true", help="Publish via MQTT")
    parser.add_argument("--broker", default="localhost", help="MQTT broker")
    parser.add_argument("--port", type=int, default=1883, help="MQTT port")
    parser.add_argument("--redis", action="store_true", help="Publish to Redis")
    parser.add_argument("--stream", default="babyoil:stream", help="Redis stream")
    
    # Single payload generators
    parser.add_argument("--status", action="store_true", help="Generate status payload")
    parser.add_argument("--connectivity", action="store_true", help="Generate connectivity payload")
    parser.add_argument("--offline", action="store_true", help="Generate offline connectivity")
    parser.add_argument("--heartbeat", action="store_true", help="Generate heartbeat payload")
    parser.add_argument("--telemetry", choices=["weight_measured", "warming", "heating_tcc", "ready", "dispensing", "session_complete"], help="Generate telemetry payload")
    parser.add_argument("--fault", choices=["sensor_mismatch", "sensor_stuck", "sensor_out_of_range"], help="Generate fault payload")
    parser.add_argument("--safety", action="store_true", help="Generate safety payload")
    parser.add_argument("--ota", choices=["ota_available", "ota_in_progress", "ota_complete", "ota_failed"], help="Generate OTA payload")
    parser.add_argument("--notification", choices=["low_oil", "battery_low_warning", "battery_critical"], help="Generate notification payload")
    parser.add_argument("--profile", choices=["profile_configured", "profile_fetched", "profile_selected", "profile_update"], help="Generate profile payload")
    parser.add_argument("--all", action="store_true", help="Generate all example payloads")
    
    args = parser.parse_args()
    
    # All examples
    if args.all:
        print("\n" + "="*60)
        print("ALL PAYLOAD EXAMPLES")
        print("="*60)
        
        examples = [
            ("Status", generate_status_payload(args.device_id, args.user_id)),
            ("Connectivity Online", generate_connectivity_payload(args.device_id, args.user_id, False)),
            ("Connectivity Offline", generate_connectivity_payload(args.device_id, args.user_id, True)),
            ("Heartbeat", generate_heartbeat_payload(args.device_id, args.user_id)),
            ("Telemetry - Weight Measured", generate_telemetry_payload("weight_measured", args.device_id, args.user_id)),
            ("Telemetry - Warming", generate_telemetry_payload("warming", args.device_id, args.user_id)),
            ("Telemetry - Heating", generate_telemetry_payload("heating_tcc", args.device_id, args.user_id)),
            ("Telemetry - Ready", generate_telemetry_payload("ready", args.device_id, args.user_id)),
            ("Telemetry - Dispensing", generate_telemetry_payload("dispensing", args.device_id, args.user_id)),
            ("Telemetry - Session Complete", generate_telemetry_payload("session_complete", args.device_id, args.user_id)),
            ("Fault - Sensor Mismatch", generate_fault_payload("sensor_mismatch", args.device_id, args.user_id)),
            ("Fault - Sensor Stuck", generate_fault_payload("sensor_stuck", args.device_id, args.user_id)),
            ("Fault - Out of Range", generate_fault_payload("sensor_out_of_range", args.device_id, args.user_id)),
            ("Safety - Hard Stop", generate_safety_payload(args.device_id, args.user_id)),
            ("OTA - Available", generate_ota_payload("ota_available", args.device_id, args.user_id)),
            ("OTA - In Progress", generate_ota_payload("ota_in_progress", args.device_id, args.user_id)),
            ("OTA - Complete", generate_ota_payload("ota_complete", args.device_id, args.user_id)),
            ("OTA - Failed", generate_ota_payload("ota_failed", args.device_id, args.user_id)),
            ("Notification - Low Oil", generate_notification_payload("low_oil", args.device_id, args.user_id)),
            ("Notification - Battery Warning", generate_notification_payload("battery_low_warning", args.device_id, args.user_id)),
            ("Notification - Battery Critical", generate_notification_payload("battery_critical", args.device_id, args.user_id)),
            ("Profile - Configured", generate_profile_payload("profile_configured", args.device_id, args.user_id)),
            ("Profile - Fetched", generate_profile_payload("profile_fetched", args.device_id, args.user_id)),
            ("Profile - Selected", generate_profile_payload("profile_selected", args.device_id, args.user_id)),
            ("Profile - Update", generate_profile_payload("profile_update", args.device_id, args.user_id)),
        ]
        
        for name, payload in examples:
            if payload:
                print(f"\n{'='*60}")
                print(f"{name}")
                print('='*60)
                print(json.dumps(payload, indent=2))
        return
    
    # Single payload
    if args.status:
        payload = generate_status_payload(args.device_id, args.user_id)
    elif args.connectivity:
        payload = generate_connectivity_payload(args.device_id, args.user_id, args.offline)
    elif args.heartbeat:
        payload = generate_heartbeat_payload(args.device_id, args.user_id)
    elif args.telemetry:
        payload = generate_telemetry_payload(args.telemetry, args.device_id, args.user_id)
    elif args.fault:
        payload = generate_fault_payload(args.fault, args.device_id, args.user_id)
    elif args.safety:
        payload = generate_safety_payload(args.device_id, args.user_id)
    elif args.ota:
        payload = generate_ota_payload(args.ota, args.device_id, args.user_id)
    elif args.notification:
        payload = generate_notification_payload(args.notification, args.device_id, args.user_id)
    elif args.profile:
        payload = generate_profile_payload(args.profile, args.device_id, args.user_id)
    else:
        # Default: generate normal telemetry
        payload = generate_telemetry_payload("weight_measured", args.device_id, args.user_id)
    
    print(json.dumps(payload, indent=2))
    
    # Continuous mode
    if args.continuous:
        mqtt_client = None
        redis_client = None
        
        if args.mqtt:
            mqtt_client = mqtt.Client()
            mqtt_client.connect(args.broker, args.port, 60)
            print(f"✅ Connected to MQTT at {args.broker}:{args.port}")
        
        if args.redis:
            redis_client = Redis.from_url(
                os.getenv('REDIS_URL', 'redis://localhost:6379'),
                decode_responses=True
            )
            print("✅ Connected to Redis")
        
        sim = BabyOilSimulator(args.device_id, args.user_id)
        sim.run_continuous(
            interval=args.interval,
            publish_mqtt=args.mqtt,
            mqtt_client=mqtt_client,
            publish_redis=args.redis,
            redis_client=redis_client
        )


if __name__ == "__main__":
    main()