"""
Backpressure Implementation
Controls flow rate based on system load
"""
import time
import threading
from enum import Enum
from typing import Dict, Any, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class PressureLevel(Enum):
    """Backpressure levels"""
    NORMAL = "normal"       
    MODERATE = "moderate"   
    HIGH = "high"          
    CRITICAL = "critical"   
    STOPPED = "stopped"     

class BackpressureController:
    """
    Controls processing rate based on system load
    
    Levels:
        0-50%:   NORMAL   - Full speed
        50-70%:  MODERATE - 50% speed
        70-85%:  HIGH     - 20% speed
        85-95%:  CRITICAL - 5% speed
        95-100%: STOPPED  - No processing
    """
    
    def __init__(self, queue_size_limit: int = 1000):
        self.queue_size_limit = queue_size_limit
        self.current_level = PressureLevel.NORMAL
        self.current_speed = 1.0  
        self.running = False
        self.lock = threading.Lock()
        
        self.stats = {
            'level_changes': 0,
            'last_change': None,
            'total_seconds_at_normal': 0,
            'total_seconds_at_moderate': 0,
            'total_seconds_at_high': 0,
            'total_seconds_at_critical': 0,
            'total_seconds_at_stopped': 0
        }
        self._level_start_time = datetime.utcnow()
    
    def update(self, current_queue_size: int) -> PressureLevel:
        """
        Update backpressure level based on queue size
        Returns: New pressure level
        """
        with self.lock:
            load_percent = (current_queue_size / self.queue_size_limit) * 100
            
            if load_percent >= 95:
                new_level = PressureLevel.STOPPED
                speed = 0.0
            elif load_percent >= 85:
                new_level = PressureLevel.CRITICAL
                speed = 0.05
            elif load_percent >= 70:
                new_level = PressureLevel.HIGH
                speed = 0.2
            elif load_percent >= 50:
                new_level = PressureLevel.MODERATE
                speed = 0.5
            else:
                new_level = PressureLevel.NORMAL
                speed = 1.0
            
            if self.current_level != new_level:
                self._track_level_duration()
                self.current_level = new_level
                self._level_start_time = datetime.utcnow()
                self.stats['level_changes'] += 1
                self.stats['last_change'] = datetime.utcnow().isoformat()
                logger.info(f"📊 Backpressure: {new_level.value} (load: {load_percent:.1f}%, speed: {speed*100:.0f}%)")
            
            self.current_speed = speed
            return new_level
    
    def _track_level_duration(self):
        """Track time spent at each level"""
        now = datetime.utcnow()
        duration = (now - self._level_start_time).total_seconds()
        
        level_map = {
            PressureLevel.NORMAL: 'total_seconds_at_normal',
            PressureLevel.MODERATE: 'total_seconds_at_moderate',
            PressureLevel.HIGH: 'total_seconds_at_high',
            PressureLevel.CRITICAL: 'total_seconds_at_critical',
            PressureLevel.STOPPED: 'total_seconds_at_stopped'
        }
        key = level_map.get(self.current_level)
        if key:
            self.stats[key] = self.stats.get(key, 0) + duration
    
    def should_process(self) -> bool:
        """Check if processing should continue"""
        with self.lock:
            return self.current_level != PressureLevel.STOPPED
    
    def get_speed(self) -> float:
        """Get current processing speed multiplier"""
        with self.lock:
            return self.current_speed
    
    def wait_if_needed(self):
        """Wait based on current backpressure level"""
        speed = self.get_speed()
        if speed < 1.0 and speed > 0:
            delay = (1 - speed) * 0.5 
            time.sleep(delay)
        elif speed == 0:
            time.sleep(5)
    
    def get_status(self) -> Dict[str, Any]:
        """Get current status"""
        with self.lock:
            return {
                'level': self.current_level.value,
                'speed': self.current_speed,
                'queue_limit': self.queue_size_limit,
                'stats': self.stats
            }