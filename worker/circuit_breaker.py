#!/usr/bin/env python3
"""
Circuit Breaker Pattern Implementation
"""
import time
import threading
from enum import Enum
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 5, 
                 timeout_seconds: int = 30, half_open_max: int = 3):
        self.name = name
        self.failure_threshold = failure_threshold
        self.timeout_seconds = timeout_seconds
        self.half_open_max = half_open_max
        
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.last_state_change = datetime.utcnow()
        self.lock = threading.Lock()
        
        self.total_failures = 0
        self.total_successes = 0
        
        logger.info(f"🔌 Circuit breaker '{name}' initialized (threshold={failure_threshold})")
    
    def call(self, func, *args, **kwargs):
        if not self.is_allowed():
            return False, None, f"Circuit OPEN - {self.name}"
        
        try:
            result = func(*args, **kwargs)
            self.record_success()
            return True, result, None
            
        except Exception as e:
            self.record_failure()
            return False, None, str(e)
    
    def is_allowed(self) -> bool:
        with self.lock:
            if self.state == CircuitState.CLOSED:
                return True
            
            elif self.state == CircuitState.OPEN:
                if self.last_failure_time:
                    elapsed = (datetime.utcnow() - self.last_failure_time).total_seconds()
                    if elapsed >= self.timeout_seconds:
                        logger.info(f"🔄 Circuit '{self.name}' entering HALF_OPEN state")
                        self.state = CircuitState.HALF_OPEN
                        self.success_count = 0
                        self.last_state_change = datetime.utcnow()
                        return True
                return False
            
            elif self.state == CircuitState.HALF_OPEN:
                if self.success_count < self.half_open_max:
                    return True
                return False
            
            return False
    
    def record_success(self):
        with self.lock:
            self.total_successes += 1
            
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.half_open_max:
                    logger.info(f"✅ Circuit '{self.name}' closed (recovered)")
                    self.state = CircuitState.CLOSED
                    self.failure_count = 0
                    self.success_count = 0
                    self.last_state_change = datetime.utcnow()
            
            elif self.state == CircuitState.CLOSED:
                self.failure_count = max(0, self.failure_count - 1)
    
    def record_failure(self):
        with self.lock:
            self.total_failures += 1
            self.last_failure_time = datetime.utcnow()
            
            if self.state == CircuitState.CLOSED:
                self.failure_count += 1
                if self.failure_count >= self.failure_threshold:
                    logger.warning(f"🔴 Circuit '{self.name}' OPEN (failures: {self.failure_count})")
                    self.state = CircuitState.OPEN
                    self.last_state_change = datetime.utcnow()
            
            elif self.state == CircuitState.HALF_OPEN:
                logger.warning(f"🔴 Circuit '{self.name}' OPEN (test failed)")
                self.state = CircuitState.OPEN
                self.last_state_change = datetime.utcnow()
                self.success_count = 0
    
    def get_status(self) -> Dict[str, Any]:
        with self.lock:
            return {
                'name': self.name,
                'state': self.state.value,
                'failure_count': self.failure_count,
                'success_count': self.success_count,
                'total_failures': self.total_failures,
                'total_successes': self.total_successes,
                'last_failure': self.last_failure_time.isoformat() if self.last_failure_time else None,
                'state_since': self.last_state_change.isoformat(),
                'is_allowed': self.is_allowed()
            }

class CircuitBreakerRegistry:
    def __init__(self):
        self.breakers: Dict[str, CircuitBreaker] = {}
        self.lock = threading.Lock()
    
    def get_or_create(self, name: str, failure_threshold: int = 5, 
                      timeout_seconds: int = 30) -> CircuitBreaker:
        with self.lock:
            if name not in self.breakers:
                self.breakers[name] = CircuitBreaker(name, failure_threshold, timeout_seconds)
            return self.breakers[name]
    
    def get_status(self) -> Dict[str, Dict]:
        return {name: cb.get_status() for name, cb in self.breakers.items()}

circuit_breaker_registry = CircuitBreakerRegistry()