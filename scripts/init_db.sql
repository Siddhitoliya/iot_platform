-- Baby Oil Dispenser Database Schema

-- Users Table
CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(50) PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    mobile VARCHAR(20) UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    name VARCHAR(100),
    role VARCHAR(20) DEFAULT 'owner',
    is_active BOOLEAN DEFAULT true,
    last_login TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Devices Table
CREATE TABLE IF NOT EXISTS devices (
    device_id VARCHAR(50) PRIMARY KEY,
    owner_id VARCHAR(50) REFERENCES users(user_id),
    device_name VARCHAR(100),
    firmware_version VARCHAR(20),
    model VARCHAR(50),
    manufacturer VARCHAR(100),
    provision_status VARCHAR(20) DEFAULT 'pending',
    wifi_status VARCHAR(20),
    ble_mac VARCHAR(17),
    serial_number VARCHAR(50),
    last_seen TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Baby Profiles Table
CREATE TABLE IF NOT EXISTS baby_profiles (
    profile_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) REFERENCES users(user_id),
    baby_name VARCHAR(50) NOT NULL,
    age_months INTEGER,
    weight_kg DECIMAL(5,2),
    target_temperature DECIMAL(4,1) DEFAULT 37.0,
    oil_volume_per_session DECIMAL(5,2) DEFAULT 5.0,
    oil_type VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sessions Table
CREATE TABLE IF NOT EXISTS sessions (
    session_id VARCHAR(50) PRIMARY KEY,
    device_id VARCHAR(50) REFERENCES devices(device_id),
    user_id VARCHAR(50) REFERENCES users(user_id),
    profile_id VARCHAR(50) REFERENCES baby_profiles(profile_id),
    baby_name VARCHAR(50),
    status VARCHAR(20),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    oil_type VARCHAR(50),
    target_temperature DECIMAL(4,1),
    oil_dispensed_ml DECIMAL(8,2),
    remaining_oil_ml DECIMAL(8,2),
    tcc_temp_start DECIMAL(5,2),
    tcc_temp_end DECIMAL(5,2),
    neck_ring_temp_start DECIMAL(5,2),
    neck_ring_temp_end DECIMAL(5,2),
    max_temperature DECIMAL(5,2),
    min_temperature DECIMAL(5,2),
    avg_temperature DECIMAL(5,2),
    tcc_sensor_healthy BOOLEAN,
    neck_ring_sensor_healthy BOOLEAN,
    sensor_sync BOOLEAN,
    sensor_anomalies JSONB DEFAULT '[]',
    errors JSONB DEFAULT '[]',
    warnings JSONB DEFAULT '[]',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Device State Table
CREATE TABLE IF NOT EXISTS device_states (
    device_id VARCHAR(50) PRIMARY KEY REFERENCES devices(device_id),
    state VARCHAR(30),
    session_status VARCHAR(30),
    oil_level DECIMAL(5,2),
    oil_volume_ml DECIMAL(8,2),
    remaining_sessions INTEGER,
    tcc_temperature DECIMAL(5,2),
    neck_ring_temperature DECIMAL(5,2),
    ambient_temperature DECIMAL(5,2),
    target_temperature DECIMAL(5,2),
    temperature_delta DECIMAL(3,2),
    tcc_sensor_ok BOOLEAN DEFAULT true,
    neck_ring_sensor_ok BOOLEAN DEFAULT true,
    sensors_agree BOOLEAN DEFAULT true,
    sensor_anomaly_count INTEGER DEFAULT 0,
    last_sensor_anomaly TIMESTAMP,
    battery DECIMAL(5,2),
    wifi_status VARCHAR(20),
    ble_status VARCHAR(20),
    errors JSONB DEFAULT '[]',
    warnings JSONB DEFAULT '[]',
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Heartbeat History
CREATE TABLE IF NOT EXISTS heartbeat_history (
    heartbeat_id VARCHAR(50) PRIMARY KEY,
    device_id VARCHAR(50) REFERENCES devices(device_id),
    timestamp TIMESTAMP,
    status VARCHAR(20),
    battery DECIMAL(5,2),
    signal_strength INTEGER,
    cpu_usage DECIMAL(5,2),
    memory_usage DECIMAL(5,2),
    session_active BOOLEAN,
    errors JSONB DEFAULT '[]',
    warnings JSONB DEFAULT '[]',
    sequence_number INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Offline Logs
CREATE TABLE IF NOT EXISTS offline_logs (
    log_id BIGSERIAL PRIMARY KEY,
    device_id VARCHAR(50) REFERENCES devices(device_id),
    timestamp TIMESTAMP,
    event VARCHAR(255),
    data JSONB,
    synced BOOLEAN DEFAULT false,
    synced_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- OTA Updates
CREATE TABLE IF NOT EXISTS ota_updates (
    update_id VARCHAR(50) PRIMARY KEY,
    device_id VARCHAR(50) REFERENCES devices(device_id),
    current_version VARCHAR(20),
    target_version VARCHAR(20),
    status VARCHAR(20),
    progress INTEGER DEFAULT 0,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Temperature Events
CREATE TABLE IF NOT EXISTS temperature_events (
    event_id BIGSERIAL PRIMARY KEY,
    device_id VARCHAR(50) REFERENCES devices(device_id),
    event_time TIMESTAMP,
    tcc_temperature DECIMAL(5,2),
    neck_ring_temperature DECIMAL(5,2),
    target_temperature DECIMAL(5,2),
    is_anomaly BOOLEAN DEFAULT false,
    anomaly_type VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Indexes
CREATE INDEX idx_sessions_device ON sessions(device_id);
CREATE INDEX idx_sessions_user ON sessions(user_id);
CREATE INDEX idx_sessions_time ON sessions(start_time DESC);
CREATE INDEX idx_device_states_device ON device_states(device_id);
CREATE INDEX idx_heartbeat_device_time ON heartbeat_history(device_id, timestamp DESC);
CREATE INDEX idx_temp_events_device_time ON temperature_events(device_id, event_time DESC);
CREATE INDEX idx_temp_events_anomaly ON temperature_events(is_anomaly) WHERE is_anomaly = true;
CREATE INDEX idx_offline_logs_device ON offline_logs(device_id);
CREATE INDEX idx_offline_logs_synced ON offline_logs(synced) WHERE synced = false;

-- Grant Permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO pipeline;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO pipeline;