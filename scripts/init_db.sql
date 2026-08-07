CREATE TABLE IF NOT EXISTS oxygen_readings (
    id BIGSERIAL PRIMARY KEY,
    device_id VARCHAR(50) NOT NULL,
    hospital_id VARCHAR(50) NOT NULL,
    facility_zone VARCHAR(100),
    firmware_version VARCHAR(20),
    liquid_level_percent DECIMAL(5,2),
    liquid_volume_liters DECIMAL(10,1),
    current_weight_kg DECIMAL(10,1),
    head_pressure_bar DECIMAL(5,2),
    tank_temperature_celsius DECIMAL(6,2),
    vacuum_jacket_pressure_millibar DECIMAL(6,3),
    main_isolation_valve VARCHAR(10),
    bypass_valve VARCHAR(10),
    economizer_valve_position_percent DECIMAL(5,2),
    pressure_building_valve VARCHAR(10),
    vaporizer_outlet_temperature_celsius DECIMAL(5,2),
    vaporizer_frost_index DECIMAL(5,3),
    output_flow_rate_m3_per_hour DECIMAL(10,1),
    pipeline_pressure_bar DECIMAL(5,2),
    gas_purity_percentage DECIMAL(5,2),
    battery_backup_percent DECIMAL(5,2),
    power_source VARCHAR(20),
    network_signal_dbm INTEGER,
    active_error_codes JSONB DEFAULT '[]'::jsonb,
    event_time TIMESTAMP WITH TIME ZONE NOT NULL,
    received_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_anomaly BOOLEAN DEFAULT FALSE
);

CREATE INDEX idx_oxygen_device ON oxygen_readings(device_id);
CREATE INDEX idx_oxygen_event_time ON oxygen_readings(event_time DESC);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO pipeline;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO pipeline;