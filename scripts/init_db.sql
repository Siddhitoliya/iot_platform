-- Create table for oxygen readings
CREATE TABLE IF NOT EXISTS oxygen_readings (
    id BIGSERIAL PRIMARY KEY,
    
    -- System metadata
    device_id VARCHAR(50) NOT NULL,
    hospital_id VARCHAR(50) NOT NULL,
    facility_zone VARCHAR(100),
    firmware_version VARCHAR(20),
    
    -- Tank metrics
    liquid_level_percent DECIMAL(5,2),
    liquid_volume_liters DECIMAL(10,1),
    current_weight_kg DECIMAL(10,1),
    head_pressure_bar DECIMAL(5,2),
    tank_temperature_celsius DECIMAL(6,2),
    vacuum_jacket_pressure_millibar DECIMAL(6,3),
    
    -- Valve states
    main_isolation_valve VARCHAR(10),
    bypass_valve VARCHAR(10),
    economizer_valve_position_percent DECIMAL(5,2),
    pressure_building_valve VARCHAR(10),
    vaporizer_outlet_temperature_celsius DECIMAL(5,2),
    vaporizer_frost_index DECIMAL(5,3),
    
    -- Pipeline distribution
    output_flow_rate_m3_per_hour DECIMAL(10,1),
    pipeline_pressure_bar DECIMAL(5,2),
    gas_purity_percentage DECIMAL(5,2),
    
    -- System health
    battery_backup_percent DECIMAL(5,2),
    power_source VARCHAR(20),
    network_signal_dbm INTEGER,
    active_error_codes JSONB DEFAULT '[]'::jsonb,
    
    -- Timestamps
    event_time TIMESTAMP WITH TIME ZONE NOT NULL,
    received_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Metadata
    is_anomaly BOOLEAN DEFAULT FALSE,
    processing_status VARCHAR(20) DEFAULT 'PENDING'
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_oxygen_device ON oxygen_readings(device_id);
CREATE INDEX IF NOT EXISTS idx_oxygen_hospital ON oxygen_readings(hospital_id);
CREATE INDEX IF NOT EXISTS idx_oxygen_event_time ON oxygen_readings(event_time DESC);
CREATE INDEX IF NOT EXISTS idx_oxygen_anomaly ON oxygen_readings(is_anomaly) WHERE is_anomaly = true;

-- Create view for current tank status
CREATE OR REPLACE VIEW current_tank_status AS
SELECT DISTINCT ON (device_id)
    device_id,
    hospital_id,
    facility_zone,
    liquid_level_percent,
    liquid_volume_liters,
    pipeline_pressure_bar,
    gas_purity_percentage,
    active_error_codes,
    event_time,
    received_at
FROM oxygen_readings
ORDER BY device_id, event_time DESC;

-- Create materialized view for hospital summary
CREATE MATERIALIZED VIEW IF NOT EXISTS hospital_summary AS
SELECT 
    hospital_id,
    COUNT(DISTINCT device_id) as total_tanks,
    AVG(liquid_level_percent) as avg_liquid_level,
    AVG(pipeline_pressure_bar) as avg_pipeline_pressure,
    AVG(gas_purity_percentage) as avg_purity,
    COUNT(*) FILTER (WHERE is_anomaly = true) as anomaly_count,
    MAX(event_time) as last_update
FROM oxygen_readings
WHERE event_time > NOW() - INTERVAL '1 hour'
GROUP BY hospital_id;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO pipeline;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO pipeline;