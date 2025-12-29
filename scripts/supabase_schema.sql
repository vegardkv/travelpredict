-- Deviations table for storing travel prediction data
-- Run this SQL in your Supabase SQL Editor to create the table

CREATE TABLE deviations (
    id BIGSERIAL PRIMARY KEY,
    aimed_arrival TIMESTAMP NOT NULL,
    line_id TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    realtime BOOLEAN NOT NULL DEFAULT TRUE,
    aimed_departure TIMESTAMP NOT NULL,
    expected_arrival TIMESTAMP NOT NULL,
    expected_departure TIMESTAMP NOT NULL,
    quay_id TEXT NOT NULL,
    line_name TEXT NOT NULL,
    transport_mode TEXT NOT NULL,
    expected_delay_seconds INTEGER NOT NULL,
    timestamp_delay_seconds INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    
    -- Unique constraint on the business key (aimed_arrival, line_id)
    CONSTRAINT unique_aimed_arrival_line_id UNIQUE (aimed_arrival, line_id)
);

-- Index for querying by line_id
CREATE INDEX idx_deviations_line_id ON deviations(line_id);

-- Index for time-based queries
CREATE INDEX idx_deviations_aimed_arrival ON deviations(aimed_arrival);

-- Index for timestamp queries
CREATE INDEX idx_deviations_timestamp ON deviations(timestamp);

COMMENT ON TABLE deviations IS 'Travel deviations data from Entur real-time API';
COMMENT ON COLUMN deviations.expected_delay_seconds IS 'Delay in seconds: expected_arrival - aimed_arrival';
COMMENT ON COLUMN deviations.timestamp_delay_seconds IS 'Delay in seconds: timestamp - aimed_arrival';
