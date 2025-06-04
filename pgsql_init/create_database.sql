DROP DATABASE saivasdata;

CREATE DATABASE saivasdata;

\c saivasdata;

CREATE EXTENSION IF NOT EXISTS  postgis;

CREATE TABLE IF NOT EXISTS session_data (
    sessionid UUID NOT NULL PRIMARY KEY,  
    devicename VARCHAR(50) NOT NULL,
    profilenumber INT NOT NULL,
    startdatetime TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
    airtemp FLOAT,
    location GEOMETRY(Point, 4326),  
    filename VARCHAR(100),
    windspeed FLOAT,
    winddirection FLOAT,
    airpressure FLOAT
);

CREATE TABLE IF NOT EXISTS raw_timeseries (
    id SERIAL PRIMARY KEY,
    sessionid UUID REFERENCES session_data(sessionid) ON DELETE CASCADE,
    seq INT NOT NULL,
    salinity FLOAT, 
    temperature FLOAT, 
    pressure_dbar FLOAT NOT NULL,
    oxygen FLOAT,
    fluorescence FLOAT,
    turbidity FLOAT
);

CREATE TABLE IF NOT EXISTS interpolated_timeseries (
    id SERIAL PRIMARY KEY,
    sessionid UUID NOT NULL REFERENCES session_data(sessionid) ON DELETE CASCADE,
    seq INT NOT NULL,
    salinity FLOAT, 
    temperature FLOAT, 
    pressure_dbar FLOAT NOT NULL,
    oxygen FLOAT,
    fluorescence FLOAT,
    turbidity FLOAT
);


CREATE INDEX idx_raw_timeseries_sessionid ON raw_timeseries(sessionid);
CREATE INDEX idx_interpolated_timeseries_sessionid ON interpolated_timeseries(sessionid);


-- CREATE USER gabriel_read WITH PASSWORD 'your_readonly_password';
GRANT CONNECT ON DATABASE saivasdata TO gabriel_read;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO gabriel_read;
-- CREATE USER gabriel_update WITH PASSWORD 'your_update_password';
GRANT CONNECT ON DATABASE saivasdata TO gabriel_update;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO gabriel_update;
-- ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO gabriel_read;
-- ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO gabriel_update;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO gabriel_update ;
