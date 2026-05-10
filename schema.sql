PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS readings (
    id                   INTEGER PRIMARY KEY,
    received_at          TEXT    NOT NULL,
    mac                  TEXT    NOT NULL,
    data_format          INTEGER,
    temperature_c        REAL,
    humidity_pct         REAL,
    pressure_hpa         REAL,
    acceleration_x_mg    INTEGER,
    acceleration_y_mg    INTEGER,
    acceleration_z_mg    INTEGER,
    battery_mv           INTEGER,
    tx_power_dbm         INTEGER,
    movement_counter     INTEGER,
    measurement_sequence INTEGER,
    rssi_dbm             INTEGER,
    epoch_ms             INTEGER GENERATED ALWAYS AS
                          (CAST(unixepoch(received_at) * 1000 AS INTEGER))
);

CREATE INDEX IF NOT EXISTS idx_readings_mac_time
    ON readings (mac, received_at);

CREATE INDEX IF NOT EXISTS idx_readings_epoch
    ON readings (epoch_ms);

CREATE INDEX IF NOT EXISTS idx_readings_mac_epoch
    ON readings (mac, epoch_ms);
