-- ONE table for the whole live dashboard. Run before starting the stream:
--   Get-Content streaming\create_tables.sql | docker compose exec -T postgres psql -U postgres -d traffic
-- WARNING: this recreates trips_live, so it empties it.
 
DROP VIEW IF EXISTS zone_hourly, hourly_overall, payment_mix, hourly_payment,
                    distance_buckets, passenger_mix, weekday_hour, sankey_routes;
DROP TABLE IF EXISTS scored_trips, zone_hour_metrics, zone_hour_live, trips_live;
 
CREATE TABLE trips_live (
    batch_id               BIGINT,
    -- time
    pickup_ts              TIMESTAMP,
    dropoff_ts             TIMESTAMP,
    -- raw fields from the TLC data dictionary
    vendor_id              INTEGER,
    passenger_count        INTEGER,
    trip_distance          DOUBLE PRECISION,
    ratecode_id            INTEGER,
    store_and_fwd_flag     TEXT,
    pu_zone                INTEGER,
    do_zone                INTEGER,
    payment_type           INTEGER,
    fare_amount            DOUBLE PRECISION,
    extra                  DOUBLE PRECISION,
    mta_tax                DOUBLE PRECISION,
    tip_amount             DOUBLE PRECISION,
    tolls_amount           DOUBLE PRECISION,
    improvement_surcharge  DOUBLE PRECISION,
    total_amount           DOUBLE PRECISION,
    congestion_surcharge   DOUBLE PRECISION,
    airport_fee            DOUBLE PRECISION,
    -- derived numbers
    duration_minutes       DOUBLE PRECISION,
    speed_mph              DOUBLE PRECISION,
    -- derived categories (text labels, ready to use as chart dimensions)
    pickup_hour            INTEGER,
    weekday                TEXT,   -- '1 Mon' ... '7 Sun'
    day_type               TEXT,   -- Weekday / Weekend
    time_of_day            TEXT,   -- '1 Night' ... '4 Evening'
    vendor_name            TEXT,
    rate_name              TEXT,
    payment_name           TEXT,
    tip_status             TEXT,
    trip_type              TEXT,   -- Airport / City
    distance_bucket        TEXT,
    group_size             TEXT,
    traffic_level          TEXT
);
CREATE INDEX idx_trips_live_dropoff ON trips_live (dropoff_ts);
 
