-- Citus init (distributed tables)
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS pois (
    venue_id VARCHAR(64) PRIMARY KEY,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    category VARCHAR(256),
    country VARCHAR(4)
);

CREATE TABLE IF NOT EXISTS checkins (
    id SERIAL,
    user_id INTEGER,
    venue_id VARCHAR(64),
    utc_time TIMESTAMP,
    timezone_offset INTEGER,
    PRIMARY KEY (user_id, id)
);

CREATE TABLE IF NOT EXISTS friendships_before (
    user_id INTEGER,
    friend_id INTEGER,
    PRIMARY KEY (user_id, friend_id)
);

CREATE TABLE IF NOT EXISTS friendships_after (
    user_id INTEGER,
    friend_id INTEGER,
    PRIMARY KEY (user_id, friend_id)
);

CREATE INDEX idx_checkins_venue ON checkins(venue_id);
CREATE INDEX idx_pois_country ON pois(country);
CREATE INDEX idx_pois_category ON pois USING gin(to_tsvector('english', category));

-- distribute after workers are added (done in setup_cluster.py)
