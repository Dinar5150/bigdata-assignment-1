"""
setup_cluster.py

Creates tables/indexes for PostgreSQL and Citus,
sets up Citus workers, ScyllaDB keyspace/tables, and MongoDB replica set.
Run this AFTER docker-compose up -d and waiting ~60s for services to start.
"""
import time
import psycopg2
from cassandra.cluster import Cluster
from pymongo import MongoClient

PG_SCHEMA = """
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
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(user_id),
    venue_id VARCHAR(64) REFERENCES pois(venue_id),
    utc_time TIMESTAMP,
    timezone_offset INTEGER
);
CREATE TABLE IF NOT EXISTS friendships_before (
    user_id INTEGER REFERENCES users(user_id),
    friend_id INTEGER REFERENCES users(user_id),
    PRIMARY KEY (user_id, friend_id)
);
CREATE TABLE IF NOT EXISTS friendships_after (
    user_id INTEGER REFERENCES users(user_id),
    friend_id INTEGER REFERENCES users(user_id),
    PRIMARY KEY (user_id, friend_id)
);
CREATE INDEX IF NOT EXISTS idx_checkins_user ON checkins(user_id);
CREATE INDEX IF NOT EXISTS idx_checkins_venue ON checkins(venue_id);
CREATE INDEX IF NOT EXISTS idx_pois_country ON pois(country);
CREATE INDEX IF NOT EXISTS idx_pois_category ON pois USING gin(to_tsvector('english', category));
"""

CITUS_SCHEMA = """
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
CREATE INDEX IF NOT EXISTS idx_checkins_venue ON checkins(venue_id);
CREATE INDEX IF NOT EXISTS idx_pois_country ON pois(country);
CREATE INDEX IF NOT EXISTS idx_pois_category ON pois USING gin(to_tsvector('english', category));
"""

def setup_postgres():
    print("=== Setting up PostgreSQL ===")
    conn = psycopg2.connect(host="localhost", port=5432, dbname="foursquaredb", user="user", password="pass")
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(PG_SCHEMA)
    cur.close()
    conn.close()
    print("PostgreSQL ready.")

def setup_citus():
    print("=== Setting up Citus cluster ===")
    conn = psycopg2.connect(host="localhost", port=5433, dbname="foursquaredb", user="user", password="pass")
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(CITUS_SCHEMA)

    cur.execute("SELECT master_add_node('citus_worker1', 5432);")
    cur.execute("SELECT master_add_node('citus_worker2', 5432);")
    cur.execute("SELECT create_distributed_table('checkins', 'user_id');")
    cur.execute("SELECT create_distributed_table('friendships_before', 'user_id');")
    cur.execute("SELECT create_distributed_table('friendships_after', 'user_id');")
    cur.execute("SELECT create_reference_table('users');")
    cur.execute("SELECT create_reference_table('pois');")

    cur.close()
    conn.close()
    print("Citus cluster ready.")

def setup_scylla():
    print("=== Setting up ScyllaDB ===")
    cluster = Cluster(["127.0.0.1"], port=9042)
    session = cluster.connect()

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS foursquaredb
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}
    """)
    session.set_keyspace("foursquaredb")

    session.execute("""
        CREATE TABLE IF NOT EXISTS checkins_by_country (
            country TEXT, venue_id TEXT, user_id INT, utc_time TIMESTAMP,
            timezone_offset INT, latitude DOUBLE, longitude DOUBLE, category TEXT,
            PRIMARY KEY (country, venue_id, user_id, utc_time)
        )
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS checkins_by_user (
            user_id INT, venue_id TEXT, utc_time TIMESTAMP, timezone_offset INT,
            country TEXT, latitude DOUBLE, longitude DOUBLE, category TEXT,
            PRIMARY KEY (user_id, utc_time, venue_id)
        )
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS friendships_before (
            user_id INT, friend_id INT, PRIMARY KEY (user_id, friend_id)
        )
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS friendships_after (
            user_id INT, friend_id INT, PRIMARY KEY (user_id, friend_id)
        )
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS pois (
            venue_id TEXT PRIMARY KEY, latitude DOUBLE, longitude DOUBLE, category TEXT, country TEXT
        )
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS users (user_id INT PRIMARY KEY)
    """)

    session.shutdown()
    cluster.shutdown()
    print("ScyllaDB ready.")

def setup_mongo():
    print("=== Setting up MongoDB replica set ===")
    client = MongoClient("mongodb://localhost:27017/", directConnection=True)

    client.admin.command("replSetInitiate", {
        "_id": "rs0",
        "members": [
            {"_id": 0, "host": "mongo1:27017", "priority": 2},
            {"_id": 1, "host": "mongo2:27017", "priority": 1},
            {"_id": 2, "host": "mongo3:27017", "priority": 1}
        ]
    })
    print("Replica set initiated, waiting for primary election...")

    # wait until this node becomes primary
    for _ in range(30):
        status = client.admin.command("replSetGetStatus")
        for m in status["members"]:
            if m["name"] == "mongo1:27017" and m["stateStr"] == "PRIMARY":
                break
        else:
            time.sleep(2)
            continue
        break

    db = client["foursquaredb"]
    db.checkins.create_index("user_id")
    db.checkins.create_index("venue_id")
    db.checkins.create_index("country")
    db.pois.create_index("country")
    db.pois.create_index([("category", "text")])
    db.friendships_before.create_index("user_id")
    db.friendships_after.create_index("user_id")
    client.close()
    print("MongoDB ready.")

if __name__ == "__main__":
    setup_postgres()
    setup_citus()
    setup_scylla()
    setup_mongo()
    print("\nAll clusters configured!")
