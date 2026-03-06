"""
setup_cluster.py

Sets up Citus workers, ScyllaDB keyspace/tables, and MongoDB replica set.
Run this AFTER docker-compose up -d and waiting ~30s for services to start.
"""
import time
import psycopg2
from cassandra.cluster import Cluster
from pymongo import MongoClient

def setup_citus():
    print("=== Setting up Citus cluster ===")
    conn = psycopg2.connect(host="localhost", port=5433, dbname="foursquaredb", user="user", password="pass")
    conn.autocommit = True
    cur = conn.cursor()

    # add workers
    cur.execute("SELECT master_add_node('citus_worker1', 5432);")
    cur.execute("SELECT master_add_node('citus_worker2', 5432);")

    # distribute tables
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
            country TEXT,
            venue_id TEXT,
            user_id INT,
            utc_time TIMESTAMP,
            timezone_offset INT,
            latitude DOUBLE,
            longitude DOUBLE,
            category TEXT,
            PRIMARY KEY (country, venue_id, user_id, utc_time)
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS checkins_by_user (
            user_id INT,
            venue_id TEXT,
            utc_time TIMESTAMP,
            timezone_offset INT,
            country TEXT,
            latitude DOUBLE,
            longitude DOUBLE,
            category TEXT,
            PRIMARY KEY (user_id, utc_time, venue_id)
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS friendships_before (
            user_id INT,
            friend_id INT,
            PRIMARY KEY (user_id, friend_id)
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS friendships_after (
            user_id INT,
            friend_id INT,
            PRIMARY KEY (user_id, friend_id)
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS pois (
            venue_id TEXT PRIMARY KEY,
            latitude DOUBLE,
            longitude DOUBLE,
            category TEXT,
            country TEXT
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INT PRIMARY KEY
        )
    """)

    session.shutdown()
    cluster.shutdown()
    print("ScyllaDB ready.")

def setup_mongo():
    print("=== Setting up MongoDB replica set ===")
    client = MongoClient("mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?directConnection=true", serverSelectionTimeoutMS=5000)
    # try connecting directly to mongo1 first
    client = MongoClient("mongodb://localhost:27017/", directConnection=True)
    try:
        client.admin.command("replSetInitiate", {
            "_id": "rs0",
            "members": [
                {"_id": 0, "host": "mongo1:27017"},
                {"_id": 1, "host": "mongo2:27017"},
                {"_id": 2, "host": "mongo3:27017"}
            ]
        })
        print("Replica set initiated, waiting 10s for election...")
        time.sleep(10)
    except Exception as e:
        if "already initialized" in str(e):
            print("Replica set already initialized.")
        else:
            raise e

    # connect to replica set
    client = MongoClient("mongodb://localhost:27017/?replicaSet=rs0&directConnection=false")
    db = client["foursquaredb"]

    # create collections with validation (optional, just to make them exist)
    for name in ["users", "pois", "checkins", "friendships_before", "friendships_after"]:
        if name not in db.list_collection_names():
            db.create_collection(name)

    # indexes
    db.checkins.create_index("user_id")
    db.checkins.create_index("venue_id")
    db.pois.create_index("country")
    db.pois.create_index([("category", "text")])
    db.friendships_before.create_index("user_id")
    db.friendships_after.create_index("user_id")

    client.close()
    print("MongoDB ready.")

if __name__ == "__main__":
    print("Waiting a bit for containers to fully start...")
    time.sleep(5)

    setup_citus()
    setup_scylla()
    setup_mongo()
    print("\nAll clusters configured!")
