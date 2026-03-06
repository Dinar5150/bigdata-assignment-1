"""
ingest_scylladb.py

Loads my_* data files into ScyllaDB.
Measures and reports total ingestion time.
"""
import time
import csv
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement

DATA = "foursquare_dataset"
BATCH_SIZE = 50  # scylla batches should be small

def parse_time(s):
    # format: "Tue Apr 03 18:00:08 +0000 2012"
    return datetime.strptime(s, "%a %b %d %H:%M:%S %z %Y")

def ingest():
    cluster = Cluster(["127.0.0.1"], port=9042)
    session = cluster.connect("foursquaredb")

    start = time.time()

    # load pois into a dict for denormalized checkin tables
    print("Loading POIs into memory...")
    pois = {}
    with open(f"{DATA}/my_POIs.tsv") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) == 5:
                pois[parts[0]] = {
                    "latitude": float(parts[1]),
                    "longitude": float(parts[2]),
                    "category": parts[3],
                    "country": parts[4]
                }
    print(f"  {len(pois)} POIs loaded.")

    # users
    print("Ingesting users...")
    prep = session.prepare("INSERT INTO users (user_id) VALUES (?)")
    with open(f"{DATA}/my_users.csv") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            session.execute(prep, (int(row[0]),))
    print("  users done.")

    # pois table
    print("Ingesting POIs...")
    prep = session.prepare("INSERT INTO pois (venue_id, latitude, longitude, category, country) VALUES (?,?,?,?,?)")
    with open(f"{DATA}/my_POIs.tsv") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) == 5:
                session.execute(prep, (parts[0], float(parts[1]), float(parts[2]), parts[3], parts[4]))
    print("  pois done.")

    # checkins (into both denormalized tables)
    print("Ingesting checkins...")
    prep_country = session.prepare(
        "INSERT INTO checkins_by_country (country, venue_id, user_id, utc_time, timezone_offset, latitude, longitude, category) VALUES (?,?,?,?,?,?,?,?)"
    )
    prep_user = session.prepare(
        "INSERT INTO checkins_by_user (user_id, venue_id, utc_time, timezone_offset, country, latitude, longitude, category) VALUES (?,?,?,?,?,?,?,?)"
    )
    count = 0
    with open(f"{DATA}/my_checkins_anonymized.tsv") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) < 4:
                continue
            uid = int(parts[0])
            vid = parts[1]
            ts = parse_time(parts[2])
            tz = int(parts[3])
            poi = pois.get(vid, {"latitude": 0.0, "longitude": 0.0, "category": "Unknown", "country": "XX"})

            session.execute(prep_country, (poi["country"], vid, uid, ts, tz, poi["latitude"], poi["longitude"], poi["category"]))
            session.execute(prep_user, (uid, vid, ts, tz, poi["country"], poi["latitude"], poi["longitude"], poi["category"]))
            count += 1
            if count % 10000 == 0:
                print(f"    {count} checkins inserted...")
    print(f"  checkins done: {count} rows.")

    # friendships_before
    print("Ingesting friendships_before...")
    prep = session.prepare("INSERT INTO friendships_before (user_id, friend_id) VALUES (?,?)")
    with open(f"{DATA}/my_friendships_before.tsv") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) == 2:
                session.execute(prep, (int(parts[0]), int(parts[1])))
    print("  friendships_before done.")

    # friendships_after
    print("Ingesting friendships_after...")
    prep = session.prepare("INSERT INTO friendships_after (user_id, friend_id) VALUES (?,?)")
    with open(f"{DATA}/my_friendships_after.tsv") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) == 2:
                session.execute(prep, (int(parts[0]), int(parts[1])))
    print("  friendships_after done.")

    elapsed = time.time() - start
    session.shutdown()
    cluster.shutdown()
    print(f"\nScyllaDB ingestion finished in {elapsed:.2f} seconds.")

if __name__ == "__main__":
    ingest()
