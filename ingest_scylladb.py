"""
ingest_scylladb.py

Loads my_* data files into ScyllaDB.
Measures and reports total ingestion time.
Uses execute_concurrent for fast parallel inserts.
"""
import time
import csv
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args

DATA = "foursquare_dataset"
CONC = 200  # concurrent requests

def parse_time(s):
    return datetime.strptime(s, "%a %b %d %H:%M:%S %z %Y")

def bulk_insert(session, prep, rows, label=""):
    for i in range(0, len(rows), 5000):
        batch = rows[i:i+5000]
        execute_concurrent_with_args(session, prep, batch, concurrency=CONC)
        if label:
            print(f"[ScyllaDB]   {label}: {min(i+5000, len(rows))}/{len(rows)}")

def ingest():
    cluster = Cluster(["127.0.0.1"], port=9042)
    session = cluster.connect("foursquaredb")
    session.default_timeout = 60

    start = time.time()

    # load pois into a dict for denormalized checkin tables
    print("[ScyllaDB] Loading POIs into memory...")
    pois = {}
    with open(f"{DATA}/my_POIs.tsv", encoding="utf-8", errors="replace") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) == 5:
                pois[parts[0]] = (float(parts[1]), float(parts[2]), parts[3], parts[4])
    print(f"[ScyllaDB]   {len(pois)} POIs loaded.")

    # users
    print("[ScyllaDB] Ingesting users...")
    prep = session.prepare("INSERT INTO users (user_id) VALUES (?)")
    rows = []
    with open(f"{DATA}/my_users.csv") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            rows.append((int(row[0]),))
    bulk_insert(session, prep, rows, "users")
    print(f"[ScyllaDB]   users done: {len(rows)}")

    # pois
    print("[ScyllaDB] Ingesting POIs...")
    prep = session.prepare("INSERT INTO pois (venue_id, latitude, longitude, category, country) VALUES (?,?,?,?,?)")
    rows = []
    with open(f"{DATA}/my_POIs.tsv", encoding="utf-8", errors="replace") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) == 5:
                rows.append((parts[0], float(parts[1]), float(parts[2]), parts[3], parts[4]))
    bulk_insert(session, prep, rows, "pois")
    print(f"[ScyllaDB]   POIs done: {len(rows)}")

    # checkins
    print("[ScyllaDB] Ingesting checkins...")
    prep_country = session.prepare(
        "INSERT INTO checkins_by_country (country, venue_id, user_id, utc_time, timezone_offset, latitude, longitude, category) VALUES (?,?,?,?,?,?,?,?)"
    )
    prep_user = session.prepare(
        "INSERT INTO checkins_by_user (user_id, venue_id, utc_time, timezone_offset, country, latitude, longitude, category) VALUES (?,?,?,?,?,?,?,?)"
    )
    rows_country = []
    rows_user = []
    with open(f"{DATA}/my_checkins_anonymized.tsv", encoding="utf-8", errors="replace") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) < 4:
                continue
            uid = int(parts[0])
            vid = parts[1]
            ts = parse_time(parts[2])
            tz = int(parts[3])
            poi = pois.get(vid, (0.0, 0.0, "Unknown", "XX"))
            rows_country.append((poi[3], vid, uid, ts, tz, poi[0], poi[1], poi[2]))
            rows_user.append((uid, vid, ts, tz, poi[3], poi[0], poi[1], poi[2]))
    print(f"[ScyllaDB]   loaded {len(rows_country)} checkins, inserting...")
    bulk_insert(session, prep_country, rows_country, "checkins_by_country")
    bulk_insert(session, prep_user, rows_user, "checkins_by_user")
    print(f"[ScyllaDB]   checkins done: {len(rows_country)}")

    # friendships_before
    print("[ScyllaDB] Ingesting friendships_before...")
    prep = session.prepare("INSERT INTO friendships_before (user_id, friend_id) VALUES (?,?)")
    rows = []
    with open(f"{DATA}/my_friendships_before.tsv") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) == 2:
                rows.append((int(parts[0]), int(parts[1])))
    bulk_insert(session, prep, rows, "friendships_before")
    print(f"[ScyllaDB]   friendships_before done: {len(rows)}")

    # friendships_after
    print("[ScyllaDB] Ingesting friendships_after...")
    prep = session.prepare("INSERT INTO friendships_after (user_id, friend_id) VALUES (?,?)")
    rows = []
    with open(f"{DATA}/my_friendships_after.tsv") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) == 2:
                rows.append((int(parts[0]), int(parts[1])))
    bulk_insert(session, prep, rows, "friendships_after")
    print(f"[ScyllaDB]   friendships_after done: {len(rows)}")

    elapsed = time.time() - start
    session.shutdown()
    cluster.shutdown()
    print(f"\n[ScyllaDB] Ingestion finished in {elapsed:.2f} seconds.")

    import json
    with open("ingest_time_scylladb.json", "w") as f:
        json.dump({"time": elapsed}, f)

    from docker_stats import print_db_info
    print_db_info("ScyllaDB")

if __name__ == "__main__":
    ingest()
