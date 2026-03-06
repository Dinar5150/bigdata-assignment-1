"""
ingest_mongodb.py

Loads my_* data files into MongoDB.
Measures and reports total ingestion time.
"""
import time
import csv
from pymongo import MongoClient

DATA = "foursquare_dataset"
BATCH = 5000

def ingest():
    client = MongoClient("mongodb://localhost:27017/", directConnection=True)
    db = client["foursquaredb"]

    start = time.time()

    # users
    print("Ingesting users...")
    batch = []
    with open(f"{DATA}/my_users.csv") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            batch.append({"user_id": int(row[0])})
            if len(batch) >= BATCH:
                db.users.insert_many(batch, ordered=False)
                batch = []
        if batch:
            db.users.insert_many(batch, ordered=False)
    print("  users done.")

    # pois
    print("Ingesting POIs...")
    batch = []
    with open(f"{DATA}/my_POIs.tsv") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) == 5:
                batch.append({
                    "venue_id": parts[0],
                    "latitude": float(parts[1]),
                    "longitude": float(parts[2]),
                    "category": parts[3],
                    "country": parts[4]
                })
                if len(batch) >= BATCH:
                    db.pois.insert_many(batch, ordered=False)
                    batch = []
        if batch:
            db.pois.insert_many(batch, ordered=False)
    print("  pois done.")

    # checkins
    print("Ingesting checkins...")
    batch = []
    count = 0
    with open(f"{DATA}/my_checkins_anonymized.tsv") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) < 4:
                continue
            batch.append({
                "user_id": int(parts[0]),
                "venue_id": parts[1],
                "utc_time": parts[2],
                "timezone_offset": int(parts[3])
            })
            if len(batch) >= BATCH:
                db.checkins.insert_many(batch, ordered=False)
                count += len(batch)
                batch = []
                if count % 100000 == 0:
                    print(f"    {count} checkins inserted...")
        if batch:
            db.checkins.insert_many(batch, ordered=False)
            count += len(batch)
    print(f"  checkins done: {count} rows.")

    # friendships_before
    print("Ingesting friendships_before...")
    batch = []
    with open(f"{DATA}/my_friendships_before.tsv") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) == 2:
                batch.append({"user_id": int(parts[0]), "friend_id": int(parts[1])})
                if len(batch) >= BATCH:
                    db.friendships_before.insert_many(batch, ordered=False)
                    batch = []
        if batch:
            db.friendships_before.insert_many(batch, ordered=False)
    print("  friendships_before done.")

    # friendships_after
    print("Ingesting friendships_after...")
    batch = []
    with open(f"{DATA}/my_friendships_after.tsv") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) == 2:
                batch.append({"user_id": int(parts[0]), "friend_id": int(parts[1])})
                if len(batch) >= BATCH:
                    db.friendships_after.insert_many(batch, ordered=False)
                    batch = []
        if batch:
            db.friendships_after.insert_many(batch, ordered=False)
    print("  friendships_after done.")

    elapsed = time.time() - start
    client.close()
    print(f"\nMongoDB ingestion finished in {elapsed:.2f} seconds.")

if __name__ == "__main__":
    ingest()
