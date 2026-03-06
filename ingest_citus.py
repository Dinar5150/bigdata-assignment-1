"""
ingest_citus.py

Loads my_* data files into Citus (distributed PostgreSQL).
Measures and reports total ingestion time.
"""
import time
import psycopg2
import csv

CONN = dict(host="localhost", port=5433, dbname="foursquaredb", user="user", password="pass")
BATCH = 5000
DATA = "foursquare_dataset"

def ingest():
    conn = psycopg2.connect(**CONN)
    conn.autocommit = False
    cur = conn.cursor()

    start = time.time()

    # users
    print("[Citus] Ingesting users...")
    with open(f"{DATA}/my_users.csv") as f:
        reader = csv.reader(f)
        next(reader)
        batch = []
        for row in reader:
            batch.append((int(row[0]),))
            if len(batch) >= BATCH:
                cur.executemany("INSERT INTO users (user_id) VALUES (%s) ON CONFLICT DO NOTHING", batch)
                conn.commit()
                batch = []
        if batch:
            cur.executemany("INSERT INTO users (user_id) VALUES (%s) ON CONFLICT DO NOTHING", batch)
            conn.commit()
    print("[Citus]   users done.")

    # pois
    print("[Citus] Ingesting POIs...")
    with open(f"{DATA}/my_POIs.tsv") as f:
        reader = csv.reader(f, delimiter="\t")
        batch = []
        for row in reader:
            batch.append((row[0], float(row[1]), float(row[2]), row[3], row[4]))
            if len(batch) >= BATCH:
                cur.executemany(
                    "INSERT INTO pois (venue_id, latitude, longitude, category, country) VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                    batch
                )
                conn.commit()
                batch = []
        if batch:
            cur.executemany(
                "INSERT INTO pois (venue_id, latitude, longitude, category, country) VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                batch
            )
            conn.commit()
    print("[Citus]   POIs done.")

    # checkins
    print("[Citus] Ingesting checkins...")
    inserted = 0
    with open(f"{DATA}/my_checkins_anonymized.tsv") as f:
        reader = csv.reader(f, delimiter="\t")
        batch = []
        for row in reader:
            batch.append((int(row[0]), row[1], row[2], int(row[3])))
            if len(batch) >= BATCH:
                cur.executemany(
                    "INSERT INTO checkins (user_id, venue_id, utc_time, timezone_offset) VALUES (%s,%s,%s,%s)",
                    batch
                )
                conn.commit()
                inserted += len(batch)
                batch = []
                if inserted % 100000 == 0:
                    print(f"[Citus]   {inserted} checkins...")
        if batch:
            cur.executemany(
                "INSERT INTO checkins (user_id, venue_id, utc_time, timezone_offset) VALUES (%s,%s,%s,%s)",
                batch
            )
            conn.commit()
            inserted += len(batch)
    print(f"[Citus]   checkins done: {inserted} rows.")

    # friendships_before
    print("[Citus] Ingesting friendships_before...")
    with open(f"{DATA}/my_friendships_before.tsv") as f:
        reader = csv.reader(f, delimiter="\t")
        batch = []
        for row in reader:
            batch.append((int(row[0]), int(row[1])))
            if len(batch) >= BATCH:
                cur.executemany(
                    "INSERT INTO friendships_before (user_id, friend_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                    batch
                )
                conn.commit()
                batch = []
        if batch:
            cur.executemany(
                "INSERT INTO friendships_before (user_id, friend_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                batch
            )
            conn.commit()
    print("[Citus]   friendships_before done.")

    # friendships_after
    print("[Citus] Ingesting friendships_after...")
    with open(f"{DATA}/my_friendships_after.tsv") as f:
        reader = csv.reader(f, delimiter="\t")
        batch = []
        for row in reader:
            batch.append((int(row[0]), int(row[1])))
            if len(batch) >= BATCH:
                cur.executemany(
                    "INSERT INTO friendships_after (user_id, friend_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                    batch
                )
                conn.commit()
                batch = []
        if batch:
            cur.executemany(
                "INSERT INTO friendships_after (user_id, friend_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                batch
            )
            conn.commit()
    print("[Citus]   friendships_after done.")

    elapsed = time.time() - start
    cur.close()
    conn.close()
    print(f"\n[Citus] Ingestion finished in {elapsed:.2f} seconds.")

if __name__ == "__main__":
    ingest()
