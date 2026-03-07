"""
ingest_citus.py

Loads my_* data files into Citus (distributed PostgreSQL).
Uses COPY for fast bulk loading.
"""
import time
import psycopg2
import csv
from io import StringIO

CONN = dict(host="localhost", port=5433, dbname="foursquaredb", user="user", password="pass")
DATA = "foursquare_dataset"

def ingest():
    conn = psycopg2.connect(**CONN)
    conn.autocommit = False
    cur = conn.cursor()

    start = time.time()

    # users
    print("[Citus] Ingesting users...")
    buf = StringIO()
    with open(f"{DATA}/my_users.csv") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            buf.write(row[0] + "\n")
    buf.seek(0)
    cur.copy_expert("COPY users (user_id) FROM STDIN WITH (FORMAT text)", buf)
    conn.commit()
    print("[Citus]   users done.")

    # pois
    print("[Citus] Ingesting POIs...")
    buf = StringIO()
    with open(f"{DATA}/my_POIs.tsv", encoding="utf-8", errors="replace") as f:
        buf.write(f.read())
    buf.seek(0)
    cur.copy_expert("COPY pois (venue_id, latitude, longitude, category, country) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t')", buf)
    conn.commit()
    print("[Citus]   POIs done.")

    # checkins
    print("[Citus] Ingesting checkins...")
    buf = StringIO()
    count = 0
    with open(f"{DATA}/my_checkins_anonymized.tsv", encoding="utf-8", errors="replace") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) >= 4:
                buf.write(f"{parts[0]}\t{parts[1]}\t{parts[2]}\t{parts[3]}\n")
                count += 1
                if count % 500000 == 0:
                    print(f"\r  reading checkins: {count} rows", end="", flush=True)
    print(f"\r  read {count} checkins, copying...        ", flush=True)
    buf.seek(0)
    cur.copy_expert("COPY checkins (user_id, venue_id, utc_time, timezone_offset) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t')", buf)
    conn.commit()
    print(f"[Citus]   checkins done: {count} rows.")

    # friendships_before
    print("[Citus] Ingesting friendships_before...")
    buf = StringIO()
    with open(f"{DATA}/my_friendships_before.tsv") as f:
        buf.write(f.read())
    buf.seek(0)
    cur.copy_expert("COPY friendships_before (user_id, friend_id) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t')", buf)
    conn.commit()
    print("[Citus]   friendships_before done.")

    # friendships_after
    print("[Citus] Ingesting friendships_after...")
    buf = StringIO()
    with open(f"{DATA}/my_friendships_after.tsv") as f:
        buf.write(f.read())
    buf.seek(0)
    cur.copy_expert("COPY friendships_after (user_id, friend_id) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t')", buf)
    conn.commit()
    print("[Citus]   friendships_after done.")

    elapsed = time.time() - start
    cur.close()
    conn.close()
    print(f"\n[Citus] Ingestion finished in {elapsed:.2f} seconds.")

    import json
    with open("ingest_time_citus.json", "w") as f:
        json.dump({"time": elapsed}, f)

    from docker_stats import print_db_info
    print_db_info("Citus")

if __name__ == "__main__":
    ingest()
