"""
ingest_postgres.py

Loads my_* data files into standalone PostgreSQL.
Uses COPY for fast bulk loading.
"""
import time
import psycopg2
import csv
from io import StringIO

CONN = dict(host="localhost", port=5432, dbname="foursquaredb", user="user", password="pass")
DATA = "foursquare_dataset"

def ingest():
    conn = psycopg2.connect(**CONN)
    conn.autocommit = False
    cur = conn.cursor()

    start = time.time()

    # users
    print("[PostgreSQL] Ingesting users...")
    buf = StringIO()
    with open(f"{DATA}/my_users.csv") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            buf.write(row[0] + "\n")
    buf.seek(0)
    cur.copy_expert("COPY users (user_id) FROM STDIN WITH (FORMAT text)", buf)
    conn.commit()
    print("[PostgreSQL]   users done.")

    # pois
    print("[PostgreSQL] Ingesting POIs...")
    buf = StringIO()
    with open(f"{DATA}/my_POIs.tsv", encoding="utf-8", errors="replace") as f:
        buf.write(f.read())
    buf.seek(0)
    cur.copy_expert("COPY pois (venue_id, latitude, longitude, category, country) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t')", buf)
    conn.commit()
    print("[PostgreSQL]   POIs done.")

    # checkins
    print("[PostgreSQL] Ingesting checkins...")
    buf = StringIO()
    count = 0
    with open(f"{DATA}/my_checkins_anonymized.tsv", encoding="utf-8", errors="replace") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) >= 4:
                buf.write(f"{parts[0]}\t{parts[1]}\t{parts[2]}\t{parts[3]}\n")
                count += 1
    buf.seek(0)
    cur.copy_expert("COPY checkins (user_id, venue_id, utc_time, timezone_offset) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t')", buf)
    conn.commit()
    print(f"[PostgreSQL]   checkins done: {count} rows.")

    # friendships_before
    print("[PostgreSQL] Ingesting friendships_before...")
    buf = StringIO()
    with open(f"{DATA}/my_friendships_before.tsv") as f:
        buf.write(f.read())
    buf.seek(0)
    cur.copy_expert("COPY friendships_before (user_id, friend_id) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t')", buf)
    conn.commit()
    print("[PostgreSQL]   friendships_before done.")

    # friendships_after
    print("[PostgreSQL] Ingesting friendships_after...")
    buf = StringIO()
    with open(f"{DATA}/my_friendships_after.tsv") as f:
        buf.write(f.read())
    buf.seek(0)
    cur.copy_expert("COPY friendships_after (user_id, friend_id) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t')", buf)
    conn.commit()
    print("[PostgreSQL]   friendships_after done.")

    elapsed = time.time() - start
    cur.close()
    conn.close()
    print(f"\n[PostgreSQL] Ingestion finished in {elapsed:.2f} seconds.")

    import json
    with open("ingest_time_postgres.json", "w") as f:
        json.dump({"time": elapsed}, f)

    from docker_stats import print_db_info
    print_db_info("PostgreSQL")

if __name__ == "__main__":
    ingest()
