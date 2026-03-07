"""
queries_postgres.py

Runs Q1-Q4 on standalone PostgreSQL, 3 times each, reports avg time.
"""
import time
import json
import psycopg2

CONN = dict(host="localhost", port=5432, dbname="foursquaredb", user="user", password="pass")
RUNS = 3

def run_query(cur, name, sql):
    times = []
    result = None
    for i in range(RUNS):
        t0 = time.time()
        cur.execute(sql)
        result = cur.fetchall()
        elapsed = time.time() - t0
        times.append(elapsed)
        print(f"  {name} run {i+1}: {elapsed:.4f}s")
    avg = sum(times) / len(times)
    print(f"  {name} avg: {avg:.4f}s\n")
    return avg, result

def main():
    conn = psycopg2.connect(**CONN)
    cur = conn.cursor()
    results = {}

    # Q1
    print("Q1: Top 10 countries with highest total check-ins")
    q1 = """
        SELECT p.country, COUNT(*) AS total_checkins
        FROM checkins c
        JOIN pois p ON c.venue_id = p.venue_id
        GROUP BY p.country
        ORDER BY total_checkins DESC
        LIMIT 10;
    """
    avg, rows = run_query(cur, "Q1", q1)
    results["Q1"] = avg
    print("  Result:")
    for r in rows:
        print(f"    {r}")

    # Q2
    print("Q2: Users who prefer POIs shared by their friends (unchanged friendships)")
    q2 = """
        SELECT DISTINCT c1.user_id, c1.venue_id
        FROM checkins c1
        JOIN (
            SELECT fb.user_id, fb.friend_id
            FROM friendships_before fb
            INNER JOIN friendships_after fa ON fb.user_id = fa.user_id AND fb.friend_id = fa.friend_id
        ) f ON c1.user_id = f.user_id
        JOIN checkins c2 ON c2.user_id = f.friend_id AND c2.venue_id = c1.venue_id
        LIMIT 20;
    """
    avg, rows = run_query(cur, "Q2", q2)
    results["Q2"] = avg
    print("  Result:")
    for r in rows:
        print(f"    {r}")

    # Q3
    print("Q3: Most attractive venues by country")
    q3 = """
        SELECT country, venue_id, category, latitude, longitude, total_shares FROM (
            SELECT p.country, p.venue_id, p.category, p.latitude, p.longitude,
                   COUNT(*) AS total_shares,
                   ROW_NUMBER() OVER (PARTITION BY p.country ORDER BY COUNT(*) DESC) AS rn
            FROM checkins c
            JOIN pois p ON c.venue_id = p.venue_id
            GROUP BY p.country, p.venue_id, p.category, p.latitude, p.longitude
        ) sub WHERE rn = 1
        ORDER BY total_shares DESC
        LIMIT 20;
    """
    avg, rows = run_query(cur, "Q3", q3)
    results["Q3"] = avg
    print("  Result:")
    for r in rows:
        print(f"    {r}")

    # Q4
    print("Q4: Categorize venues using full text search")
    q4 = """
        SELECT
            CASE
                WHEN to_tsvector('english', category) @@ to_tsquery('english', 'Restaurant') THEN 'Restaurant'
                WHEN to_tsvector('english', category) @@ to_tsquery('english', 'Club') THEN 'Club'
                WHEN to_tsvector('english', category) @@ to_tsquery('english', 'Museum') THEN 'Museum'
                WHEN to_tsvector('english', category) @@ to_tsquery('english', 'Shop') THEN 'Shop'
                ELSE 'Others'
            END AS custom_category,
            COUNT(*) AS venue_count
        FROM pois
        GROUP BY custom_category
        ORDER BY venue_count DESC;
    """
    avg, rows = run_query(cur, "Q4", q4)
    results["Q4"] = avg
    print("  Result:")
    for r in rows:
        print(f"    {r}")

    cur.close()
    conn.close()

    with open("results_postgres.json", "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nAll timings saved to results_postgres.json")
    print(json.dumps(results, indent=2))

if __name__ == "__main__":
    main()
