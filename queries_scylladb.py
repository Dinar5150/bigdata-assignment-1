"""
queries_scylladb.py

Runs Q1-Q4 on ScyllaDB, 3 times each, reports avg time.
ScyllaDB doesn't support joins/aggregations well, so we do
some processing in Python after fetching data.
"""
import time
import json
from collections import Counter, defaultdict
from cassandra.cluster import Cluster

RUNS = 3

def timed(fn, name):
    times = []
    result = None
    for i in range(RUNS):
        t0 = time.time()
        result = fn()
        elapsed = time.time() - t0
        times.append(elapsed)
        print(f"  {name} run {i+1}: {elapsed:.4f}s")
    avg = sum(times) / len(times)
    print(f"  {name} avg: {avg:.4f}s\n")
    return avg, result

def main():
    cluster = Cluster(["127.0.0.1"], port=9042)
    session = cluster.connect("foursquaredb")
    session.default_timeout = 120
    results = {}

    # Q1: top 10 countries by checkins
    print("Q1: Top 10 countries with highest total check-ins")
    def q1():
        rows = session.execute("SELECT country, venue_id, user_id FROM checkins_by_country")
        counter = Counter()
        for row in rows:
            counter[row.country] += 1
        return counter.most_common(10)
    avg, res = timed(q1, "Q1")
    results["Q1"] = avg
    print("  Result:")
    for country, cnt in res:
        print(f"    {country}: {cnt}")

    # Q2: users checking POIs shared by friends (unchanged friendships)
    print("Q2: Users who prefer POIs shared by friends (unchanged friendships)")
    def q2():
        # get unchanged friendships
        before = set()
        rows_b = session.execute("SELECT user_id, friend_id FROM friendships_before")
        for r in rows_b:
            before.add((r.user_id, r.friend_id))
        after = set()
        rows_a = session.execute("SELECT user_id, friend_id FROM friendships_after")
        for r in rows_a:
            after.add((r.user_id, r.friend_id))
        unchanged = before & after

        # build friend map
        friend_map = defaultdict(set)
        for u, f in unchanged:
            friend_map[u].add(f)

        # get checkins per user -> venues
        user_venues = defaultdict(set)
        rows_c = session.execute("SELECT user_id, venue_id FROM checkins_by_user")
        for r in rows_c:
            user_venues[r.user_id].add(r.venue_id)

        # find users who visited same POI as a friend
        output = []
        for uid, friends in friend_map.items():
            if uid not in user_venues:
                continue
            for fid in friends:
                if fid not in user_venues:
                    continue
                common = user_venues[uid] & user_venues[fid]
                for vid in common:
                    output.append((uid, vid))
                    if len(output) >= 20:
                        return output
        return output
    avg, res = timed(q2, "Q2")
    results["Q2"] = avg
    print("  Result:")
    for uid, vid in res[:20]:
        print(f"    user={uid}, venue={vid}")

    # Q3: most attractive venues by country
    print("Q3: Most attractive venues by country")
    def q3():
        rows = session.execute("SELECT country, venue_id, user_id, category, latitude, longitude FROM checkins_by_country")
        counter = Counter()
        info = {}
        for r in rows:
            key = (r.country, r.venue_id)
            counter[key] += 1
            if key not in info:
                info[key] = (r.category, r.latitude, r.longitude)
        top = counter.most_common(20)
        return [(k[0], k[1], info[k][0], info[k][1], info[k][2], cnt) for k, cnt in top]
    avg, res = timed(q3, "Q3")
    results["Q3"] = avg
    print("  Result:")
    for country, vid, cat, lat, lon, cnt in res:
        print(f"    {country} | {vid} | {cat} | {lat},{lon} | shares={cnt}")

    # Q4: categorize venues using text matching
    print("Q4: Categorize venues")
    def q4():
        rows = session.execute("SELECT venue_id, category FROM pois")
        counter = Counter()
        for r in rows:
            cat = r.category.lower() if r.category else ""
            if "restaurant" in cat:
                counter["Restaurant"] += 1
            elif "club" in cat:
                counter["Club"] += 1
            elif "museum" in cat:
                counter["Museum"] += 1
            elif "shop" in cat:
                counter["Shop"] += 1
            else:
                counter["Others"] += 1
        return counter.most_common()
    avg, res = timed(q4, "Q4")
    results["Q4"] = avg
    print("  Result:")
    for cat, cnt in res:
        print(f"    {cat}: {cnt}")

    session.shutdown()
    cluster.shutdown()

    with open("results_scylladb.json", "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nAll timings saved to results_scylladb.json")
    print(json.dumps(results, indent=2))

if __name__ == "__main__":
    main()
