"""
queries_mongodb.py

Runs Q1-Q4 on MongoDB, 3 times each, reports avg time.
"""
import time
import json
from collections import defaultdict
from pymongo import MongoClient

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
    client = MongoClient("mongodb://localhost:27017/", directConnection=True)
    db = client["foursquaredb"]
    results = {}

    # Q1
    print("Q1: Top 10 countries with highest total check-ins")
    def q1():
        pipeline = [
            {"$group": {"_id": "$country", "total": {"$sum": 1}}},
            {"$sort": {"total": -1}},
            {"$limit": 10}
        ]
        return list(db.checkins.aggregate(pipeline, allowDiskUse=True))
    avg, res = timed(q1, "Q1")
    results["Q1"] = avg
    print("  Result:")
    for r in res:
        print(f"    {r['_id']}: {r['total']}")

    # Q2
    print("Q2: Users who prefer POIs shared by friends (unchanged friendships)")
    def q2():
        before = set()
        for doc in db.friendships_before.find({}, {"_id": 0, "user_id": 1, "friend_id": 1}):
            before.add((doc["user_id"], doc["friend_id"]))
        after = set()
        for doc in db.friendships_after.find({}, {"_id": 0, "user_id": 1, "friend_id": 1}):
            after.add((doc["user_id"], doc["friend_id"]))
        unchanged = before & after

        friend_map = defaultdict(set)
        for u, f in unchanged:
            friend_map[u].add(f)

        relevant_users = set(friend_map.keys())
        for friends in friend_map.values():
            relevant_users |= friends

        user_venues = defaultdict(set)
        for doc in db.checkins.find({"user_id": {"$in": list(relevant_users)}}, {"user_id": 1, "venue_id": 1}):
            user_venues[doc["user_id"]].add(doc["venue_id"])

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

    # Q3
    print("Q3: Most attractive venues by country")
    def q3():
        pipeline = [
            {"$group": {
                "_id": {"country": "$country", "venue_id": "$venue_id"},
                "total_shares": {"$sum": 1},
                "category": {"$first": "$category"}
            }},
            {"$sort": {"total_shares": -1}},
            {"$group": {
                "_id": "$_id.country",
                "venue_id": {"$first": "$_id.venue_id"},
                "total_shares": {"$first": "$total_shares"},
                "category": {"$first": "$category"}
            }},
            {"$sort": {"total_shares": -1}},
            {"$limit": 20},
            {"$project": {
                "country": "$_id",
                "venue_id": 1,
                "total_shares": 1,
                "category": 1
            }}
        ]
        return list(db.checkins.aggregate(pipeline, allowDiskUse=True))
    avg, res = timed(q3, "Q3")
    results["Q3"] = avg
    print("  Result:")
    for r in res:
        print(f"    {r.get('country','?')} | {r['venue_id']} | {r.get('category','?')} | shares={r['total_shares']}")

    # Q4
    print("Q4: Categorize venues using text search")
    def q4():
        pipeline = [
            {"$project": {
                "custom_category": {
                    "$switch": {
                        "branches": [
                            {"case": {"$regexMatch": {"input": {"$toLower": "$category"}, "regex": "restaurant"}}, "then": "Restaurant"},
                            {"case": {"$regexMatch": {"input": {"$toLower": "$category"}, "regex": "club"}}, "then": "Club"},
                            {"case": {"$regexMatch": {"input": {"$toLower": "$category"}, "regex": "museum"}}, "then": "Museum"},
                            {"case": {"$regexMatch": {"input": {"$toLower": "$category"}, "regex": "shop"}}, "then": "Shop"},
                        ],
                        "default": "Others"
                    }
                }
            }},
            {"$group": {"_id": "$custom_category", "venue_count": {"$sum": 1}}},
            {"$sort": {"venue_count": -1}}
        ]
        return {r["_id"]: r["venue_count"] for r in db.pois.aggregate(pipeline)}
    avg, res = timed(q4, "Q4")
    results["Q4"] = avg
    print("  Result:")
    for cat, cnt in res.items():
        print(f"    {cat}: {cnt}")

    client.close()

    with open("results_mongodb.json", "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nAll timings saved to results_mongodb.json")
    print(json.dumps(results, indent=2))

if __name__ == "__main__":
    main()
