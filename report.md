# Assignment 1: Big Data Storage in SQL vs. NoSQL Databases

**Student name:** Dinar Yakupov  
**Group:** B23-DS-01  
**Student SID:** 95  

**Note:** I used a 5% user sample (`USERS_PERC = 0.05`) instead of the assigned 25%. I tried running the full 25% slice (~22.5M checkins) first, but it kept eating up all the RAM, causing OOM errors and Docker container crashes. ScyllaDB ingestion alone ran for 8+ hours and still didn't finish. Because of these issues on my machine, I went with the smaller 5% sample so I could actually complete everything.

## I. Data Preparation

### 1. User Slice Selection

I ran the provided `select_my_users_slice.py` script to get my random subset of users from `users.txt` based on my SID.

![User slice selection](screenshots/select-users.png)

### 2. Data Filtering Script

My `prepare_data.py` script reads the full dataset files in chunks of 500,000 rows using pandas and keeps only the rows for users in `my_users.csv`. It also checks timestamps and drops any malformed ones. The output gets saved as `my_*.tsv` files.

![Data preparation output 1](screenshots/prepare-data-1.png)
![Data preparation output 2](screenshots/prepare-data-2.png)
![Data preparation output 3](screenshots/prepare-data-3.png)

## II. Data Modeling & Ingestion

### A. Database Schema Design

### 1. PostgreSQL (Standalone)

I went with a standard normalized relational schema with foreign keys and indexes.

**Design decisions:**
- Separate tables for users, POIs, checkins, and friendships (normalized).
- Foreign keys between checkins and users/POIs for referential integrity.
- B-tree indexes on `checkins(user_id)` and `checkins(venue_id)` to speed up JOINs.
- GIN index on `pois(category)` with `to_tsvector` for full-text search in Q4.
- Index on `pois(country)` for the country-based aggregation in Q1.

![PostgreSQL schema](screenshots/pg.png)

![Setup PostgreSQL 1](screenshots/setup-cluster-1.png)
![Setup PostgreSQL 2](screenshots/setup-cluster-3.png)

### 2. PostgreSQL (Citus Data)

The Citus schema is similar to standalone PostgreSQL but adapted for distributed execution. The main differences are the sharding strategy and no foreign keys (Citus can't do cross-shard foreign keys).

**Differences from PostgreSQL:**
- Distribution column: I picked `user_id` as the shard key for `checkins`, `friendships_before`, and `friendships_after`. This way joins between these tables stay on the same shard when filtering by user, which matters for Q2.
- Reference tables: `users` and `pois` are set as reference tables (copied to all nodes) since they're small and get joined a lot.
- No foreign keys on distributed tables - Citus doesn't support cross-shard foreign keys.
- Composite primary key `(user_id, id)` on checkins because Citus requires the distribution column in the primary key.
- `SET citus.enable_repartition_joins TO on;` is needed for Q2 since the join on `friend_id` has to go across shards.

![Citus schema](screenshots/citus.png)

![Setup Citus 1](screenshots/setup-cluster-2.png)
![Setup Citus 2](screenshots/setup-cluster-3.png)

### 3. ScyllaDB

ScyllaDB is a column-family store with no support for JOINs or complex aggregations, so I had to use denormalized tables tailored to each query's access pattern. I created two separate checkin tables (`checkins_by_country` and `checkins_by_user`) because one table can't efficiently handle both country-based queries (Q1, Q3) and user-based queries (Q2). Without this denormalization, the queries needed full table scans with ALLOW FILTERING - which was way too slow on the dataset.

**Design decisions:**
- `checkins_by_country` partitioned by `country` for Q1 and Q3. Clustering columns `(venue_id, user_id, utc_time)` keep rows unique within a partition.
- `checkins_by_user` partitioned by `user_id` for Q2. Clustering by `(utc_time, venue_id)` so data comes back in time order.
- Venue info (latitude, longitude, category, country) is copied into both checkin tables so there's no need for JOINs.
- Data is replicated across 3 nodes.

![Scylla schema](screenshots/chebotko.png)

![Setup ScyllaDB](screenshots/setup-cluster-4.png)

---

### 4. MongoDB

MongoDB uses a document model. I stored checkins with embedded country and category fields from the POIs collection. Without embedding these, Q1 and Q3 would need `$lookup` (basically a JOIN) between checkins and POIs, and that was way too slow - it couldn't even finish in a reasonable time. Putting country and category directly into each checkin document gets rid of these joins completely.

**Justification:** I embedded `country` and `category` from POIs right into each checkin document. This avoids `$lookup` joins during Q1 (aggregate by country) and Q3 (find attractive venues). These fields don't change and the queries are read-heavy, so embedding makes more sense than referencing.

![MongoDB schema](screenshots/mongo.png)

![Setup MongoDB](screenshots/setup-cluster-5.png)

### B. Data Ingestion

### PostgreSQL & Citus Ingestion

Both use `COPY FROM STDIN` via `psycopg2.copy_expert()` with `StringIO` buffers. This is much faster than doing row-by-row INSERTs.

![PostgreSQL ingestion](screenshots/ingest-postgres-1.png)
![PostgreSQL ingestion](screenshots/ingest-postgres-2.png)
![PostgreSQL ingestion](screenshots/ingest-postgres-3.png)

![Citus ingestion](screenshots/ingest-citus-1.png)
![Citus ingestion](screenshots/ingest-citus-2.png)
![Citus ingestion](screenshots/ingest-citus-3.png)

### ScyllaDB Ingestion

Uses `execute_concurrent_with_args` from the `scylla-driver` with 500 concurrent requests and chunks of 10,000 rows. Each checkin gets inserted into both `checkins_by_country` and `checkins_by_user` (because of the denormalization).

![ScyllaDB ingestion](screenshots/ingest-scylla-1.png)
![ScyllaDB ingestion](screenshots/ingest-scylla-2.png)
![ScyllaDB ingestion](screenshots/ingest-scylla-3.png)

### MongoDB Ingestion

Uses `insert_many` with batches of 5,000 documents. During ingestion, each checkin gets its POI's country and category looked up and embedded into the document.

![MongoDB ingestion](screenshots/ingest-mongodb-1.png)
![MongoDB ingestion](screenshots/ingest-mongodb-2.png)
![MongoDB ingestion](screenshots/ingest-mongodb-3.png)
![MongoDB ingestion](screenshots/ingest-mongodb-4.png)

### Ingestion Summary Table

| Database   | Ingestion Time (s) | Setup                    | CPU cores | Memory     |
|------------|--------------------|--------------------------|-----------|------------|
| PostgreSQL | 204.57             | A single server          | 16        | 0.33 GiB   |
| Citus Data | 39.72              | A cluster of 3 nodes     | 16        | 0.54 GiB   |
| ScyllaDB   | 2088.77            | A cluster of 3 nodes     | 3         | 1.19 GiB   |
| MongoDB    | 877.47             | A replica set of 3 nodes | 16        | 1.20 GiB   |

**Observations:**
- Citus was fastest because `COPY FROM` distributes data across workers in parallel.
- Standalone PostgreSQL was also fast since `COPY` is very efficient.
- MongoDB was slower - inserting documents one batch at a time plus replica set overhead adds up.
- ScyllaDB was by far the slowest since every checkin has to be inserted twice (two denormalized tables), which means roughly double the CQL inserts.

---

## III. Analytical Query Execution

Each query was run 3 times and the average execution time is reported.

### Q1: Top 10 Countries with Highest Total Check-ins

**PostgreSQL & Citus:** Joins checkins with POIs to get the country, groups by country and counts. The indexes on `pois(country)` and `checkins(venue_id)` help speed this up.

```sql
SELECT p.country, COUNT(*) AS total_checkins
FROM checkins c
JOIN pois p ON c.venue_id = p.venue_id
GROUP BY p.country
ORDER BY total_checkins DESC
LIMIT 10;
```

**ScyllaDB:** Scans the whole `checkins_by_country` table and counts by `country` partition key in Python with `Counter`. The data is already grouped by country since that's the partition key.

```python
rows = session.execute("SELECT country, venue_id, user_id FROM checkins_by_country")
counter = Counter()
for row in rows:
    counter[row.country] += 1
result = counter.most_common(10)
```

**MongoDB:** Uses an aggregation pipeline with `$group` on the embedded `country` field. No `$lookup` needed because country is already in each checkin document.

```javascript
db.checkins.aggregate([
    {$group: {_id: "$country", total: {$sum: 1}}},
    {$sort: {total: -1}},
    {$limit: 10}
])
```

![PostgreSQL Q1](screenshots/pg-q1.png)
![Citus Q1](screenshots/citus-q1-2.png)
![ScyllaDB Q1](screenshots/scylla-q1.png)
![MongoDB Q1](screenshots/mongo-q1.png)

---

### Q2: Users Who Prefer POIs Shared by Friends (Unchanged Friendships)

**PostgreSQL & Citus:**

First finds unchanged friendships (intersection of before and after), then looks for cases where both the user and their friend checked into the same venue. Citus needs `SET citus.enable_repartition_joins TO on;` because the join on `friend_id` goes across shards.

```sql
SELECT DISTINCT c1.user_id, c1.venue_id
FROM checkins c1
JOIN (
    SELECT fb.user_id, fb.friend_id
    FROM friendships_before fb
    INNER JOIN friendships_after fa
      ON fb.user_id = fa.user_id AND fb.friend_id = fa.friend_id
) f ON c1.user_id = f.user_id
JOIN checkins c2 ON c2.user_id = f.friend_id AND c2.venue_id = c1.venue_id
LIMIT 20;
```

**ScyllaDB & MongoDB:** Both do the processing in Python:
1. Load all friendships from both snapshots.
2. Find the intersection (unchanged friendships).
3. Build a friend map and user-venue sets.
4. Check which users share venues with their friends.

```python
# ScyllaDB / MongoDB pseudocode
before = set of (user_id, friend_id) from friendships_before
after = set of (user_id, friend_id) from friendships_after
unchanged = before & after

friend_map = defaultdict(set)  # user -> set of friends
for u, f in unchanged:
    friend_map[u].add(f)

user_venues = defaultdict(set)  # user -> set of venue_ids
for row in checkins:
    user_venues[row.user_id].add(row.venue_id)

# find users who visited same venue as a friend
for uid, friends in friend_map.items():
    for fid in friends:
        common = user_venues[uid] & user_venues[fid]
```

![PostgreSQL Q2](screenshots/pg-q2.png)
![Citus Q2](screenshots/citus-q1-2.png)
![ScyllaDB Q2](screenshots/scylla-q2.png)
![MongoDB Q2](screenshots/mongo-q2.png)

---

### Q3: Most Attractive Venues by Country

**PostgreSQL & Citus:** Groups checkins by venue (joining with POIs for country) and orders by total check-in count per venue.

```sql
SELECT p.country, p.venue_id, p.category, p.latitude, p.longitude,
       COUNT(*) AS total_shares
FROM checkins c
JOIN pois p ON c.venue_id = p.venue_id
GROUP BY p.country, p.venue_id, p.category, p.latitude, p.longitude
ORDER BY total_shares DESC
LIMIT 20;
```

**ScyllaDB:** Scans the entire `checkins_by_country` table, counts each `(country, venue_id)` pair in Python with `Counter`, then picks the top 20.

```python
rows = session.execute(
    "SELECT country, venue_id, category, latitude, longitude FROM checkins_by_country"
)
counter = Counter()
for r in rows:
    counter[(r.country, r.venue_id)] += 1
top = counter.most_common(20)
```

**MongoDB:** Aggregation pipeline that groups by `venue_id`, using `$first` to grab the embedded `country` and `category` fields.

```javascript
db.checkins.aggregate([
    {$group: {
        _id: "$venue_id",
        total_shares: {$sum: 1},
        country: {$first: "$country"},
        category: {$first: "$category"}
    }},
    {$sort: {total_shares: -1}},
    {$limit: 20}
])
```

![PostgreSQL Q3](screenshots/pg-q3.png)
![Citus Q3](screenshots/citus-q3-4.png)
![ScyllaDB Q3](screenshots/scylla-q3.png)
![MongoDB Q3](screenshots/mongo-q3.png)

### Q4: Categorize Venues Using Full Text Search

Each venue gets classified into one of these categories: 'Restaurant', 'Club', 'Museum', 'Shop', or 'Others', using full-text search.

**PostgreSQL & Citus:** Uses `to_tsvector` and `to_tsquery` for full-text search. The GIN index on the category column makes this fast.

```sql
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
```

**ScyllaDB:** ScyllaDB has no full-text search support, so I used simple Python string matching (`"restaurant" in category.lower()`) as a workaround.

```python
rows = session.execute("SELECT venue_id, category FROM pois")
counter = Counter()
for r in rows:
    cat = r.category.lower() if r.category else ""
    if "restaurant" in cat:     counter["Restaurant"] += 1
    elif "club" in cat:         counter["Club"] += 1
    elif "museum" in cat:       counter["Museum"] += 1
    elif "shop" in cat:         counter["Shop"] += 1
    else:                       counter["Others"] += 1
```

**MongoDB:** Uses `$text` search with a text index on the `category` field.

```javascript
// For each category:
db.pois.countDocuments({$text: {$search: "Restaurant"}})
db.pois.countDocuments({$text: {$search: "Club"}})
db.pois.countDocuments({$text: {$search: "Museum"}})
db.pois.countDocuments({$text: {$search: "Shop"}})
// Others = total - sum of above
```

![PostgreSQL Q4](screenshots/pg-q4.png)
![Citus Q4](screenshots/citus-q3-4.png)
![ScyllaDB Q4](screenshots/scylla-q4.png)
![MongoDB Q4](screenshots/mongo-q4.png)

---

## IV. Performance Analysis & Visualization

### A. Summary Table [10 points]

The `performance.py` script reads the result JSON files and generates the summary tables and charts.

**Query Performance Summary (average seconds, 3 runs each):**

| Database   | Q1     | Q2      | Q3      | Q4     | Setup                    | CPU cores  | Memory       |
|------------|--------|---------|---------|--------|--------------------------|------------|--------------|
| PostgreSQL |  2.87  |  56.19  |   4.27  |  3.95  | A single server          | 16         | 0.33 GiB     |
| Citus Data |  4.48  |   2.82  |   7.23  |  3.98  | A cluster of 3 nodes     | 16         | 0.54 GiB     |
| ScyllaDB   | 73.94  |  62.71  | 117.07  | 24.26  | A cluster of 3 nodes     | 3          | 1.19 GiB     |
| MongoDB    |  7.08  |  38.86  |  23.62  |  2.62  | A replica set of 3 nodes | 16         | 1.20 GiB     |

![Performance script 1](screenshots/performance-1.png)
![Performance script 2](screenshots/performance-2.png)
![Performance script 3](screenshots/performance-3.png)

### B. Visualization [6 points]

Bar charts comparing query performance across the four databases.

![Performance comparison chart](performance_chart.png)
![Per-query performance chart](performance_per_query.png)

### C. Analysis [10 points]

**Q1 - Top 10 Countries by Check-ins:**
PostgreSQL was the fastest here (2.87s) since it can use the indexes on `checkins(venue_id)` and `pois(country)` to do the JOIN and GROUP BY efficiently. Citus was a bit slower (4.48s) - there's some overhead from coordinating the aggregation across shards, even though `pois` is a reference table. MongoDB did okay (7.08s) because the country is already embedded in each checkin doc so it's just a `$group`, but it still has to scan every document. ScyllaDB was much slower (73.94s) because it can't do aggregation on the server side, so everything gets pulled to the Python client and counted there.

**Q2 - Users Preferring Friends' POIs:**
Citus was really fast on Q2 (2.82s) because friendships and checkins are both sharded by `user_id`, so a lot of the joins stay on the same shard. The repartition join handles the cross-shard part for the friend side. PostgreSQL took much longer (56.19s) since it has to do a big three-way join on a single server with no parallelism. For MongoDB (38.86s) and ScyllaDB (62.71s), I had to do all the processing in Python - loading friendships, computing intersections, building venue sets - which is just slower than letting the database handle it.

**Q3 - Most Attractive Venues:**
PostgreSQL was fastest again (4.27s) with its JOIN and GROUP BY working well on one server. Citus was slower (7.23s) because the GROUP BY is on `venue_id` but the table is distributed by `user_id`, so data has to be shuffled between shards. MongoDB (23.62s) has to scan all the checkin docs and run the aggregation pipeline, which isn't as fast as PostgreSQL for this kind of grouped counting. ScyllaDB was slowest again (117.07s) - it has to stream the whole `checkins_by_country` table to Python and do all the counting there.

**Q4 - Venue Categorization with Full Text Search:**
MongoDB was fastest on Q4 (2.62s) thanks to its text index on `category` - each category search is basically just an index lookup. PostgreSQL (3.95s) and Citus (3.98s) were close behind using the GIN index with `to_tsvector/to_tsquery`. Citus has no advantage here since `pois` is a reference table, so the query only runs on one copy anyway. ScyllaDB was slowest (24.26s) because it has no full-text search at all, so I had to scan every POI and do string matching in Python.

## Docker Infrastructure

All databases run in Docker containers with `docker-compose.yml`:

- **PostgreSQL 16**: Single server, port 5432
- **Citus 12.1**: Coordinator + 2 workers, port 5433
- **ScyllaDB 5.4**: 3-node cluster, port 9042
- **MongoDB 7**: 3-node replica set, port 27017

![Docker containers 1](screenshots/docker-1.png)
![Docker containers 2](screenshots/docker-2.png)
![Docker containers 3](screenshots/docker-3.png)

I used `docker_stats.py` to get statistics from Docker containers (memory usage etc.):

![Docker stats 1](screenshots/docker-stats-1.png)
![Docker stats 2](screenshots/docker-stats-2.png)
![Docker stats 3](screenshots/docker-stats-3.png)