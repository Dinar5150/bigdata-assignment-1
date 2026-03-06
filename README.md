# How to run

## 1. Install Python deps
```
pip install -r requirements.txt
```

## 2. Data preparation (skip if my_*.tsv files already exist)
```
python prepare_data.py
```

## 3. Start all databases
```
docker-compose up -d
```
Wait ~30-60 seconds for all services to boot.

## 4. Setup clusters (Citus workers, ScyllaDB keyspace, Mongo replica set)
```
python setup_cluster.py
```

## 5. Ingest data into each database
```
python ingest_postgres.py
python ingest_citus.py
python ingest_scylladb.py
python ingest_mongodb.py
```

## 6. Run queries (Q1-Q4) on each database
```
python queries_postgres.py
python queries_citus.py
python queries_scylladb.py
python queries_mongodb.py
```

## 7. Generate performance summary and charts
```
python performance.py
```

## 8. Stop everything
```
docker-compose down
```
To also delete volumes: `docker-compose down -v`
