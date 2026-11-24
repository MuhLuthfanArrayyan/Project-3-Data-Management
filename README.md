# Project-3-Data-Management

## Getting Started

`docker-compose.yml` sekarang mengorkestrasi seluruh alur.

```powershell
docker compose up -d
docker compose ps        # tunggu sampai postgres/cassandra/couchbase "healthy"
```

Setelah itu kedua DB bisa langsung di-query, misalnya:

```powershell
python run_read_queries.py
docker exec -i bri-cassandra cqlsh < queries_read_cql.cql
```
