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

## Cara Query

### Couchbase

#### Menggunakan CLI

```powershell
docker exec bri-couchbase cbq -u Administrator -p password -e http://localhost:8093 -s "-- <query>"
```

#### Menggunakan Web UI

1. Buka [http://localhost:8091/](http://localhost:8091/).
2. Login ke akun Couchbase. Username `Administrator` dan password `password`.
3. Navigasi ke bagian query dan masukkan query.


### Cassandra

#### Menggunakan CLI 

```powershell
docker exec bri-cassandra cqlsh -e "-- query"
```

