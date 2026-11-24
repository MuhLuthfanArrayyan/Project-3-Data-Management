# Image loader Part-3: menjalankan load_couchbase_from_pg.py dan
# load_cassandra.py di dalam jaringan compose (lihat docker-compose.yml,
# service `loader`). Skrip-nya di-bind-mount dari repo ke /app, jadi image
# ini hanya menyediakan Python + driver.
FROM python:3.11-slim

# psycopg  -> baca dump dari PostgreSQL
# cassandra-driver -> tulis ledger ke Cassandra
# couchbase -> tulis dokumen ke Couchbase
# pyasyncore -> jaring pengaman untuk Python 3.12+ (asyncore dihapus dari stdlib)
RUN pip install --no-cache-dir \
      "psycopg[binary]" \
      "cassandra-driver>=3.29" \
      "couchbase>=4.1" \
      pyasyncore

WORKDIR /app
