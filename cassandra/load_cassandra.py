"""Muat LEDGER transaksi Tabungan BRI dari PostgreSQL (dump `tabungan_bri.dump`
yang sudah di-restore) ke Cassandra.

Sesuai PLAN.md §3 & §6, Cassandra hanya menyimpan ledger transaksi:
  - transaksi_by_rekening       (R-01: riwayat per rekening)
  - transaksi_by_kanal_periode  (R-03: agregasi per kanal/bulan)
Entitas state (nasabah, rekening, kartu, dst.) ada di Couchbase, bukan di sini.

Jalankan: python load_cassandra.py
  PostgreSQL  : :5434  (override: PG_DSN)
  Cassandra   : :9042  (override: CASSANDRA_HOST, CASSANDRA_PORT)
"""
import os
import re
import time
from datetime import timezone
from pathlib import Path
import psycopg
from cassandra.cluster import Cluster
from cassandra.io.asyncioreactor import AsyncioConnection
from cassandra.concurrent import execute_concurrent_with_args

# Default = jalur host (kompatibel dengan SETUP_LOKAL.md). Di docker-compose,
# nilai ini di-override lewat env supaya menunjuk nama service.
PG = os.getenv(
    "PG_DSN",
    "host=localhost port=5434 dbname=tabungan_bri user=postgres password=password",
)
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))

cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT, connection_class=AsyncioConnection)
session = None
for _ in range(30):
    try:
        session = cluster.connect()
        break
    except Exception as e:
        print("menunggu Cassandra...", type(e).__name__)
        time.sleep(5)
if session is None:
    raise SystemExit("Cassandra tidak siap setelah 30 percobaan")

# buat skema. Komentar CQL (-- ...) dibuang SEBELUM split(';') supaya tanda ';'
# yang muncul di dalam komentar tidak memecah statement (dan komentar inline
# ikut terbuang).
raw = Path(__file__).with_name("schema.cql").read_text(encoding="utf8")
cql = "\n".join(re.sub(r"--.*$", "", line) for line in raw.splitlines())
for stmt in [s.strip() for s in cql.split(";") if s.strip()]:
    if not stmt.upper().startswith("USE"):
        session.execute(stmt)
        if stmt.upper().startswith("CREATE KEYSPACE"):
            session.set_keyspace("bri_tabungan")
session.set_keyspace("bri_tabungan")
session.default_timeout = 60


def load(table_sql, cql_insert, mapper=lambda r: r, name=""):
    t = time.time()
    with psycopg.connect(PG) as conn, conn.cursor(name="cur_" + name or "c") as cur:
        cur.itersize = 20000
        cur.execute(table_sql)
        prep = session.prepare(cql_insert)
        n = 0
        batch = []
        for row in cur:
            batch.append(mapper(row))
            if len(batch) >= 5000:
                for ok, res in execute_concurrent_with_args(session, prep, batch, concurrency=64, raise_on_first_error=True):
                    pass
                n += len(batch); batch = []
        if batch:
            execute_concurrent_with_args(session, prep, batch, concurrency=64, raise_on_first_error=True)
            n += len(batch)
    print(f"{name:25s} {n:>8} baris  {time.time()-t:6.1f}s", flush=True)


# --- LEDGER: R-01 ---------------------------------------------------------
load("""select t.no_rekening,t.waktu_transaksi,t.id_transaksi,t.jenis_transaksi,t.arah,t.nominal,t.mata_uang,t.id_kanal,k.nama_kanal,t.no_kartu,t.status_transaksi,t.saldo_setelah
        from transaksi_tabungan t join kanal_layanan k using(id_kanal)""",
     "insert into transaksi_by_rekening (no_rekening,waktu_transaksi,id_transaksi,jenis_transaksi,arah,nominal,mata_uang,id_kanal,nama_kanal,no_kartu,status_transaksi,saldo_setelah) values (?,?,?,?,?,?,?,?,?,?,?,?)",
     name="transaksi_by_rekening")


# --- LEDGER: R-03 ---------------------------------------------------------
def kanal_periode_mapper(row):
    """TIMESTAMPTZ -> naive UTC, sama dengan nilai yang dipakai seed_bri.py."""
    values = list(row)
    when = values[4]
    if when is not None:
        values[4] = when.astimezone(timezone.utc).replace(tzinfo=None)
    return tuple(values)


load("""select k.id_kanal, to_char(t.waktu_transaksi, 'YYYYMM') as periode,
         t.status_transaksi, t.jenis_transaksi, t.waktu_transaksi,
         t.id_transaksi, k.nama_kanal, t.nominal
         from transaksi_tabungan t join kanal_layanan k using(id_kanal)""",
     "insert into transaksi_by_kanal_periode (id_kanal,periode,status_transaksi,jenis_transaksi,waktu_transaksi,id_transaksi,nama_kanal,nominal) values (?,?,?,?,?,?,?,?)",
     mapper=kanal_periode_mapper, name="transaksi_by_kanal_periode")
print("SELESAI")
cluster.shutdown()
