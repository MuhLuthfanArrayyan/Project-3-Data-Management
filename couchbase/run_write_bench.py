"""Benchmark 10 query WRITE versi Couchbase (PLAN.md §4).

Pemetaan engine:
  W-01..W-06, W-08..W-10 -> Couchbase (dokumen/GSI)
  W-07                   -> Couchbase (transaksi ACID update saldo + field turunan)
                          + Cassandra (append ledger transaksi_by_rekening)

Untuk tiap query: 5 run (+1 run bertrace), state dipulihkan sebelum tiap run.
Hasil -> couchbase/write_bench_results.json

Jalankan: python couchbase/run_write_bench.py
  Couchbase  : couchbase://localhost  (override: CB_CONN_STR, CB_USERNAME, CB_PASSWORD)
  Cassandra  : 127.0.0.1:9042         (override: CASSANDRA_HOST, CASSANDRA_PORT)
"""
import json
import os
import statistics
import time
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import DocumentNotFoundException
from couchbase.options import ClusterOptions, QueryOptions

CB_CONN = os.getenv("CB_CONN_STR", "couchbase://localhost")
CB_USER = os.getenv("CB_USERNAME", "Administrator")
CB_PASS = os.getenv("CB_PASSWORD", "password")
CB_BUCKET = "bri_tabungan"
CB_SCOPE = "operasional"
CASS_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
CASS_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))

# Rekening/data uji (dump yang diverifikasi).
REK = "002101000000001"          # saldo 151263509.16 / 151213509.16, tx_count 41
REK2 = "002101000000004"
NID_NEW, NIK_NEW = "NSB900000001", "3201010101900001"
NID_EXIST = "NSB000000002"
REK_NEW = "002109000000001"
CARD_NEW = "5221849999999999"
POST_PERIODE = "202611"
INS_NEW = "INSW09000000001"
TRX_NEW = "TRX900000001"
LEDGER_TS = datetime(2026, 11, 21, 12, 0, 0)


# ---------------------------------------------------------------------------
# Koneksi
# ---------------------------------------------------------------------------
cluster = Cluster(CB_CONN, ClusterOptions(PasswordAuthenticator(CB_USER, CB_PASS)))
cluster.wait_until_ready(timedelta(seconds=30))
scope = cluster.bucket(CB_BUCKET).scope(CB_SCOPE)


def col(name):
    return scope.collection(name)


# Cassandra opsional: W-07 tetap berjalan (bagian Couchbase) bila tak tersedia.
cass_session = None
cass_insert = None
try:
    from cassandra.cluster import Cluster as CCluster
    from cassandra.io.asyncioreactor import AsyncioConnection
    _c = CCluster([CASS_HOST], port=CASS_PORT, connection_class=AsyncioConnection)
    cass_session = _c.connect("bri_tabungan")
    cass_session.default_timeout = 60
    cass_insert = cass_session.prepare(
        "insert into transaksi_by_rekening (no_rekening,waktu_transaksi,id_transaksi,"
        "jenis_transaksi,arah,nominal,mata_uang,id_kanal,nama_kanal,no_kartu,status_transaksi,saldo_setelah) "
        "values (?,?,?,?,?,?,?,?,?,?,?,?)"
    )
    print("Cassandra terhubung -> W-07 mencakup append ledger")
except Exception as e:
    print("Cassandra tidak tersedia -> W-07 hanya bagian Couchbase:", type(e).__name__)


# ---------------------------------------------------------------------------
# Referensi awal (dibaca dari DB, bukan hardcoded)
# ---------------------------------------------------------------------------
def one(sql, **params):
    rows = list(cluster.query(sql, QueryOptions(named_parameters=params)))
    return rows[0] if rows else None


INIT = dict(col("rekening").get(f"rekening::{REK}").content)
INIT2 = dict(col("rekening").get(f"rekening::{REK2}").content)
CARD_A = one(
    "SELECT RAW no_kartu FROM `bri_tabungan`.`operasional`.`kartu_debit` "
    "WHERE no_rekening = $r LIMIT 1", r=REK)
CARD_INIT = dict(col("kartu_debit").get(f"kartu_debit::{CARD_A}").content) if CARD_A else None
print("state awal:", {k: INIT.get(k) for k in ("saldo_efektif", "saldo_tersedia", "tx_count", "status_rekening")},
      "| kartu uji:", CARD_A)


def txn(logic):
    cluster.transactions().run(logic)


def restore_rekening(target, doc):
    col("rekening").replace(f"rekening::{target}", dict(doc))


# ---------------------------------------------------------------------------
# W-01..W-10 — build() mengembalikan [(label, callable)], cleanup() memulihkan
# ---------------------------------------------------------------------------
def w1_build():
    doc = {
        "_type": "nasabah", "id_nasabah": NID_NEW, "nik": NIK_NEW,
        "nama_lengkap": "Budi Santoso", "jenis_nasabah": "INDIVIDU",
        "segmen_nasabah": "RITEL", "status_kyc": "MENUNGGU_VERIFIKASI",
        "tanggal_registrasi": "2026-11-21", "rekening_refs": [],
    }

    def op():
        def logic(ctx):
            ctx.insert(col("nasabah"), f"nik::{NIK_NEW}", {"id_nasabah": NID_NEW})
            ctx.insert(col("nasabah"), f"nasabah::{NID_NEW}", doc)
        txn(logic)
    return [("txn: guard nik + dokumen nasabah", op)]


def w1_cleanup():
    for k in (f"nik::{NIK_NEW}", f"nasabah::{NID_NEW}"):
        try:
            col("nasabah").remove(k)
        except DocumentNotFoundException:
            pass


def w2_build():
    doc = {
        "_type": "rekening", "no_rekening": REK_NEW, "id_produk": "PRD01",
        "mata_uang": "IDR", "tanggal_buka": "2026-11-21", "tanggal_tutup": None,
        "status_rekening": "AKTIF", "saldo_efektif": 0.0, "saldo_tersedia": 0.0,
        "id_unit_kerja": "UK0005", "pemegang_rekening": [],
        "last_transaction_at": None, "tx_count": 0,
    }
    return [("insert dokumen rekening", lambda: col("rekening").insert(f"rekening::{REK_NEW}", doc))]


def w2_cleanup():
    try:
        col("rekening").remove(f"rekening::{REK_NEW}")
    except DocumentNotFoundException:
        pass


def w3_build():
    def op():
        def logic(ctx):
            r = ctx.get(col("rekening"), f"rekening::{REK}")
            r.content["pemegang_rekening"].append({
                "id_nasabah": NID_EXIST, "peran": "PESERTA",
                "penanda_utama": "N", "tanggal_mulai": "2026-11-21"})
            ctx.replace(r, r.content)
            n = ctx.get(col("nasabah"), f"nasabah::{NID_EXIST}")
            n.content.setdefault("rekening_refs", []).append({
                "no_rekening": REK, "peran": "PESERTA", "penanda_utama": "N"})
            ctx.replace(n, n.content)
        txn(logic)
    return [("txn: pemegang_rekening[] + nasabah.rekening_refs[]", op)]


def w3_cleanup():
    r = col("rekening").get(f"rekening::{REK}").content
    r["pemegang_rekening"] = [
        h for h in r["pemegang_rekening"]
        if not (h.get("id_nasabah") == NID_EXIST and h.get("peran") == "PESERTA")]
    col("rekening").replace(f"rekening::{REK}", r)
    n = col("nasabah").get(f"nasabah::{NID_EXIST}").content
    n["rekening_refs"] = [
        x for x in n.get("rekening_refs", [])
        if not (x.get("no_rekening") == REK and x.get("peran") == "PESERTA")]
    col("nasabah").replace(f"nasabah::{NID_EXIST}", n)


def w4_build():
    doc = {
        "_type": "kartu_debit", "no_kartu": CARD_NEW, "no_rekening": REK,
        "bentuk_kartu": "FISIK", "jaringan_kartu": "VISA", "status_kartu": "AKTIF",
        "tanggal_terbit": "2026-11-21", "tanggal_kadaluarsa": "2031-11-01",
    }
    return [("insert dokumen kartu_debit", lambda: col("kartu_debit").insert(f"kartu_debit::{CARD_NEW}", doc))]


def w4_cleanup():
    try:
        col("kartu_debit").remove(f"kartu_debit::{CARD_NEW}")
    except DocumentNotFoundException:
        pass


def w5_build():
    def op():
        r = col("rekening").get(f"rekening::{REK}")
        r.content["status_rekening"] = "TERBLOKIR"
        col("rekening").replace(f"rekening::{REK}", r.content)
    return [("update status_rekening=TERBLOKIR", op)]


def w5_cleanup():
    restore_rekening(REK, INIT)


def w6_build():
    def op():
        c = col("kartu_debit").get(f"kartu_debit::{CARD_A}")
        c.content["status_kartu"] = "BLOKIR_SEMENTARA"
        col("kartu_debit").replace(f"kartu_debit::{CARD_A}", c.content)
    return [("update status_kartu=BLOKIR_SEMENTARA", op)]


def w6_cleanup():
    if CARD_INIT:
        col("kartu_debit").replace(f"kartu_debit::{CARD_A}", dict(CARD_INIT))


def w7_build():
    def cb():
        def logic(ctx):
            r = ctx.get(col("rekening"), f"rekening::{REK}")
            r.content["saldo_efektif"] = float(INIT["saldo_efektif"]) + 500000.0
            r.content["saldo_tersedia"] = float(INIT["saldo_tersedia"]) + 500000.0
            r.content["last_transaction_at"] = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
            r.content["tx_count"] = int(INIT.get("tx_count", 0)) + 1
            ctx.replace(r, r.content)
        txn(logic)

    def cass():
        cass_session.execute(cass_insert, [
            REK, LEDGER_TS, TRX_NEW, "SETORAN", "KREDIT", Decimal("500000.00"),
            "IDR", "KNL01", "BRImo Super App", CARD_A, "BERHASIL",
            Decimal(str(INIT["saldo_efektif"])) + Decimal("500000.00"),
        ])

    ops = [("Couchbase txn: saldo + last_transaction_at + tx_count", cb)]
    if cass_session:
        ops.append(("Cassandra: append ledger transaksi_by_rekening", cass))
    return ops


def w7_cleanup():
    restore_rekening(REK, INIT)
    if cass_session:
        try:
            cass_session.execute(
                "delete from transaksi_by_rekening where no_rekening=%s and waktu_transaksi=%s and id_transaksi=%s",
                [REK, LEDGER_TS, TRX_NEW])
        except Exception:
            pass


def w8_build():
    doc = {
        "_type": "posting_bunga", "no_rekening": REK, "periode": POST_PERIODE,
        "hari_berjalan": 30, "tarif_bunga": 0.015, "bunga_kotor": 12500.00,
        "pajak_pph_final": 2500.00, "bunga_bersih": 10000.00,
        "tanggal_posting": "2026-11-30T23:59:59",
    }
    return [("insert posting_bunga", lambda: col("posting_bunga").insert(f"posting::{REK}::{POST_PERIODE}", doc))]


def w8_cleanup():
    try:
        col("posting_bunga").remove(f"posting::{REK}::{POST_PERIODE}")
    except DocumentNotFoundException:
        pass


def w9_build():
    doc = {
        "_type": "instruksi_terjadwal", "id_instruksi": INS_NEW, "jenis_instruksi": "AFT",
        "no_rekening_sumber": REK, "no_rekening_tujuan": REK2, "frekuensi": "BULANAN",
        "hari_eksekusi": 25, "nominal_target": 1000000.00,
        "status_instruksi": "AKTIF", "tanggal_mulai": "2026-11-21", "tanggal_selesai": None,
    }
    return [("insert instruksi_terjadwal", lambda: col("instruksi_terjadwal").insert(f"instruksi::{INS_NEW}", doc))]


def w9_cleanup():
    try:
        col("instruksi_terjadwal").remove(f"instruksi::{INS_NEW}")
    except DocumentNotFoundException:
        pass


def w10_build():
    def op():
        r = col("rekening").get(f"rekening::{REK2}")
        r.content["status_rekening"] = "DITUTUP"
        r.content["tanggal_tutup"] = "2026-11-21"
        r.content["saldo_efektif"] = 0
        r.content["saldo_tersedia"] = 0
        col("rekening").replace(f"rekening::{REK2}", r.content)
    return [("update status_rekening=DITUTUP", op)]


def w10_cleanup():
    restore_rekening(REK2, INIT2)


SC = [
    ("W1 Mendaftarkan nasabah baru", w1_build, w1_cleanup),
    ("W2 Membuka rekening tabungan baru", w2_build, w2_cleanup),
    ("W3 Mencatat pemegang rekening", w3_build, w3_cleanup),
    ("W4 Mendaftarkan kartu debit", w4_build, w4_cleanup),
    ("W5 Memblokir rekening", w5_build, w5_cleanup),
    ("W6 Memblokir sementara kartu debit", w6_build, w6_cleanup),
    ("W7 Transaksi setoran + update saldo (+ ledger)", w7_build, w7_cleanup),
    ("W8 Posting bunga bulanan", w8_build, w8_cleanup),
    ("W9 Instruksi terjadwal baru (AFT)", w9_build, w9_cleanup),
    ("W10 Penutupan rekening", w10_build, w10_cleanup),
]


def time_ops(ops):
    total = 0.0
    for _label, fn in ops:
        t = time.perf_counter()
        fn()
        total += (time.perf_counter() - t) * 1000
    return total


def main():
    if not CARD_A:
        print("PERINGATAN: tidak ada kartu untuk rekening uji -> W-06 dilewati")
    results = []
    for name, build, cleanup in SC:
        if name.startswith("W6") and not CARD_A:
            continue
        cleanup()
        runs = []
        for _ in range(5):
            runs.append(round(time_ops(build()), 3))
            cleanup()
        trace = []
        for label, fn in build():
            t = time.perf_counter()
            fn()
            trace.append({"op": label, "client_ms": round((time.perf_counter() - t) * 1000, 3)})
        cleanup()
        avg = round(statistics.mean(runs), 3)
        results.append({"name": name, "runs": runs, "avg": avg, "trace": trace})
        print(name, runs, "avg", avg, flush=True)

    out = Path(__file__).with_name("write_bench_results.json")
    out.write_text(json.dumps(results, indent=1, ensure_ascii=False), encoding="utf8")
    print(f"tersimpan {out}")


if __name__ == "__main__":
    try:
        main()
    finally:
        cluster.close()
