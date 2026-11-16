"""
Seeder Couchbase — Project Part-3 (Tabungan BRI)
=================================================

Port 1:1 dari part-2/seed_bri.py. Logika dan URUTAN pemanggilan
random.*/generate.* dibuat PERSIS sama (random.seed(42), Faker.seed(42))
supaya data yang dihasilkan di Couchbase equivalent -- idealnya identik --
dengan data relasional yang sudah ada di Part-2. Yang berbeda hanya TUJUAN
TULISnya:

  - produk, unit_kerja, kanal, nasabah, rekening (+ pemegang_rekening
    di-EMBED ke dalam rekening & nasabah), kartu_debit, posting_bunga,
    instruksi_terjadwal  -> ditulis ke Couchbase (bucket bri_tabungan,
    scope operasional), sesuai part-3/couchbase_ddl.n1ql.

  - transaksi_tabungan TIDAK ditulis ke Couchbase -- itu scope Cassandra
    (lihat part-3/PLAN.md §6). Generator transaksinya tetap DIJALANKAN
    APA ADANYA (urutan panggilan random harus identik dgn seed_bri.py!),
    tapi hasilnya hanya dipakai untuk mengisi field TURUNAN pada dokumen
    `rekening`:
        * saldo_efektif / saldo_tersedia  (saldo akhir setelah semua tx)
        * last_transaction_at             (waktu transaksi BERHASIL terakhir)
        * tx_count                        (jumlah transaksi tercatat)

CATATAN KESETARAAN (equivalence):
  Supaya nilai yang dihasilkan benar-benar IDENTIK (bukan cuma equivalent)
  dengan yang sudah di-seed ke Postgres di part-2, jalankan script ini
  dengan versi `Faker` yang SAMA dengan yang dipakai part-2/.venv (beda
  versi Faker bisa mengubah daftar nama/kota internal walau seed sama).
  Kalau itu tidak memungkinkan, datanya tetap EQUIVALENT: struktur, jumlah,
  dan distribusi (persentase status, dsb) tetap sama persis.

Install:
    pip install couchbase faker
    # atau aktifkan dulu venv yang sama dengan part-2: part-2\\.venv\\Scripts\\activate
    # lalu: pip install couchbase

Jalankan:
    python seed_couchbase.py
"""

import random
from datetime import date, datetime, timedelta
from faker import Faker

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions

# ----------------------------------------------------------------------------
# KONFIGURASI — sesuaikan dengan environment Couchbase-mu
# ----------------------------------------------------------------------------
CB_CONN_STR = "couchbase://localhost"
CB_USERNAME = "Administrator"
CB_PASSWORD = "Password123"
CB_BUCKET = "bri_tabungan"
CB_SCOPE = "operasional"

generate = Faker('id_ID')
Faker.seed(42)
random.seed(42)

# ---- cache (identik dengan seed_bri.py, dipakai untuk menyusun referensi) --
produk_ids = []
unit_ids = []
kanal_ids = []
nasabah_ids = []
rekening_ids = []
rekening_aktif_ids = []          # khusus rekening berstatus AKTIF
kartu_map = {}                   # {no_rekening: [no_kartu, ...]}
rekening_info_map = {}           # {no_rekening: {'saldo_efektif':..., 'hold':...}}

# ---- dokumen yang akan di-upsert ke Couchbase (key -> dict) ----------------
docs_produk = {}
docs_unit_kerja = {}
docs_kanal = {}
docs_nasabah = {}
docs_rekening = {}
docs_kartu = {}
docs_posting = {}
docs_instruksi = {}


def iso(d):
    """date/datetime -> string ISO 8601; None tetap None (json null)."""
    return d.isoformat() if d is not None else None


# ----------------------------------------------------------------------------
# HELPER — upsert satu collection secara batch
# ----------------------------------------------------------------------------
def upsert_all(collection, docs: dict, label: str, batch_size: int = 1000):
    items = list(docs.items())
    total = len(items)
    for i in range(0, total, batch_size):
        batch = dict(items[i:i + batch_size])
        try:
            collection.upsert_multi(batch)              # SDK Python >= 4.1 (cepat)
        except AttributeError:
            for k, v in batch.items():                   # fallback SDK lama
                collection.upsert(k, v)
        print(f"  ... {label}: {min(i + batch_size, total)}/{total}")
    print(f"selesai upsert {label} ({total} dokumen)")


# ----------------------------------------------------------------------------
# 1. produk_tabungan (10, statis)
# ----------------------------------------------------------------------------
def backfill_produk():
    global produk_ids
    produk_list = [
        ('PRD01', 'BritAma Umum', 'BRITAMA', 'UMUM', 'IDR', 50000.00, 'TIER', 12000.00, 'AKTIF'),
        ('PRD02', 'BritAma Bisnis', 'BRITAMA', 'MITRA', 'IDR', 100000.00, 'TIER', 20000.00, 'AKTIF'),
        ('PRD03', 'Simpedes Utama', 'SIMPEDES', 'UMUM', 'IDR', 25000.00, 'FLAT', 5500.00, 'AKTIF'),
        ('PRD04', 'Simpedes UMi', 'SIMPEDES', 'UMUM', 'IDR', 10000.00, 'FLAT', 0.00, 'AKTIF'),
        ('PRD05', 'BRI Junio', 'JUNIO', 'ANAK', 'IDR', 50000.00, 'FLAT', 5000.00, 'AKTIF'),
        ('PRD06', 'SimPel BRI', 'SIMPEL', 'PELAJAR', 'IDR', 5000.00, 'FLAT', 0.00, 'AKTIF'),
        ('PRD07', 'Tabungan Haji BRI', 'HAJI', 'UMUM', 'IDR', 500000.00, 'FLAT', 0.00, 'AKTIF'),
        ('PRD08', 'Tabunganku BRI', 'TABUNGANKU', 'UMUM', 'IDR', 20000.00, 'FLAT', 0.00, 'AKTIF'),
        ('PRD09', 'BritAma Valas', 'BRITAMA', 'PRIORITAS', 'USD', 100000.00, 'TIER', 25000.00, 'AKTIF'),
        ('PRD10', 'BritAma Muda', 'SIMUDA', 'PELAJAR', 'IDR', 50000.00, 'TIER', 10000.00, 'AKTIF'),
    ]
    for (id_produk, nama, keluarga, segmen, mata_uang, saldo_min, skema, biaya, status) in produk_list:
        docs_produk[f"produk::{id_produk}"] = {
            "_type": "produk", "id_produk": id_produk, "nama_produk": nama,
            "keluarga_produk": keluarga, "segmen_tujuan": segmen, "mata_uang": mata_uang,
            "saldo_minimum": saldo_min, "skema_bunga": skema, "biaya_admin": biaya,
            "status_produk": status,
        }
    produk_ids = [p[0] for p in produk_list]
    print("selesai generate produk_tabungan")


# ----------------------------------------------------------------------------
# 2. unit_kerja (4 statis + 46 acak, self-hierarchy)
# ----------------------------------------------------------------------------
def backfill_unit_kerja():
    global unit_ids
    units = [
        ('UK0001', 'Kantor Pusat BRI', 'HO', 'Jl. Jend. Sudirman No.44-46, Jakarta Pusat', None),
        ('UK0002', 'Kanwil BRI Jakarta 1', 'RO', 'Jl. Veteran No.8, Jakarta Pusat', 'UK0001'),
        ('UK0003', 'Kanwil BRI Bandung', 'RO', 'Jl. Asia Afrika No.51, Bandung', 'UK0001'),
        ('UK0004', 'Kanwil BRI Surabaya', 'RO', 'Jl. Jend. Basuki Rachmat No.122, Surabaya', 'UK0001'),
    ]
    for i in range(5, 51):
        uid = f"UK{i:04d}"
        jenis = random.choice(['CABANG', 'SUB_CABANG', 'UNIT', 'CASH_OFFICE', 'BRI_TERRACE'])
        induk = random.choice(['UK0002', 'UK0003', 'UK0004'])
        nama = f"BRI {jenis.title()} {generate.city()}"
        alamat = generate.address().replace('\n', ', ')[:140]
        units.append((uid, nama, jenis, alamat, induk))

    for (uid, nama, jenis, alamat, induk) in units:
        docs_unit_kerja[f"unit_kerja::{uid}"] = {
            "_type": "unit_kerja", "id_unit_kerja": uid, "nama_unit_kerja": nama,
            "jenis_unit_kerja": jenis, "alamat_lokasi": alamat, "id_unit_induk": induk,
        }
    unit_ids = [u[0] for u in units]
    print("selesai generate unit_kerja")


# ----------------------------------------------------------------------------
# 3. kanal_layanan (7, statis)
# ----------------------------------------------------------------------------
def backfill_kanal():
    global kanal_ids
    kanals = [
        ('KNL01', 'BRImo Super App', 'DIGITAL', 'ONLINE'),
        ('KNL02', 'Internet Banking BRI', 'DIGITAL', 'ONLINE'),
        ('KNL03', 'ATM / CRM BRI', 'SELF_SERVICE', 'ONLINE'),
        ('KNL04', 'Teller Kantor Cabang', 'TELLER_UNIT', 'ONLINE'),
        ('KNL05', 'AgenBRILink', 'AGENT', 'ONLINE'),
        ('KNL06', 'EDC Merchant BRI', 'MERCHANT', 'ONLINE'),
        ('KNL07', 'Kiosk Self Service', 'SELF_SERVICE', 'MAINTENANCE'),
    ]
    for (kid, nama, tipe, status) in kanals:
        docs_kanal[f"kanal::{kid}"] = {
            "_type": "kanal", "id_kanal": kid, "nama_kanal": nama,
            "tipe_kanal": tipe, "status_operasional": status,
        }
    kanal_ids = [k[0] for k in kanals]
    print("selesai generate kanal_layanan")


# ----------------------------------------------------------------------------
# 4. nasabah (10.000) — rekening_refs diisi belakangan (embed)
# ----------------------------------------------------------------------------
def backfill_nasabah():
    global nasabah_ids
    total_nasabah = 10000
    used_niks = set()

    for i in range(1, total_nasabah + 1):
        nid = f"NSB{i:09d}"
        jenis = random.choices(['INDIVIDU', 'KORPORASI', 'LEMBAGA'], weights=[90, 7, 3])[0]

        if jenis == 'INDIVIDU':
            while True:
                nik = (f"32{random.randint(1, 99):02d}{random.randint(1, 99):02d}"
                       f"{random.randint(1, 28):02d}{random.randint(1, 12):02d}"
                       f"{random.randint(50, 99):02d}{random.randint(1000, 9999)}")
                if nik not in used_niks:
                    used_niks.add(nik)
                    break
            nama = generate.name()
            segmen = random.choices(['MIKRO_UMI', 'RITEL', 'PRIORITAS', 'PRIVATE'], weights=[60, 30, 8, 2])[0]
        else:
            nik = f"NPWP{random.randint(100000000000, 999999999999)}"
            nama = generate.company()
            segmen = 'KORPORASI'

        status_kyc = random.choices(['TERVERIFIKASI', 'MENUNGGU_VERIFIKASI', 'PERLU_REKYC'], weights=[92, 5, 3])[0]
        tgl_reg = generate.date_between(start_date='-5y', end_date='today')

        docs_nasabah[f"nasabah::{nid}"] = {
            "_type": "nasabah", "id_nasabah": nid, "nik": nik, "nama_lengkap": nama,
            "jenis_nasabah": jenis, "segmen_nasabah": segmen, "status_kyc": status_kyc,
            "tanggal_registrasi": iso(tgl_reg),
            "rekening_refs": [],   # diisi di backfill_pemegang_rekening()
        }
        nasabah_ids.append(nid)

    print("selesai generate nasabah")


# ----------------------------------------------------------------------------
# 5. rekening (12.000) — pemegang_rekening diisi belakangan (embed)
# ----------------------------------------------------------------------------
def backfill_rekening():
    global rekening_ids, rekening_aktif_ids, rekening_info_map
    total_rekening = 12000

    for i in range(1, total_rekening + 1):
        norek = f"002101{i:09d}"
        id_prd = random.choice(produk_ids)
        id_uk = random.choice(unit_ids)
        tgl_buka = generate.date_between(start_date='-4y', end_date='-1M')

        status_rek = random.choices(['AKTIF', 'DORMAN', 'TERBLOKIR', 'DITUTUP'], weights=[85, 8, 4, 3])[0]
        tgl_tutup = None
        hold = 0.00

        if status_rek == 'DITUTUP':
            tgl_tutup = generate.date_between(start_date=tgl_buka, end_date='today')
            saldo_efektif = 0.00
            saldo_tersedia = 0.00
        else:
            saldo_efektif = round(random.uniform(500000, 500000000), 2)
            hold = float(random.choice([0, 50000, 100000]))
            saldo_tersedia = max(0.00, round(saldo_efektif - hold, 2))

        docs_rekening[f"rekening::{norek}"] = {
            "_type": "rekening", "no_rekening": norek, "id_produk": id_prd,
            "mata_uang": "IDR", "tanggal_buka": iso(tgl_buka), "tanggal_tutup": iso(tgl_tutup),
            "status_rekening": status_rek, "saldo_efektif": saldo_efektif, "saldo_tersedia": saldo_tersedia,
            "id_unit_kerja": id_uk,
            "pemegang_rekening": [],        # diisi di backfill_pemegang_rekening()
            "last_transaction_at": None,    # diisi di backfill_transaksi_derived()
            "tx_count": 0,
        }
        rekening_ids.append(norek)

        if status_rek == 'AKTIF':
            rekening_aktif_ids.append(norek)
            rekening_info_map[norek] = {'saldo_efektif': saldo_efektif, 'hold': hold}

    print("selesai generate rekening_tabungan")


# ----------------------------------------------------------------------------
# 6. pemegang_rekening (M:N) -> di-EMBED ke rekening.pemegang_rekening
#    DAN ke nasabah.rekening_refs (denormalisasi, lihat couchbase_ddl.n1ql §6)
# ----------------------------------------------------------------------------
def backfill_pemegang_rekening():
    pemegang_data = []
    # 90% nasabah pertama jadi kandidat pemegang; 10% sisanya sengaja TANPA
    # rekening (dipakai untuk uji query R-05: nasabah tanpa rekening).
    active_nasabah_subset = nasabah_ids[:9000]

    for idx, norek in enumerate(rekening_ids):
        nid = active_nasabah_subset[idx % len(active_nasabah_subset)]
        pemegang_data.append((norek, nid, 'PEMEGANG_UTAMA', 'Y',
                               generate.date_between(start_date='-4y', end_date='-1M')))

    joint_reks = random.sample(rekening_ids, 800)
    for norek in joint_reks:
        nid = random.choice(active_nasabah_subset)
        pemegang_data.append((norek, nid, 'PESERTA', 'N',
                               generate.date_between(start_date='-3y', end_date='-1M')))

    unique_pemegang = list({(p[0], p[1]): p for p in pemegang_data}.values())

    for (norek, nid, peran, utama, tgl_mulai) in unique_pemegang:
        docs_rekening[f"rekening::{norek}"]["pemegang_rekening"].append({
            "id_nasabah": nid, "peran": peran, "penanda_utama": utama,
            "tanggal_mulai": iso(tgl_mulai),
        })
        docs_nasabah[f"nasabah::{nid}"]["rekening_refs"].append({
            "no_rekening": norek, "peran": peran, "penanda_utama": utama,
        })

    print(f"selesai embed pemegang_rekening ({len(unique_pemegang)} relasi)")


# ----------------------------------------------------------------------------
# 7. kartu_debit — hanya untuk rekening AKTIF (peluang 80%)
# ----------------------------------------------------------------------------
def backfill_kartu_debit():
    global kartu_map
    today = date.today()

    for idx, norek in enumerate(rekening_aktif_ids):
        if random.random() < 0.8:
            nokartu = f"522184{idx + 1:010d}"
            bentuk = random.choice(['FISIK', 'VIRTUAL'])
            jaringan = random.choice(['MASTERCARD', 'VISA', 'GPN_PRIVATE_LABEL'])
            status = random.choices(['AKTIF', 'BLOKIR_SEMENTARA', 'KEDALUWARSA'], weights=[90, 5, 5])[0]

            # 10% kartu sengaja mendekati kadaluarsa (1-29 hari) -> uji query R-09
            if random.random() < 0.10:
                tgl_exp = generate.date_between(start_date=today + timedelta(days=1),
                                                 end_date=today + timedelta(days=29))
                tgl_terbit = tgl_exp - timedelta(days=5 * 365)
            else:
                tgl_terbit = generate.date_between(start_date='-5y', end_date='-1M')
                tgl_exp = tgl_terbit + timedelta(days=5 * 365)

            docs_kartu[f"kartu_debit::{nokartu}"] = {
                "_type": "kartu_debit", "no_kartu": nokartu, "no_rekening": norek,
                "bentuk_kartu": bentuk, "jaringan_kartu": jaringan, "status_kartu": status,
                "tanggal_terbit": iso(tgl_terbit), "tanggal_kadaluarsa": iso(tgl_exp),
            }
            kartu_map.setdefault(norek, []).append(nokartu)

    print(f"selesai generate kartu_debit ({len(docs_kartu)} kartu)")


# ----------------------------------------------------------------------------
# 8. transaksi_tabungan — DIJALANKAN APA ADANYA (urutan random harus identik
#    dgn seed_bri.py) tapi TIDAK ditulis ke Couchbase. Hanya field turunan
#    pada `rekening` yang disimpan (saldo akhir, last_transaction_at, tx_count).
#    Transaksi mentahnya adalah tugas seeder Cassandra (bri_ledger), terpisah.
# ----------------------------------------------------------------------------
def backfill_transaksi_derived():
    total_tx = 0
    reks_with_cards = list(kartu_map.keys())

    for norek in reks_with_cards:
        num_tx = random.randint(40, 80)
        info = rekening_info_map[norek]
        running_saldo = info['saldo_efektif']
        hold_amount = info['hold']

        timestamps = sorted([generate.date_time_between(start_date='-1y', end_date='now')
                              for _ in range(num_tx)])
        last_berhasil_at = None

        for waktu in timestamps:
            jenis = random.choice(['SETORAN', 'PENARIKAN', 'TRANSFER', 'PEMBAYARAN', 'BIAYA'])
            arah = 'KREDIT' if jenis == 'SETORAN' else 'DEBIT'
            nominal = round(random.uniform(50000, 2000000), 2)
            _ = random.choice(kanal_ids)          # dipertahankan agar urutan random tetap sama
            _ = random.choice(kartu_map[norek])   # idem

            if arah == 'DEBIT' and nominal > running_saldo:
                status = 'GAGAL'
            else:
                status = random.choices(['BERHASIL', 'GAGAL', 'PENDING'], weights=[94, 5, 1])[0]
                if status == 'BERHASIL':
                    running_saldo += nominal if arah == 'KREDIT' else -nominal
                    last_berhasil_at = waktu

            total_tx += 1

        final_efektif = round(running_saldo, 2)
        final_tersedia = max(0.00, round(final_efektif - hold_amount, 2))

        rk = docs_rekening[f"rekening::{norek}"]
        rk["saldo_efektif"] = final_efektif
        rk["saldo_tersedia"] = final_tersedia
        rk["last_transaction_at"] = iso(last_berhasil_at)   # None = belum pernah transaksi BERHASIL
        rk["tx_count"] = num_tx

    print(f"selesai hitung field turunan dari {total_tx:,} transaksi "
          f"(TIDAK ditulis ke Couchbase, lihat seeder Cassandra untuk ledger-nya)")
    return total_tx


# ----------------------------------------------------------------------------
# 9. posting_bunga — sample 5.000 rekening AKTIF x 4 periode
# ----------------------------------------------------------------------------
def backfill_posting_bunga():
    periodes = ['202607', '202608', '202609', '202610']
    for norek in random.sample(rekening_aktif_ids, min(5000, len(rekening_aktif_ids))):
        for prd in periodes:
            hari = random.randint(28, 31)
            tarif = 0.0150
            b_kotor = round(random.uniform(5000, 150000), 2)
            pajak = round(b_kotor * 0.20, 2)
            b_bersih = round(b_kotor - pajak, 2)
            tgl_post = datetime.strptime(f"{prd}25 23:59:59", "%Y%m%d %H:%M:%S")

            docs_posting[f"posting::{norek}::{prd}"] = {
                "_type": "posting_bunga", "no_rekening": norek, "periode": prd,
                "hari_berjalan": hari, "tarif_bunga": tarif, "bunga_kotor": b_kotor,
                "pajak_pph_final": pajak, "bunga_bersih": b_bersih,
                "tanggal_posting": iso(tgl_post),
            }
    print(f"selesai generate posting_bunga ({len(docs_posting)} dokumen)")


# ----------------------------------------------------------------------------
# 10. instruksi_terjadwal (1.500)
# ----------------------------------------------------------------------------
def backfill_instruksi():
    for i in range(1, 1501):
        id_inst = f"INS{i:012d}"
        jenis = random.choice(['AFT', 'AUTODEBET', 'AGF', 'ACCOUNT_SWEEP'])
        sumber = random.choice(rekening_aktif_ids)

        tujuan = None
        if jenis in ['AFT', 'ACCOUNT_SWEEP']:
            while True:
                tujuan = random.choice(rekening_aktif_ids)
                if tujuan != sumber:
                    break

        frekuensi = random.choice(['HARIAN', 'MINGGUAN', 'BULANAN'])
        if frekuensi == 'HARIAN':
            hari_eksekusi = None
        elif frekuensi == 'MINGGUAN':
            hari_eksekusi = random.randint(1, 7)
        else:
            hari_eksekusi = random.randint(1, 28)

        nominal = (round(random.uniform(100000, 2000000), 2) if jenis == 'AFT'
                   else round(random.uniform(50000, 1000000), 2))
        status = random.choices(['AKTIF', 'DITANGGUHKAN', 'BERAKHIR'], weights=[85, 10, 5])[0]

        tgl_mulai = generate.date_between(start_date='-1y', end_date='today')
        tgl_selesai = tgl_mulai + timedelta(days=365) if status == 'BERAKHIR' else None

        docs_instruksi[f"instruksi::{id_inst}"] = {
            "_type": "instruksi_terjadwal", "id_instruksi": id_inst, "jenis_instruksi": jenis,
            "no_rekening_sumber": sumber, "no_rekening_tujuan": tujuan, "frekuensi": frekuensi,
            "hari_eksekusi": hari_eksekusi, "nominal_target": nominal, "status_instruksi": status,
            "tanggal_mulai": iso(tgl_mulai), "tanggal_selesai": iso(tgl_selesai),
        }
    print("selesai generate instruksi_terjadwal")


# ----------------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------------
def already_seeded(cluster) -> bool:
    q = f"SELECT RAW COUNT(*) FROM `{CB_BUCKET}`.`{CB_SCOPE}`.`nasabah`"
    rows = list(cluster.query(q))
    return bool(rows) and rows[0] > 0


if __name__ == "__main__":
    cluster = Cluster(CB_CONN_STR, ClusterOptions(PasswordAuthenticator(CB_USERNAME, CB_PASSWORD)))
    bucket = cluster.bucket(CB_BUCKET)
    scope = bucket.scope(CB_SCOPE)

    if already_seeded(cluster):
        raise SystemExit("sudah pernah backfill (collection nasabah tidak kosong)")

    print("Memulai proses seeding data ke Couchbase...")
    backfill_produk()
    backfill_unit_kerja()
    backfill_kanal()
    backfill_nasabah()
    backfill_rekening()
    backfill_pemegang_rekening()
    backfill_kartu_debit()
    total_tx = backfill_transaksi_derived()
    backfill_posting_bunga()
    backfill_instruksi()

    upsert_all(scope.collection('produk'), docs_produk, "produk")
    upsert_all(scope.collection('unit_kerja'), docs_unit_kerja, "unit_kerja")
    upsert_all(scope.collection('kanal'), docs_kanal, "kanal")
    upsert_all(scope.collection('nasabah'), docs_nasabah, "nasabah")
    upsert_all(scope.collection('rekening'), docs_rekening, "rekening")
    upsert_all(scope.collection('kartu_debit'), docs_kartu, "kartu_debit")
    upsert_all(scope.collection('posting_bunga'), docs_posting, "posting_bunga")
    upsert_all(scope.collection('instruksi_terjadwal'), docs_instruksi, "instruksi_terjadwal")

    print(f"Seeding {len(docs_nasabah):,} nasabah, {len(docs_rekening):,} rekening, "
          f"{len(docs_kartu):,} kartu, {total_tx:,} transaksi (dihitung, "
          f"tidak ditulis ke Couchbase), {len(docs_posting):,} posting_bunga, "
          f"{len(docs_instruksi):,} instruksi -- SELESAI!")
