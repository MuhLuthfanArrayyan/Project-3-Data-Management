-- Project Part-3 | Cassandra | Sistem Tabungan BRI
-- ============================================================================
-- Sesuai PLAN.md §3 & §6: Cassandra HANYA menyimpan LEDGER transaksi.
--
--   transaksi_by_rekening       -> R-01  (riwayat transaksi per rekening)
--   transaksi_by_kanal_periode  -> R-03  (agregasi jumlah/nominal per kanal/bulan)
--
-- Entitas state (nasabah, rekening, kartu_debit, posting_bunga, instruksi,
-- produk, unit_kerja, kanal) TIDAK ada di sini -- semuanya di Couchbase
-- (bucket bri_tabungan, scope operasional). Semua WRITE W-01..W-10 juga
-- ditangani Couchbase, Cassandra hanya menerima append event dari W-07.
-- ============================================================================

CREATE KEYSPACE IF NOT EXISTS bri_tabungan
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE bri_tabungan;

-- Transaksi: partisi per rekening, terbaru dulu (melayani R-01)
CREATE TABLE IF NOT EXISTS transaksi_by_rekening (
  no_rekening text, waktu_transaksi timestamp, id_transaksi text,
  jenis_transaksi text, arah text, nominal decimal, mata_uang text,
  id_kanal text, nama_kanal text, no_kartu text, status_transaksi text, saldo_setelah decimal,
  PRIMARY KEY ((no_rekening), waktu_transaksi, id_transaksi)
) WITH CLUSTERING ORDER BY (waktu_transaksi DESC, id_transaksi ASC);

-- Agregat kanal/bulan untuk R-03.
-- status_transaksi & jenis_transaksi jadi clustering key supaya filter
-- BERHASIL dan GROUP BY bisa dilakukan langsung di CQL.
CREATE TABLE IF NOT EXISTS transaksi_by_kanal_periode (
  id_kanal          text,
  periode           text,          -- 'YYYYMM' (UTC, sama dengan DATE_TRUNC di Part-2)
  status_transaksi  text,
  jenis_transaksi   text,
  waktu_transaksi   timestamp,
  id_transaksi      text,
  nama_kanal        text,          -- denormalisasi dari kanal_layanan
  nominal           decimal,
  PRIMARY KEY ((id_kanal, periode), status_transaksi, jenis_transaksi, waktu_transaksi, id_transaksi)
);
