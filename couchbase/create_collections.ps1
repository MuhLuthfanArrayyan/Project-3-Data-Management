# Buat 8 collection lewat couchbase-cli.
# Diperlukan karena Couchbase Community 7.6.x menolak CREATE COLLECTION lewat
# N1QL dengan error "maxTTL:Supported in enterprise edition only".
# Jalankan dari folder part-3 (atau dari mana saja, container yang dipakai).

$ErrorActionPreference = 'Continue'
$collections = @(
    'produk',
    'unit_kerja',
    'kanal',
    'nasabah',
    'rekening',
    'kartu_debit',
    'posting_bunga',
    'instruksi_terjadwal'
)

foreach ($c in $collections) {
    Write-Host " creating operasional.$c"
    docker exec bri-couchbase couchbase-cli collection-manage `
        -c 127.0.0.1:8091 -u Administrator -p password `
        --bucket bri_tabungan --create-collection "operasional.$c"
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "gagal membuat collection: $c (mungkin sudah ada)"
    }
}

Write-Host "`nDaftar collection sekarang:"
docker exec bri-couchbase couchbase-cli collection-manage `
    -c 127.0.0.1:8091 -u Administrator -p password `
    --bucket bri_tabungan --list-collections operasional
