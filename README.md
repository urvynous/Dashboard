# Proyek Penerapan BD P09 — Big Data Visualization

**Dashboard Penjualan Marketplace Nasional dengan Apache Spark**

Contoh penerapan lengkap materi *Pertemuan 9: Visualisasi Dataset Skala Besar dan Integrasi Dashboard*. Proyek ini memuat tiga komponen dalam satu folder:

| Komponen | Isi | Lokasi |
|---|---|---|
| **Big Data** | 5 juta pesanan (bisa dinaikkan ke puluhan juta) dibangkitkan langsung di Spark, lengkap dengan "kotoran" data nyata | `pipeline/step1_bronze.py` |
| **Spark virtual** | Mode lokal `local[*]` **atau** klaster Spark virtual 1 master + 2 worker lewat Docker Compose | `pipeline/spark_session.py`, `docker-compose.yml`, `Dockerfile` |
| **Dashboard** | Dashboard interaktif Streamlit (5 tab, filter, KPI, drill-down) + dashboard PNG statis untuk laporan | `dashboard/app.py`, `output/figures/` |

---

## 1. Arsitektur

```
 spark.range(5 jt)        BRONZE                SILVER                  GOLD                     VISUALISASI
 ┌──────────────┐   ┌─────────────────┐   ┌──────────────────┐   ┌───────────────────────┐   ┌──────────────────────┐
 │ Pembangkit   │──▶│ Parquet mentah  │──▶│ bersih + diperkaya│──▶│ cube · rollup         │──▶│ Streamlit (interaktif)│
 │ data (Spark) │   │ partisi bulan   │   │ partisi bulan     │   │ kuantil · heatmap     │   │ PNG laporan (statis)  │
 └──────────────┘   │ + duplikat,     │   │ dedup, normalisasi│   │ deret waktu + MA7     │   │ CSV → Excel/Power BI  │
                    │ data kotor      │   │ flag kualitas data│   │ (± 7.500 baris total) │   └──────────────────────┘
                    └─────────────────┘   └──────────────────┘   └───────────────────────┘
        └──────────────────────── Apache Spark (local[*] atau spark://spark-master:7077) ──────────┘
```

Prinsip yang diterapkan: **agregasi di klaster, visualisasi di driver**. Dashboard tidak pernah membaca 5 juta baris; ia hanya membaca tabel *gold* berukuran ribuan baris, sehingga setiap klik filter terasa instan.

## 2. Struktur folder

```
BD_P09_Proyek_Dashboard_Penjualan/
├── README.md                    ← panduan ini
├── config.py                    ← semua pengaturan (jumlah baris, master Spark, folder)
├── requirements.txt
├── run_local.sh / run_local.bat ← jalankan semuanya dengan satu perintah
├── Dockerfile                   ← image klaster Spark virtual
├── docker-compose.yml           ← master + 2 worker + pipeline + dashboard
├── .streamlit/config.toml       ← tema dashboard
├── pipeline/
│   ├── spark_session.py         ← SparkSession yang sama untuk lokal & klaster
│   ├── step1_bronze.py          ← membangkitkan Big Data mentah (+ data kotor)
│   ├── step2_silver.py          ← pembersihan, normalisasi, laporan kualitas data
│   ├── step3_gold.py            ← cube, rollup, GROUPING SETS, kuantil, MA7
│   ├── step4_laporan_statis.py  ← 5 grafik PNG (teknik-teknik P9)
│   └── run_pipeline.py          ← menjalankan langkah 1–4 + uji latensi + metadata
├── dashboard/
│   └── app.py                   ← dashboard Streamlit interaktif
├── data/
│   ├── bronze/  silver/         ← dibuat saat pipeline dijalankan (tidak disertakan di zip)
│   └── gold/                    ← SUDAH DISERTAKAN (*.parquet, csv/, gold_metadata.json)
└── output/figures/              ← PNG hasil langkah 4 (sudah disertakan)
```

> Tabel *gold* hasil eksekusi 5 juta baris **sudah disertakan**, sehingga dashboard bisa langsung dibuka tanpa menjalankan Spark (Cara A).

## 3. Cara menjalankan

### Cara A — Dashboard saja (paling cepat, tanpa Spark/Java)

```bash
pip install streamlit plotly pandas pyarrow
streamlit run dashboard/app.py
```
Buka `http://localhost:8501`.

### Cara B — Spark lokal `local[*]` (laptop)

Prasyarat: Python 3.9+ dan **Java JDK 17 atau 21** (mis. Eclipse Temurin), `java -version` harus berjalan.

```bash
pip install -r requirements.txt
python pipeline/run_pipeline.py --rows 5000000     # turunkan ke 1000000 bila RAM < 8 GB
streamlit run dashboard/app.py
```
Atau cukup `./run_local.sh` (Linux/macOS/WSL) / `run_local.bat` (Windows).
Selama pipeline berjalan, buka **Spark UI** di `http://localhost:4040` untuk melihat *job*, *stage*, dan DAG.

Langkah dapat dijalankan satu per satu: `python pipeline/step1_bronze.py`, lalu `step2_silver.py`, `step3_gold.py`, `step4_laporan_statis.py`.

### Cara C — Klaster Spark virtual dengan Docker (1 master + 2 worker)

Prasyarat: Docker Desktop (Windows/macOS) atau Docker Engine + Compose (Linux), RAM bebas ± 6 GB.

```bash
# 1) Nyalakan klaster
docker compose up -d --build spark-master spark-worker-1 spark-worker-2

# 2) Cek dua worker sudah terdaftar
#    buka http://localhost:8080  (Spark Master UI)

# 3) Jalankan pipeline di klaster (driver = kontainer spark-pipeline)
docker compose run --rm --service-ports pipeline
#    selama berjalan: http://localhost:4040 (Spark UI aplikasi)

# 4) Nyalakan dashboard
docker compose up -d dashboard
#    buka http://localhost:8501

# 5) Matikan semuanya
docker compose down
```

| Alamat | Isi |
|---|---|
| `http://localhost:8080` | Spark Master: daftar worker, core, memori, aplikasi berjalan |
| `http://localhost:8081`, `:8082` | UI masing-masing worker (log executor) |
| `http://localhost:4040` | Spark UI aplikasi: job, stage, DAG, SQL plan |
| `http://localhost:8501` | Dashboard Streamlit |

Ubah ukuran data lewat variabel `N_ROWS` pada layanan `pipeline` di `docker-compose.yml` (mis. `"20000000"`). Tambah worker dengan menyalin blok `spark-worker-2`.

**Mengapa disebut klaster "virtual"?** Setiap kontainer berperan seperti satu mesin: master mengatur sumber daya, worker menjalankan *executor*, dan driver mengirim pekerjaan. Kode Python yang sama dipakai di Cara B dan Cara C; yang berbeda hanya nilai `SPARK_MASTER` (`local[*]` vs `spark://spark-master:7077`). Folder proyek di-*mount* ke semua kontainer sebagai pengganti HDFS/S3.

## 4. Data: 5 juta pesanan marketplace (sintetis)

Periode 1 Okt 2025 – 30 Sep 2026, 10 provinsi, 6 kategori, 3 kanal, 5 metode bayar. Pola yang sengaja ditanam agar "ditemukan" lewat visualisasi:

| Pola | Cara ditanam | Ditemukan di |
|---|---|---|
| Lonjakan hari promo | 11.11 & 12.12 volume ×4, tanggal kembar lain ×2 | Tab *Ringkasan & Tren* (deret harian + anotasi) |
| Musim Ramadan | volume ×1,35 (18 Feb – 19 Mar 2026) | Omzet bulanan memuncak di Maret |
| Pengiriman luar Jawa-Bali | lebih lama **dan** lebih bervariasi | Box plot kuantil, porsi kirim > 3 hari |
| Kepuasan pelanggan | rating turun ±0,35 per hari kirim | Heatmap kirim × rating |
| Risiko COD | peluang batal 11% vs 2–4% metode lain | Tab *Perilaku Pelanggan* |
| Pola jam belanja | puncak siang dan 19.00–22.00 | Heatmap jam × hari |

"Kotoran" data mentah (ditangani di silver): ±0,5% duplikat, nama provinsi tidak baku, harga kosong/negatif, kategori kosong, rating 0, lama kirim hilang. Ringkasannya tampil di tab *Pipeline & Kualitas Data*.

## 5. Tabel gold dan teknik Spark

| Tabel gold | Teknik Spark | Dipakai untuk |
|---|---|---|
| `gold_cube` | `cube("bulan","provinsi","kategori")` + `grouping_id()` | KPI, tren bulanan, peringkat — semua filter |
| `gold_rollup_wilayah` | `rollup("pulau","provinsi","kategori")` | Treemap drill-down wilayah |
| `gold_harian` | `rollup` + `Window.rowsBetween(-6,0)` | Deret waktu harian + rata-rata bergerak 7 hari |
| `gold_heatmap_kirim_rating` | `GROUPING SETS` + binning 0,5 hari | Heatmap pengganti scatter jutaan titik |
| `gold_kuantil_kirim` / `_nilai` | `percentile_approx(..., array(0.05,...,0.95))` | Box plot terdistribusi (`plt.bxp`, `go.Box`) |
| `gold_jam_hari` | `GROUPING SETS` | Heatmap jam × hari |
| `gold_metode_bayar` | `groupBy().pivot("status")` | Tingkat pembatalan per metode bayar |
| `gold_data_quality` | agregasi kondisi di silver | Laporan kualitas data |
| `gold_metadata.json` | — | Waktu pembaruan, ukuran tiap lapisan, durasi, latensi |

**Membaca `grouping_id` (kolom `gid`) pada `gold_cube`:** bit 4 = bulan diagregasi, bit 2 = provinsi diagregasi, bit 1 = kategori diagregasi.

| gid | Arti baris | Contoh pemakaian |
|---|---|---|
| 0 | bulan × provinsi × kategori | detail terkecil |
| 3 | per bulan (nasional, semua kategori) | tren bulanan nasional |
| 5 | per provinsi (seluruh periode) | peringkat provinsi |
| 6 | per kategori (seluruh periode) | omzet per kategori |
| 7 | total nasional | kartu KPI |

`gid` lebih aman daripada `isNull()` karena NULL "subtotal" tidak tertukar dengan NULL "data kosong".

**Metrik aditif.** Gold menyimpan `sum_kirim` dan `n_kirim`, bukan `rata2_kirim`. Dashboard menjumlahkan bulan yang dipilih, *baru kemudian* membagi. Rata-rata dari rata-rata bulanan akan salah bila jumlah pesanan tiap bulan berbeda.

## 6. Isi dashboard

| Tab | Isi | Prinsip P9 |
|---|---|---|
| KPI (atas) | Omzet, pesanan, AOV, lama kirim, rating, tingkat batal + Δ vs periode sebelumnya | KPI dengan pembanding |
| Ringkasan & Tren | Omzet bulanan (puncak diberi warna aksen), kategori terurut, deret harian + MA7 + anotasi promo/Ramadan | Judul = kesimpulan, warna aksen, anotasi |
| Wilayah | Treemap pulau → provinsi → kategori, peringkat provinsi, tabel detail | *Overview first, zoom & filter, details on demand* |
| Logistik & Kepuasan | Box plot kuantil, heatmap kirim × rating, rating vs lama kirim, porsi terlambat | Distribusi, bukan hanya rata-rata; binning |
| Perilaku Pelanggan | Heatmap jam × hari, pembatalan per metode bayar, sebaran nilai transaksi | Heatmap, batang horizontal terurut |
| Pipeline & Kualitas Data | Diagram arsitektur, jumlah baris & ukuran tiap lapisan, latensi gold vs silver, unduh CSV | Transparansi sumber & waktu pembaruan |

Filter di sidebar: **provinsi**, **kategori**, **rentang bulan**.

## 7. Grafik statis untuk laporan (`output/figures/`)

| Berkas | Teknik |
|---|---|
| `01_overplotting.png` | Scatter sampel 1% vs heatmap seluruh data (skala log) |
| `02_deret_waktu.png` | Omzet harian + MA7 + anotasi 11.11/12.12 + pita Ramadan |
| `03_boxplot_kirim.png` | Box plot dari kuantil aproksimasi (`ax.bxp`) |
| `04_sebelum_sesudah.png` | Pie 6 irisan → batang horizontal terurut dengan warna aksen |
| `05_dashboard_satu_halaman.png` | Dashboard satu halaman (KPI → tren → perbandingan → penjelasan) |

## 8. Ide pengembangan (latihan lanjutan)

1. Tambah dimensi `kanal` ke `cube` dan filter kanal di dashboard. Bandingkan jumlah baris gold sebelum dan sesudahnya (ledakan kombinasi pada `cube`).
2. Ganti `cube` dengan `GROUPING SETS` yang hanya memuat kombinasi yang benar-benar dipakai dashboard. Berapa baris yang dihemat?
3. Naikkan `N_ROWS` ke 20 juta di klaster Docker. Catat durasi tiap langkah di tab *Pipeline* dan amati Spark UI (jumlah *task*, *shuffle*).
4. Jadwalkan `run_pipeline.py --skip-bronze` setiap malam (cron) agar tabel gold selalu segar; tunjukkan waktu pembaruan di dashboard.
5. Impor `data/gold/csv/gold_cube.csv` ke Power BI/Tableau dan buat ulang halaman *Ringkasan*.

## 9. Kendala umum

| Gejala | Penyebab & solusi |
|---|---|
| `JAVA_HOME is not set` / `java` tidak dikenal | Pasang JDK 17/21, atur `JAVA_HOME`, buka ulang terminal |
| Windows: `winutils.exe` / `HADOOP_HOME unset` saat menulis Parquet | Pakai Cara C (Docker) atau WSL; dashboard tetap bisa dibuka lewat Cara A |
| `OutOfMemoryError` | Turunkan `--rows`, atau naikkan `SPARK_DRIVER_MEMORY=4g` |
| Arrow: `DirectByteBuffer not available` | Sudah ditangani: Arrow dimatikan di `spark_session.py` (tabel gold kecil, tidak perlu Arrow) |
| Worker tidak muncul di `:8080` | `docker compose logs spark-worker-1`; pastikan Docker Desktop diberi RAM ≥ 6 GB |
| Port 8080/4040/8501 terpakai | Ubah pemetaan port di `docker-compose.yml` (mis. `"18080:8080"`) |
| Tag image `spark:3.5.7-...` tidak ditemukan | Ganti `SPARK_TAG` di `Dockerfile` dengan tag 3.5.x lain dari Docker Hub |

---
*Data sepenuhnya sintetis untuk keperluan praktikum. Referensi: Shneiderman (1996); Knaflic (2015) Storytelling with Data; Liu, Jiang & Heer (2013) imMens; dokumentasi Apache Spark, Streamlit, dan Plotly.*
