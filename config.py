"""
Konfigurasi terpusat proyek BD P09 -- Big Data Visualization.

Semua nilai dapat diganti lewat variabel lingkungan (environment variable),
sehingga skrip yang sama bisa berjalan di laptop (local[*]) maupun di
klaster Spark virtual (Docker Compose) tanpa mengubah kode.
"""
import os

# Folder akar proyek (folder tempat file ini berada).
ROOT = os.path.dirname(os.path.abspath(__file__))

# ----------------------------------------------------------------------------
# Spark
# ----------------------------------------------------------------------------
# local[*]                      -> Spark lokal memakai semua core laptop
# spark://spark-master:7077     -> klaster Spark virtual (docker-compose)
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")
SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "2g")
SPARK_SHUFFLE_PARTITIONS = int(os.environ.get("SPARK_SHUFFLE_PARTITIONS", "16"))

# ----------------------------------------------------------------------------
# Ukuran data
# ----------------------------------------------------------------------------
# Jumlah pesanan yang dibangkitkan (default 5 juta). Turunkan ke 1_000_000
# bila laptop berRAM kecil, naikkan ke 20_000_000+ di klaster.
N_ROWS = int(os.environ.get("N_ROWS", "5000000"))
SEED = int(os.environ.get("SEED", "2026"))

# Periode data: 1 Oktober 2025 s.d. 30 September 2026 (365 hari).
TANGGAL_MULAI = "2025-10-01"
JUMLAH_HARI = 365

# ----------------------------------------------------------------------------
# Lapisan data (arsitektur medallion: bronze -> silver -> gold)
# ----------------------------------------------------------------------------
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(ROOT, "data"))
BRONZE_DIR = os.path.join(DATA_DIR, "bronze", "pesanan_mentah")
SILVER_DIR = os.path.join(DATA_DIR, "silver", "pesanan_bersih")
GOLD_DIR = os.path.join(DATA_DIR, "gold")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", os.path.join(ROOT, "output"))
FIG_DIR = os.path.join(OUTPUT_DIR, "figures")

# ----------------------------------------------------------------------------
# Dimensi bisnis
# ----------------------------------------------------------------------------
PROVINSI = {
    # provinsi: (bobot porsi pesanan, pulau, tambahan ongkir Rp, luar Jawa-Bali?)
    "DKI Jakarta":      (0.22, "Jawa",       0,     False),
    "Jawa Barat":       (0.20, "Jawa",       0,     False),
    "Jawa Timur":       (0.15, "Jawa",       0,     False),
    "Jawa Tengah":      (0.12, "Jawa",       0,     False),
    "Banten":           (0.08, "Jawa",       0,     False),
    "Sumatera Utara":   (0.06, "Sumatera",   9000,  True),
    "Sulawesi Selatan": (0.05, "Sulawesi",   15000, True),
    "Bali":             (0.04, "Bali-Nusa",  4000,  False),
    "Kalimantan Timur": (0.04, "Kalimantan", 15000, True),
    "DI Yogyakarta":    (0.04, "Jawa",       0,     False),
}

KATEGORI = {
    # kategori: (bobot, mu log-harga, sigma log-harga)
    "Fashion":      (0.27, 11.6, 0.70),
    "Kecantikan":   (0.20, 11.3, 0.60),
    "Rumah Tangga": (0.17, 11.8, 0.80),
    "Makanan":      (0.15, 10.9, 0.50),
    "Elektronik":   (0.13, 13.1, 0.90),
    "Hobi":         (0.08, 12.0, 0.85),
}

KANAL = {"Aplikasi": 0.68, "Web": 0.20, "Live Shopping": 0.12}

METODE_BAYAR = {
    # metode: (bobot, peluang batal)
    "E-Wallet":     (0.38, 0.025),
    "Transfer VA":  (0.22, 0.040),
    "COD":          (0.20, 0.110),
    "Paylater":     (0.12, 0.035),
    "Kartu Kredit": (0.08, 0.020),
}

# Hari promo nasional (volume x4 dan diskon) serta periode Ramadan 2026.
HARI_PROMO = ["2025-10-10", "2025-11-11", "2025-12-12", "2026-01-01",
              "2026-02-02", "2026-03-03", "2026-04-04", "2026-05-05",
              "2026-06-06", "2026-07-07", "2026-08-08", "2026-09-09"]
HARI_PROMO_BESAR = ["2025-11-11", "2025-12-12"]  # x4; promo tanggal kembar lain x2
RAMADAN = ("2026-02-18", "2026-03-19")            # volume x1,35

# Ambang bisnis untuk penanda warna di dashboard.
AMBANG_KIRIM_LAMBAT = 3.0  # hari
