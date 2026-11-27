"""
Menjalankan seluruh pipeline: BRONZE -> SILVER -> GOLD -> LAPORAN STATIS.

Contoh:
  python pipeline/run_pipeline.py                     # 5 juta baris, Spark lokal
  python pipeline/run_pipeline.py --rows 1000000      # versi ringan
  python pipeline/run_pipeline.py --skip-bronze       # pakai data mentah yang sudah ada

Di klaster Spark virtual (lihat docker-compose.yml):
  docker compose run --rm pipeline
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: E402
import step1_bronze  # noqa: E402
import step2_silver  # noqa: E402
import step3_gold  # noqa: E402
import step4_laporan_statis  # noqa: E402
from spark_session import buat_spark, info_spark  # noqa: E402


def ukuran_folder_mb(jalur):
    total = 0
    for akar, _, berkas in os.walk(jalur):
        total += sum(os.path.getsize(os.path.join(akar, b)) for b in berkas)
    return round(total / 1024 ** 2, 2)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=config.N_ROWS, help="jumlah pesanan dibangkitkan")
    ap.add_argument("--skip-bronze", action="store_true", help="lewati pembangkitan data mentah")
    ap.add_argument("--skip-laporan", action="store_true", help="lewati pembuatan PNG")
    args = ap.parse_args()

    spark = buat_spark("BD-P09-Pipeline")
    info = info_spark(spark)
    print("=" * 70)
    print(f"Spark {info['spark_versi']} | master: {info['master']} | core: {info['core_paralel']}")
    print(f"Spark UI: {info['spark_ui']}")
    print("=" * 70)

    durasi = {}
    t = time.perf_counter()
    if not args.skip_bronze:
        step1_bronze.jalankan(spark, args.rows)
    durasi["bronze"] = round(time.perf_counter() - t, 1)

    t = time.perf_counter(); dq = step2_silver.jalankan(spark)
    durasi["silver"] = round(time.perf_counter() - t, 1)

    t = time.perf_counter(); ringkas_gold = step3_gold.jalankan(spark)
    durasi["gold"] = round(time.perf_counter() - t, 1)

    # Pembanding latensi: jawaban dashboard dari GOLD vs dihitung ulang dari SILVER.
    import pandas as pd
    from pyspark.sql import functions as F
    t = time.perf_counter()
    g = pd.read_parquet(os.path.join(config.GOLD_DIR, "gold_cube.parquet"))
    _ = g[g.gid == 5][["provinsi", "omzet", "sum_kirim", "n_kirim"]]
    t_gold = time.perf_counter() - t
    t = time.perf_counter()
    _ = (spark.read.parquet(config.SILVER_DIR).groupBy("provinsi")
         .agg(F.sum("nilai_bersih"), F.avg("hari_kirim")).collect())
    t_silver = time.perf_counter() - t
    print(f"[uji latensi] jawaban 'omzet per provinsi': gold {t_gold:.3f} dtk vs silver {t_silver:.2f} dtk")

    if not args.skip_laporan:
        t = time.perf_counter(); step4_laporan_statis.jalankan(spark)
        durasi["laporan"] = round(time.perf_counter() - t, 1)

    meta = {
        "diperbarui_pada": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "spark": info,
        "baris_bronze": dq["baris_bronze"],
        "baris_silver": dq["baris_silver"],
        "baris_gold": ringkas_gold,
        "ukuran_mb": {"bronze": ukuran_folder_mb(config.BRONZE_DIR),
                      "silver": ukuran_folder_mb(config.SILVER_DIR),
                      "gold": ukuran_folder_mb(config.GOLD_DIR)},
        "durasi_detik": durasi,
        "latensi_detik": {"gold": round(t_gold, 4), "silver": round(t_silver, 3)},
        "periode": f"{config.TANGGAL_MULAI} s.d. +{config.JUMLAH_HARI} hari",
    }
    with open(os.path.join(config.GOLD_DIR, "gold_metadata.json"), "w") as f:
        json.dump(meta, f, indent=2, default=str)
    print("=" * 70)
    print(json.dumps(meta, indent=2, default=str))
    spark.stop()


if __name__ == "__main__":
    main()
