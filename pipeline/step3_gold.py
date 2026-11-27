"""
LANGKAH 3 -- GOLD: pra-agregasi untuk dashboard.

Prinsip utama: "agregasi di klaster, visualisasi di driver".
Dashboard TIDAK pernah membaca jutaan baris silver; ia hanya membaca tabel-tabel
gold berikut yang ukurannya ribuan baris.

| Tabel gold                 | Teknik Spark                         | Dipakai untuk                         |
|----------------------------|--------------------------------------|---------------------------------------|
| gold_cube                  | cube(bulan, provinsi, kategori)      | KPI, tren bulanan, filter apa pun     |
| gold_rollup_wilayah        | rollup(pulau, provinsi, kategori)    | Drill-down wilayah (treemap)          |
| gold_harian                | rollup + window rata-rata bergerak   | Deret waktu harian + MA7              |
| gold_heatmap_kirim_rating  | GROUPING SETS + binning 2D           | Heatmap pengganti scatter jutaan titik|
| gold_kuantil_kirim         | percentile_approx                    | Box plot terdistribusi per provinsi   |
| gold_kuantil_nilai         | percentile_approx                    | Box plot nilai transaksi per kategori |
| gold_jam_hari              | GROUPING SETS                        | Heatmap jam x hari                    |
| gold_metode_bayar          | groupBy + pivot status               | Tingkat batal per metode bayar        |

CATATAN DESAIN: metrik disimpan dalam bentuk ADITIF (jumlah dan pembilang/
penyebut), misalnya sum_kirim dan n_kirim, BUKAN rata-rata. Dengan begitu
dashboard bisa menjumlahkan beberapa bulan lalu menghitung rata-rata yang
benar (rata-rata dari rata-rata itu SALAH bila ukuran kelompok berbeda).

Jalankan mandiri:  python pipeline/step3_gold.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.window import Window  # noqa: E402

KUANTIL = [0.05, 0.25, 0.5, 0.75, 0.95]


def metrik_aditif():
    selesai = F.col("status") == "SELESAI"
    return [
        F.count("*").alias("pesanan"),
        F.sum(selesai.cast("int")).alias("pesanan_selesai"),
        F.sum((F.col("status") == "DIBATALKAN").cast("int")).alias("pesanan_batal"),
        F.sum((F.col("status") == "DIKEMBALIKAN").cast("int")).alias("pesanan_retur"),
        F.sum(F.when(selesai, F.col("nilai_bersih")).otherwise(0)).alias("omzet"),
        F.sum(F.when(selesai, F.col("qty")).otherwise(0)).alias("item_terjual"),
        F.sum(F.when(selesai, F.col("nilai_kotor") - F.col("nilai_bersih")).otherwise(0)).alias("total_diskon"),
        F.sum("ongkir").alias("total_ongkir"),
        F.sum("hari_kirim").alias("sum_kirim"),
        F.count("hari_kirim").alias("n_kirim"),
        F.sum(F.col("kirim_lambat").cast("int")).alias("n_kirim_lambat"),
        F.sum("rating").alias("sum_rating"),
        F.count("rating").alias("n_rating"),
        F.sum((F.col("rating") <= 2).cast("int")).alias("n_rating_rendah"),
    ]


def simpan(df_spark, nama):
    """Tabel gold kecil -> aman dibawa ke driver -> Parquet (dashboard) + CSV (Excel/Power BI)."""
    pdf = df_spark.toPandas()
    pdf.to_parquet(os.path.join(config.GOLD_DIR, f"{nama}.parquet"), index=False)
    pdf.to_csv(os.path.join(config.GOLD_DIR, "csv", f"{nama}.csv"), index=False)
    print(f"[gold] {nama:<28} {len(pdf):>7,} baris")
    return len(pdf)


def _kuantil_ke_kolom(df, kolom_q="q"):
    for i, nama in enumerate(["p05", "q1", "median", "q3", "p95"]):
        df = df.withColumn(nama, F.col(kolom_q)[i])
    return df.drop(kolom_q)


def jalankan(spark):
    os.makedirs(os.path.join(config.GOLD_DIR, "csv"), exist_ok=True)
    silver = spark.read.parquet(config.SILVER_DIR).cache()
    silver.createOrReplaceTempView("silver")
    ringkas = {}

    # 1) CUBE: semua kombinasi bulan x provinsi x kategori + subtotal + total.
    #    grouping_id membedakan NULL "subtotal" dari NULL "data kosong".
    gold_cube = (
        silver.cube("bulan", "provinsi", "kategori")
        .agg(*metrik_aditif(), F.grouping_id().alias("gid"))
    )
    ringkas["gold_cube"] = simpan(gold_cube, "gold_cube")

    # 2) ROLLUP: subtotal hierarkis pulau -> provinsi -> kategori.
    gold_rollup = (
        silver.rollup("pulau", "provinsi", "kategori")
        .agg(*metrik_aditif(), F.grouping_id().alias("gid"))
    )
    ringkas["gold_rollup_wilayah"] = simpan(gold_rollup, "gold_rollup_wilayah")

    # 3) DERET WAKTU HARIAN per provinsi + NASIONAL, dengan rata-rata bergerak 7 hari.
    harian = (
        silver.rollup("tanggal", "provinsi")
        .agg(F.count("*").alias("pesanan"),
             F.sum(F.when(F.col("status") == "SELESAI", F.col("nilai_bersih")).otherwise(0)).alias("omzet"),
             F.sum("hari_kirim").alias("sum_kirim"), F.count("hari_kirim").alias("n_kirim"))
        .filter(F.col("tanggal").isNotNull())
        .withColumn("provinsi", F.coalesce("provinsi", F.lit("NASIONAL")))
    )
    w7 = Window.partitionBy("provinsi").orderBy("tanggal").rowsBetween(-6, 0)
    harian = (
        harian.withColumn("omzet_ma7", F.avg("omzet").over(w7))
        .withColumn("pesanan_ma7", F.avg("pesanan").over(w7))
        .withColumn("is_promo", F.date_format("tanggal", "yyyy-MM-dd").isin(*config.HARI_PROMO))
        .orderBy("provinsi", "tanggal")
    )
    ringkas["gold_harian"] = simpan(harian, "gold_harian")

    # 4) HEATMAP lama kirim (bin 0,5 hari) x rating -- GROUPING SETS:
    #    sekaligus per provinsi dan nasional dalam satu kali lintas data.
    heat = spark.sql("""
        SELECT COALESCE(provinsi, 'NASIONAL') AS provinsi,
               FLOOR(hari_kirim * 2) / 2      AS bin_kirim,
               rating,
               COUNT(*)                       AS jumlah
        FROM silver
        WHERE rating IS NOT NULL AND hari_kirim IS NOT NULL
        GROUP BY GROUPING SETS ((provinsi, FLOOR(hari_kirim * 2) / 2, rating),
                                (FLOOR(hari_kirim * 2) / 2, rating))
    """)
    ringkas["gold_heatmap_kirim_rating"] = simpan(heat, "gold_heatmap_kirim_rating")

    # 5) KUANTIL lama kirim per provinsi (+ nasional) -> box plot tanpa data mentah.
    q_expr = F.expr(f"percentile_approx(hari_kirim, array({','.join(map(str, KUANTIL))}), 1000)")
    q_prov = silver.groupBy("provinsi").agg(q_expr.alias("q"), F.avg("hari_kirim").alias("rata2"),
                                            F.count("hari_kirim").alias("n"))
    q_nas = silver.agg(q_expr.alias("q"), F.avg("hari_kirim").alias("rata2"),
                       F.count("hari_kirim").alias("n")).withColumn("provinsi", F.lit("NASIONAL"))
    q_kirim = _kuantil_ke_kolom(q_prov.unionByName(q_nas))
    ringkas["gold_kuantil_kirim"] = simpan(q_kirim, "gold_kuantil_kirim")

    # 6) KUANTIL nilai transaksi per kategori.
    q_nilai = _kuantil_ke_kolom(
        silver.filter(F.col("status") == "SELESAI").groupBy("kategori")
        .agg(F.expr(f"percentile_approx(nilai_bersih, array({','.join(map(str, KUANTIL))}), 1000)").alias("q"),
             F.avg("nilai_bersih").alias("rata2"), F.count("*").alias("n")))
    ringkas["gold_kuantil_nilai"] = simpan(q_nilai, "gold_kuantil_nilai")

    # 7) POLA JAM x HARI (per provinsi + nasional).
    jam_hari = spark.sql("""
        SELECT COALESCE(provinsi, 'NASIONAL') AS provinsi, hari_minggu, jam,
               COUNT(*) AS pesanan,
               SUM(CASE WHEN status = 'SELESAI' THEN nilai_bersih ELSE 0 END) AS omzet
        FROM silver
        GROUP BY GROUPING SETS ((provinsi, hari_minggu, jam), (hari_minggu, jam))
    """)
    ringkas["gold_jam_hari"] = simpan(jam_hari, "gold_jam_hari")

    # 8) METODE BAYAR x KANAL dengan pivot status.
    bayar = (
        silver.groupBy("metode_bayar", "kanal")
        .pivot("status", ["SELESAI", "DIBATALKAN", "DIKEMBALIKAN"]).count()
        .fillna(0)
    )
    ringkas["gold_metode_bayar"] = simpan(bayar, "gold_metode_bayar")

    silver.unpersist()
    return ringkas


if __name__ == "__main__":
    from spark_session import buat_spark
    sp = buat_spark("BD-P09-Gold")
    jalankan(sp)
    sp.stop()
