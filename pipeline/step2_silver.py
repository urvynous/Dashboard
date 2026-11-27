"""
LANGKAH 2 -- SILVER: membersihkan dan memperkaya data mentah.

- Normalisasi nama provinsi lewat join ke tabel dimensi (broadcast join)
- Menghapus duplikat id_pesanan
- Membuang baris dengan harga/kategori tidak valid
- Rating di luar skala 1-5 dijadikan NULL
- Menambah kolom turunan: tanggal, jam, hari, nilai transaksi, pulau, flag promo
- Mencatat metrik kualitas data (dipakai tab "Pipeline & Kualitas Data" di dashboard)

Jalankan mandiri:  python pipeline/step2_silver.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402


def tabel_dim_provinsi(spark):
    baris = [(p.lower(), p, v[1], v[3]) for p, v in config.PROVINSI.items()]
    return spark.createDataFrame(baris, "kunci string, provinsi_baku string, pulau string, luar_jawa_bali boolean")


def jalankan(spark):
    bronze = spark.read.parquet(config.BRONZE_DIR)

    # ---------------- metrik kualitas data (satu kali lintas data) ----------------
    dim = tabel_dim_provinsi(spark)
    dq = bronze.agg(
        F.count("*").alias("baris_bronze"),
        F.sum((~F.col("provinsi").isin(*config.PROVINSI.keys())).cast("int"))
         .cast("long").alias("provinsi_tidak_baku"),
        F.sum((F.col("harga_satuan").isNull() | (F.col("harga_satuan") <= 0)).cast("int"))
         .cast("long").alias("harga_tidak_valid"),
        F.sum(F.col("kategori").isNull().cast("int")).cast("long").alias("kategori_kosong"),
        F.sum(((F.col("rating") < 1) | (F.col("rating") > 5)).cast("int")).cast("long").alias("rating_di_luar_skala"),
        F.sum(((F.col("status") != "DIBATALKAN") & F.col("hari_kirim").isNull()).cast("int"))
         .cast("long").alias("lama_kirim_hilang"),
    ).first().asDict()

    # ---------------- pembersihan ----------------
    silver = (
        bronze
        .dropDuplicates(["id_pesanan"])
        .withColumn("kunci", F.lower(F.trim(F.col("provinsi"))))
        .join(F.broadcast(dim), on="kunci", how="inner")        # normalisasi + pulau
        .drop("provinsi", "kunci").withColumnRenamed("provinsi_baku", "provinsi")
        .filter(F.col("harga_satuan").isNotNull() & (F.col("harga_satuan") > 0))
        .filter(F.col("kategori").isNotNull())
        .withColumn("rating", F.when(F.col("rating").between(1, 5), F.col("rating")))
    )

    # ---------------- pengayaan ----------------
    tgl = F.to_date("waktu_pesan")
    silver = (
        silver
        .withColumn("tanggal", tgl)
        .withColumn("jam", F.hour("waktu_pesan"))
        # 1=Senin ... 7=Minggu (format ISO, lebih intuitif untuk pembaca Indonesia)
        .withColumn("hari_minggu", ((F.dayofweek("waktu_pesan") + 5) % 7) + 1)
        .withColumn("nilai_kotor", F.col("qty") * F.col("harga_satuan"))
        .withColumn("nilai_bersih", F.round(F.col("nilai_kotor") * (1 - F.col("diskon_pct")), 0))
        .withColumn("is_promo", F.date_format(tgl, "yyyy-MM-dd").isin(*config.HARI_PROMO))
        .withColumn("kirim_lambat", F.col("hari_kirim") > config.AMBANG_KIRIM_LAMBAT)
        .select("id_pesanan", "waktu_pesan", "tanggal", "bulan", "jam", "hari_minggu",
                "pulau", "provinsi", "luar_jawa_bali", "kategori", "kanal", "metode_bayar",
                "qty", "harga_satuan", "diskon_pct", "nilai_kotor", "nilai_bersih", "ongkir",
                "hari_kirim", "kirim_lambat", "status", "rating", "is_promo")
    )

    silver.write.mode("overwrite").partitionBy("bulan").parquet(config.SILVER_DIR)
    n_silver = spark.read.parquet(config.SILVER_DIR).count()

    dq["baris_silver"] = n_silver
    dq["baris_dibuang_total"] = dq["baris_bronze"] - n_silver
    print(f"[silver] {n_silver:,} baris bersih ({dq['baris_dibuang_total']:,} baris dibuang)")

    # Simpan laporan kualitas data sebagai tabel gold kecil.
    import pandas as pd
    os.makedirs(os.path.join(config.GOLD_DIR, "csv"), exist_ok=True)
    keterangan = {
        "baris_bronze": "Total baris mentah (termasuk duplikat)",
        "provinsi_tidak_baku": "Nama provinsi tidak baku -> dinormalisasi, tidak dibuang",
        "harga_tidak_valid": "Harga kosong / <= 0 -> dibuang",
        "kategori_kosong": "Kategori kosong -> dibuang",
        "rating_di_luar_skala": "Rating di luar 1-5 -> dijadikan NULL",
        "lama_kirim_hilang": "Lama kirim kosong pada pesanan terkirim -> diabaikan saat rata-rata",
        "baris_silver": "Baris bersih di lapisan silver",
        "baris_dibuang_total": "Duplikat + harga/kategori tidak valid",
    }
    dq_df = pd.DataFrame([{"metrik": k, "nilai": int(v), "keterangan": keterangan[k]}
                          for k, v in dq.items()])
    dq_df.to_parquet(os.path.join(config.GOLD_DIR, "gold_data_quality.parquet"), index=False)
    dq_df.to_csv(os.path.join(config.GOLD_DIR, "csv", "gold_data_quality.csv"), index=False)
    return dq


if __name__ == "__main__":
    from spark_session import buat_spark
    sp = buat_spark("BD-P09-Silver")
    jalankan(sp)
    sp.stop()
