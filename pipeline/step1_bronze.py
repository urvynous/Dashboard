"""
LANGKAH 1 -- BRONZE: membangkitkan Big Data pesanan marketplace mentah.

Data dibangkitkan LANGSUNG di Spark (terdistribusi), bukan di pandas, sehingga
ukuran data hanya dibatasi oleh klaster, bukan oleh memori driver.
Data mentah sengaja diberi "kotoran" khas data nyata (duplikat, format nama
provinsi tidak konsisten, harga tidak valid, rating di luar skala) agar
langkah silver punya pekerjaan pembersihan yang realistis.

Jalankan mandiri:  python pipeline/step1_bronze.py
"""
import os
import sys
from datetime import date, timedelta

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.ml.feature import Bucketizer  # noqa: E402


def _pilih_berbobot(kolom_u, pilihan_bobot):
    """Memilih label dari kolom bilangan acak u ~ U(0,1) berdasarkan bobot kumulatif.

    PENTING: u harus berupa KOLOM yang sudah disimpan (withColumn), bukan F.rand()
    langsung. Bila F.rand() ditulis di dalam when berantai, setiap cabang dapat
    mengevaluasi bilangan acak baru sehingga cabang terakhir jarang terpilih.
    """
    label = list(pilihan_bobot.keys())
    bobot = np.array(list(pilihan_bobot.values()), dtype=float)
    kum = np.cumsum(bobot / bobot.sum())
    ekspresi = F.when(kolom_u < float(kum[0]), label[0])
    for lab, k in zip(label[1:-1], kum[1:-1]):
        ekspresi = ekspresi.when(kolom_u < float(k), lab)
    return ekspresi.otherwise(label[-1])


def _bobot_harian():
    """Bobot volume pesanan untuk tiap hari dalam periode (365 nilai)."""
    mulai = date.fromisoformat(config.TANGGAL_MULAI)
    ram_a, ram_b = (date.fromisoformat(d) for d in config.RAMADAN)
    bobot = []
    for i in range(config.JUMLAH_HARI):
        d = mulai + timedelta(days=i)
        w = 1.0 + 0.25 * i / config.JUMLAH_HARI          # tren tumbuh 25% setahun
        if d.weekday() >= 5:
            w *= 1.12                                      # akhir pekan
        if ram_a <= d <= ram_b:
            w *= 1.35                                      # Ramadan
        if d.isoformat() in config.HARI_PROMO_BESAR:
            w *= 4.0                                       # 11.11 dan 12.12
        elif d.isoformat() in config.HARI_PROMO:
            w *= 2.0                                       # promo tanggal kembar
        bobot.append(w)
    return np.array(bobot)


# Pola jam: sepi dini hari, puncak makan siang dan 19.00-22.00.
BOBOT_JAM = np.array([0.6, 0.35, 0.2, 0.15, 0.2, 0.4, 0.8, 1.2, 1.5, 1.6, 1.7, 1.9,
                      2.3, 2.0, 1.7, 1.6, 1.6, 1.7, 1.9, 2.4, 2.8, 2.9, 2.2, 1.2])


def _splits(bobot):
    kum = np.cumsum(bobot / bobot.sum())
    kum[-1] = 1.0 + 1e-9
    return [0.0] + [float(x) for x in kum]


def bangkitkan_bronze(spark, n_rows=config.N_ROWS, seed=config.SEED):
    n_partisi = max(8, n_rows // 400_000)
    s = seed

    df = (
        spark.range(n_rows, numPartitions=n_partisi)
        .withColumn("u_hari", F.rand(s + 1))
        .withColumn("u_jam", F.rand(s + 2))
    )

    # Bucketizer = pencarian biner pada batas kumulatif -> memilih hari & jam
    # sesuai bobot tanpa loop di Python (berjalan terdistribusi di executor).
    df = Bucketizer(splits=_splits(_bobot_harian()), inputCol="u_hari",
                    outputCol="hari_ke").transform(df)
    df = Bucketizer(splits=_splits(BOBOT_JAM), inputCol="u_jam",
                    outputCol="jam").transform(df)

    prov_bobot = {k: v[0] for k, v in config.PROVINSI.items()}
    kat_bobot = {k: v[0] for k, v in config.KATEGORI.items()}
    bayar_bobot = {k: v[0] for k, v in config.METODE_BAYAR.items()}

    df = (
        df
        .withColumn("id_pesanan", F.format_string("ORD-%09d", F.col("id")))
        .withColumn("waktu_pesan", (
            F.unix_timestamp(F.date_add(F.lit(config.TANGGAL_MULAI).cast("date"),
                                        F.col("hari_ke").cast("int")).cast("timestamp"))
            + F.col("jam") * 3600 + F.floor(F.rand(s + 3) * 3600)).cast("timestamp"))
        .withColumn("_u1", F.rand(s + 4)).withColumn("_u2", F.rand(s + 5))
        .withColumn("_u3", F.rand(s + 6)).withColumn("_u4", F.rand(s + 7))
        .withColumn("provinsi", _pilih_berbobot(F.col("_u1"), prov_bobot))
        .withColumn("kategori", _pilih_berbobot(F.col("_u2"), kat_bobot))
        .withColumn("kanal", _pilih_berbobot(F.col("_u3"), config.KANAL))
        .withColumn("metode_bayar", _pilih_berbobot(F.col("_u4"), bayar_bobot))
        .withColumn("qty", (F.floor(F.pow(F.rand(s + 8), 2.5) * 4) + 1).cast("int"))
    )

    # Harga satuan lognormal per kategori.
    mu = F.lit(11.5); sigma = F.lit(0.7)
    for kat, (_, m, sd) in config.KATEGORI.items():
        mu = F.when(F.col("kategori") == kat, F.lit(m)).otherwise(mu)
        sigma = F.when(F.col("kategori") == kat, F.lit(sd)).otherwise(sigma)
    df = df.withColumn("harga_satuan", F.round(F.exp(mu + sigma * F.randn(s + 9)), -2))

    # Diskon lebih besar pada hari promo.
    tgl_str = F.date_format("waktu_pesan", "yyyy-MM-dd")
    df = df.withColumn("_u5", F.rand(s + 10)).withColumn("_u6", F.rand(s + 11)).withColumn(
        "diskon_pct",
        F.when(tgl_str.isin(*config.HARI_PROMO_BESAR), F.round(0.15 + F.col("_u5") * 0.25, 2))
         .when(tgl_str.isin(*config.HARI_PROMO), F.round(0.05 + F.col("_u5") * 0.20, 2))
         .otherwise(F.round(F.when(F.col("_u6") < 0.3, F.col("_u5") * 0.10).otherwise(0), 2)))

    # Ongkir dan lama kirim bergantung pada provinsi.
    tambah_ongkir = F.lit(0)
    luar = F.lit(False)
    for p, (_, _, ong, lj) in config.PROVINSI.items():
        tambah_ongkir = F.when(F.col("provinsi") == p, F.lit(ong)).otherwise(tambah_ongkir)
        luar = F.when(F.col("provinsi") == p, F.lit(lj)).otherwise(luar)
    df = (
        df.withColumn("_luar", luar)
        .withColumn("ongkir", F.round(8000 + F.rand(s + 12) * 22000 + tambah_ongkir, -2))
        .withColumn("_kirim", F.round(
            1 + F.abs(F.randn(s + 13)) * F.when(F.col("_luar"), 2.4).otherwise(1.1)
            + F.when(F.col("_luar"), 1.5).otherwise(0)
            # lonjakan volume pada hari promo memperlambat pengiriman
            + F.when(tgl_str.isin(*config.HARI_PROMO_BESAR), 1.2).otherwise(0), 1))
    )

    # Status: peluang batal bergantung metode bayar; retur naik saat kirim lama.
    p_batal = F.lit(0.03)
    for m, (_, pb) in config.METODE_BAYAR.items():
        p_batal = F.when(F.col("metode_bayar") == m, F.lit(pb)).otherwise(p_batal)
    p_retur = 0.01 + 0.012 * F.greatest(F.col("_kirim") - 3, F.lit(0))
    df = df.withColumn("_u7", F.rand(s + 14))
    u_status = F.col("_u7")
    df = df.withColumn(
        "status",
        F.when(u_status < p_batal, "DIBATALKAN")
         .when(u_status < p_batal + p_retur, "DIKEMBALIKAN")
         .otherwise("SELESAI"))

    # Hanya pesanan yang dikirim memiliki lama kirim; rating hanya untuk SELESAI.
    df = (
        df.withColumn("hari_kirim", F.when(F.col("status") != "DIBATALKAN", F.col("_kirim")))
        .withColumn("rating", F.when(
            F.col("status") == "SELESAI",
            F.least(F.lit(5), F.greatest(F.lit(1), F.round(
                5.25 - 0.36 * F.col("_kirim") + 0.55 * F.randn(s + 15)))).cast("int")))
    )

    # ------------------------------------------------------------------
    # Menyuntikkan "kotoran" khas data mentah
    # ------------------------------------------------------------------
    df = df.withColumn("_u8", F.rand(s + 16))
    u = F.col("_u8")
    df = (
        df
        # 0,4% nama provinsi ditulis tidak konsisten (huruf kecil/besar + spasi)
        .withColumn("provinsi", F.when(u < 0.002, F.concat(F.lower("provinsi"), F.lit("  ")))
                                 .when(u < 0.004, F.upper("provinsi"))
                                 .otherwise(F.col("provinsi")))
        # 0,3% harga tidak valid (kosong atau negatif)
        .withColumn("harga_satuan", F.when((u >= 0.004) & (u < 0.006), F.lit(None).cast("double"))
                                     .when((u >= 0.006) & (u < 0.007), -F.col("harga_satuan"))
                                     .otherwise(F.col("harga_satuan")))
        # 0,1% kategori kosong
        .withColumn("kategori", F.when((u >= 0.007) & (u < 0.008), F.lit(None))
                                 .otherwise(F.col("kategori")))
        # 0,2% rating di luar skala 1-5 (salah input sistem lama)
        .withColumn("rating", F.when((u >= 0.008) & (u < 0.010) & F.col("rating").isNotNull(),
                                     F.lit(0)).otherwise(F.col("rating")))
        # 0,3% lama kirim hilang pada pesanan yang dikirim
        .withColumn("hari_kirim", F.when((u >= 0.010) & (u < 0.013), F.lit(None).cast("double"))
                                   .otherwise(F.col("hari_kirim")))
        .withColumn("bulan", F.date_format("waktu_pesan", "yyyy-MM"))
        .withColumn("ingested_at", F.current_timestamp())
        .select("id_pesanan", "waktu_pesan", "provinsi", "kategori", "kanal", "metode_bayar",
                "qty", "harga_satuan", "diskon_pct", "ongkir", "hari_kirim", "status",
                "rating", "bulan", "ingested_at")
    )

    # 0,5% baris terkirim dua kali (duplikat akibat retry API / at-least-once delivery)
    duplikat = df.sample(fraction=0.005, seed=s + 17)
    return df.unionByName(duplikat)


def jalankan(spark, n_rows=config.N_ROWS):
    df = bangkitkan_bronze(spark, n_rows)
    (df.write.mode("overwrite")
       .partitionBy("bulan")          # partisi per bulan -> pruning saat kueri per periode
       .parquet(config.BRONZE_DIR))
    n = spark.read.parquet(config.BRONZE_DIR).count()
    print(f"[bronze] {n:,} baris mentah ditulis ke {config.BRONZE_DIR}")
    return n


if __name__ == "__main__":
    from spark_session import buat_spark
    sp = buat_spark("BD-P09-Bronze")
    jalankan(sp)
    sp.stop()
