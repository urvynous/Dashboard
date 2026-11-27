"""Pembuat SparkSession yang sama untuk mode lokal maupun klaster virtual."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: E402


def buat_spark(nama_aplikasi="BD-P09-Pipeline"):
    # Driver dan worker memakai interpreter Python yang sama (penting di mode lokal).
    if config.SPARK_MASTER.startswith("local"):
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    from pyspark.sql import SparkSession

    # Java 17/21 menutup akses internal NIO yang dipakai Apache Arrow (toPandas cepat).
    opsi_java = "--add-opens=java.base/java.nio=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true"

    spark = (
        SparkSession.builder
        .master(config.SPARK_MASTER)
        .appName(nama_aplikasi)
        .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY)
        .config("spark.driver.extraJavaOptions", opsi_java)
        .config("spark.executor.extraJavaOptions", opsi_java)
        .config("spark.sql.shuffle.partitions", str(config.SPARK_SHUFFLE_PARTITIONS))
        # Arrow dimatikan: tabel gold kecil sehingga toPandas() tanpa Arrow sudah
        # cepat, dan Arrow sering gagal di JDK 21 ("DirectByteBuffer not available").
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        # Adaptive Query Execution: Spark menyesuaikan jumlah partisi saat runtime.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "Asia/Jakarta")
        .config("spark.ui.showConsoleProgress", "false")
    )

    # Pengaturan khusus klaster virtual (diisi lewat docker-compose.yml).
    if os.environ.get("SPARK_DRIVER_HOST"):
        # worker harus bisa menghubungi balik driver lewat nama kontainer
        spark = (spark.config("spark.driver.host", os.environ["SPARK_DRIVER_HOST"])
                      .config("spark.driver.bindAddress", "0.0.0.0"))
    if os.environ.get("SPARK_EXECUTOR_MEMORY"):
        spark = spark.config("spark.executor.memory", os.environ["SPARK_EXECUTOR_MEMORY"])

    spark = spark.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def info_spark(spark):
    sc = spark.sparkContext
    return {
        "spark_versi": spark.version,
        "master": sc.master,
        "core_paralel": sc.defaultParallelism,
        "app_id": sc.applicationId,
        "spark_ui": sc.uiWebUrl,
    }
