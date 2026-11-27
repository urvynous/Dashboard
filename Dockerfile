# ============================================================================
# Image untuk KLASTER SPARK VIRTUAL (master, worker), pipeline, dan dashboard.
# Basis: image resmi Apache Spark (Java 17 + Python 3) dari Docker Hub.
# Ganti SPARK_TAG bila ingin versi lain, mis. 3.5.9-scala2.12-java17-python3-ubuntu
# ============================================================================
ARG SPARK_TAG=3.5.7-scala2.12-java17-python3-ubuntu
FROM spark:${SPARK_TAG}

USER root

# PySpark diambil dari instalasi Spark di image (versi driver == versi klaster).
RUN ln -sf "$(ls /opt/spark/python/lib/py4j-*-src.zip | head -n1)" /opt/spark/python/lib/py4j-src.zip
ENV SPARK_HOME=/opt/spark \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-src.zip \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PATH="/opt/spark/bin:/opt/spark/sbin:${PATH}"

# Pustaka Python untuk pipeline dan dashboard (pyspark tidak diinstal ulang).
COPY requirements.txt /tmp/requirements.txt
RUN grep -v -i "^pyspark" /tmp/requirements.txt > /tmp/req-docker.txt \
 && (pip3 install --no-cache-dir -r /tmp/req-docker.txt \
     || pip3 install --no-cache-dir --break-system-packages -r /tmp/req-docker.txt)

WORKDIR /opt/project
