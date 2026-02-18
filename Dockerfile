# ===============================
# Dockerfile: Spark 4.1.1 + Python 3.12
# ===============================
FROM eclipse-temurin:17-jdk

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        git \
        python3.12 \
        python3.12-venv \
        python3-pip \
        wget \
        curl \
        tar \
        procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/opt/java/openjdk
ENV PATH=$JAVA_HOME/bin:$PATH

# -------------------------------
# Install Spark 4.1.1 (Hadoop 3)
# -------------------------------
ENV SPARK_VERSION=4.1.1
ENV HADOOP_VERSION=3

RUN wget -q https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

# -------------------------------
# PySpark environment config
# -------------------------------
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-src.zip:$PYTHONPATH

# -------------------------------
# Set working directory
# -------------------------------
WORKDIR /opt/spark/work-dir

# -------------------------------
# Install Python dependencies
# -------------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir --break-system-packages -r requirements.txt

# -------------------------------
# Default entrypoint
# -------------------------------
ENTRYPOINT ["/bin/bash"]
