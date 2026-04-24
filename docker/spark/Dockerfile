FROM python:3.11-slim-bookworm

ARG SPARK_VERSION=3.5.1
ARG HADOOP_VERSION=3
ARG SPARK_PACKAGE=spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}
ARG SPARK_URL=https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz

ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

USER root
RUN apt-get update && apt-get install -y --no-install-recommends bash openjdk-17-jre-headless curl tini procps && rm -rf /var/lib/apt/lists/*

# RUN curl -fsSL "${SPARK_URL}" -o /tmp/spark.tgz && tar -xzf /tmp/spark.tgz -C /opt && mv /opt/${SPARK_PACKAGE} ${SPARK_HOME} && rm -f /tmp/spark.tgz
COPY downloads/spark-3.5.1-bin-hadoop3.tgz /tmp/spark.tgz

RUN tar -xzf /tmp/spark.tgz -C /opt  && mv /opt/spark-3.5.1-bin-hadoop3 /opt/spark  && rm -f /tmp/spark.tgz


RUN pip install --no-cache-dir pyyaml==6.0.2 requests==2.32.3 boto3==1.34.162 delta-spark==3.2.0 --quiet

RUN useradd -m -u 1001 -s /bin/bash spark && mkdir -p /opt/spark/work-dir /tmp/.ivy2 /tmp/checkpoints /tmp/health && chown -R spark:spark /opt/spark /tmp/.ivy2 /tmp/checkpoints /tmp/health /home/spark

WORKDIR /opt/spark/work-dir

USER spark

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python3", "--version"]