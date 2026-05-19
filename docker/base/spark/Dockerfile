ARG PYTHON_BASE_IMAGE=rltm-python-base:local
FROM ${PYTHON_BASE_IMAGE}

ARG SPARK_VERSION=3.5.1
ARG HADOOP_VERSION=3
ARG SPARK_PACKAGE=spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}

ENV SPARK_HOME=/opt/spark  JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64  PATH=/opt/spark/bin:/opt/spark/sbin:${PATH}  PYSPARK_PYTHON=python3  PYSPARK_DRIVER_PYTHON=python3

USER root

RUN apt-get update && apt-get install -y --no-install-recommends  openjdk-17-jre-headless  && apt-get clean  && rm -rf /var/lib/apt/lists/*

COPY shared/spark-dist/spark-3.5.1-bin-hadoop3.tgz /tmp/spark.tgz

RUN tar -xzf /tmp/spark.tgz -C /opt  && mv /opt/${SPARK_PACKAGE} ${SPARK_HOME}  && rm -f /tmp/spark.tgz

COPY shared/spark-jars/hadoop-aws-3.3.4.jar /opt/spark/jars/
COPY shared/spark-jars/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/
COPY shared/spark-jars/mssql-jdbc-13.4.0.jre11.jar /opt/spark/jars/
COPY shared/spark-jars/delta-spark_2.12-3.2.0.jar /opt/spark/jars/
COPY shared/spark-jars/delta-storage-3.2.0.jar /opt/spark/jars/
COPY shared/spark-jars/postgresql-42.7.4.jar /opt/spark/jars/
COPY shared/spark-jars/mysql-connector-j-9.0.0.jar /opt/spark/jars/

COPY shared/spark-jars/spark-sql-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/
COPY shared/spark-jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/
COPY shared/spark-jars/kafka-clients-3.5.1.jar /opt/spark/jars/
COPY shared/spark-jars/commons-pool2-2.11.1.jar /opt/spark/jars/



RUN pip install --no-cache-dir  pyyaml==6.0.2   requests==2.32.3  boto3==1.34.162  delta-spark==3.2.0  psycopg2-binary  pyarrow  pandas   numpy  --quiet

RUN useradd -m -u 1001 -s /bin/bash spark  && mkdir -p /opt/spark/work-dir /tmp/.ivy2 /tmp/checkpoints /tmp/health  && chown -R spark:spark /opt/spark /tmp/.ivy2 /tmp/checkpoints /tmp/health /home/spark

WORKDIR /opt/spark/work-dir

USER spark

CMD ["python3", "--version"]