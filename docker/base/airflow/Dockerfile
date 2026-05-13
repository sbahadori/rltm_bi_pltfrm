FROM apache/airflow:3.1.8-python3.11

USER root

RUN apt-get update && apt-get install -y --no-install-recommends  openjdk-17-jre-headless  curl  procps  && apt-get clean  && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install --no-cache-dir  pyyaml==6.0.2   pyspark==3.5.1   delta-spark==3.2.0   requests==2.32.3 boto3==1.34.162  psycopg2-binary  --quiet

COPY --chown=airflow:0 shared/spark-jars/hadoop-aws-3.3.4.jar /tmp/local-spark-jars/
COPY --chown=airflow:0 shared/spark-jars/aws-java-sdk-bundle-1.12.262.jar /tmp/local-spark-jars/
COPY --chown=airflow:0 shared/spark-jars/mssql-jdbc-13.4.0.jre11.jar /tmp/local-spark-jars/
COPY --chown=airflow:0 shared/spark-jars/delta-spark_2.12-3.2.0.jar /tmp/local-spark-jars/
COPY --chown=airflow:0 shared/spark-jars/delta-storage-3.2.0.jar /tmp/local-spark-jars/
COPY --chown=airflow:0 shared/spark-jars/postgresql-42.7.4.jar /tmp/local-spark-jars/
COPY --chown=airflow:0 shared/spark-jars/mysql-connector-j-9.0.0.jar /tmp/local-spark-jars/

RUN python - <<'PY'
from pathlib import Path
import shutil
import pyspark

src_dir = Path("/tmp/local-spark-jars")
dst_dir = Path(pyspark.__file__).resolve().parent / "jars"
dst_dir.mkdir(parents=True, exist_ok=True)

for jar in src_dir.glob("*.jar"):
    shutil.copy2(jar, dst_dir / jar.name)
PY