FROM apache/airflow:3.1.8-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps && apt-get clean && rm -rf /var/lib/apt/lists/* 

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
RUN pip install --no-cache-dir pyyaml==6.0.2  pyspark==3.5.1  delta-spark==3.2.0  requests==2.32.3  boto3==1.34.162 --quiet

# Local S3A / MinIO JARs
COPY --chown=airflow:0 downloads/jars/hadoop-aws-3.3.4.jar /tmp/local-spark-jars/
COPY --chown=airflow:0 downloads/jars/aws-java-sdk-bundle-1.12.262.jar /tmp/local-spark-jars/

RUN python - <<'PY'
from pathlib import Path
import shutil
import pyspark

src_dir = Path("/tmp/local-spark-jars")
dst_dir = Path(pyspark.__file__).resolve().parent / "jars"

dst_dir.mkdir(parents=True, exist_ok=True)

for jar in src_dir.glob("*.jar"):
    target = dst_dir / jar.name
    shutil.copy2(jar, target)
    print(f"Copied {jar.name} -> {target}")

print(f"PySpark jars directory: {dst_dir}")
PY