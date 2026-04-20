FROM apache/airflow:3.1.8-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
RUN pip install --no-cache-dir pyyaml==6.0.2  pyspark==3.5.1  delta-spark==3.2.0  requests==2.32.3  boto3==1.34.162 --quiet