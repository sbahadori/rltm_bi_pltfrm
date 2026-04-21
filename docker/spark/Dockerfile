FROM apache/spark:3.5.1-python3

USER root

RUN apt-get update && apt-get install -y --no-install-recommends  curl procps && ln -s /usr/bin/python3 /usr/bin/python && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir  pyyaml==6.0.2  requests==2.32.3  boto3==1.34.162 --quiet

USER spark