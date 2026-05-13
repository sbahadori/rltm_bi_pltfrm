docker build -t rltm-python-base:local -f docker/base/python/Dockerfile .

docker build -t rltm-spark-base:local -f docker/base/spark/Dockerfile .

docker build -t rltm-airflow-base:local -f docker/base/airflow/Dockerfile .