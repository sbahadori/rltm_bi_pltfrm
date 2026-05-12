ARG SPARK_BASE_IMAGE=rltm-spark-base:local
FROM ${SPARK_BASE_IMAGE}

USER root

RUN pip install --no-cache-dir  jupyterlab   notebook   matplotlib   ipykernel   tabulate --quiet

RUN mkdir -p /opt/notebooks && chown -R spark:spark /opt/notebooks

WORKDIR /opt/notebooks

EXPOSE 8888

USER spark

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--NotebookApp.token=", "--NotebookApp.password="]