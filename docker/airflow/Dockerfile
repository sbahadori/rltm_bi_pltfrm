FROM runtime:latest
ENV PATH="${AIRFLOW_HOME}/.local/bin:${PATH}"
# Airflow runtime is already provided by the base image.
# Keep this image thin so scheduler, dag-processor, and api-server
# all use the exact same Spark/Java/Python runtime.