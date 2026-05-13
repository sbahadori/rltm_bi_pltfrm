FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1   PIP_NO_CACHE_DIR=1    PYTHONDONTWRITEBYTECODE=1

USER root

RUN apt-get update && apt-get install -y --no-install-recommends  bash  curl  ca-certificates  procps   tini  && apt-get clean  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python3", "--version"]