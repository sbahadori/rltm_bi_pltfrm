ARG PYTHON_BASE_IMAGE=rltm-python-base:local
FROM ${PYTHON_BASE_IMAGE}

WORKDIR /workspace/rltm_bi_pltfrm

COPY apps/dashboard ./apps/dashboard
COPY shared ./shared

ENV PYTHONPATH=/workspace/rltm_bi_pltfrm


RUN pip install --no-cache-dir  fastapi==0.115.0  uvicorn[standard]==0.30.6 psycopg2-binary==2.9.9 --quiet

WORKDIR /workspace/rltm_bi_pltfrm/apps/dashboard


CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8001"]