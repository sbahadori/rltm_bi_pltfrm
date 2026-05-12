ARG PYTHON_BASE_IMAGE=rltm-python-base:local
FROM ${PYTHON_BASE_IMAGE}

WORKDIR /app

COPY apps/dashboard/app.py .

RUN pip install --no-cache-dir  fastapi==0.115.0  uvicorn[standard]==0.30.6 --quiet

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8001"]