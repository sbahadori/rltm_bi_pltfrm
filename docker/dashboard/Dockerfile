FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY apps/dashboard/app.py .

RUN pip install fastapi==0.115.0 uvicorn[standard]==0.30.6

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8001"]