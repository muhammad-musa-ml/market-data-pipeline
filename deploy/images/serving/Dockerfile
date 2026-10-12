FROM python:3.13-slim

WORKDIR /app
ENV PYTHONPATH=/app/src PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

COPY requirements.txt requirements-serving.txt ./
RUN pip install --no-cache-dir -r requirements-serving.txt

COPY proto proto
COPY sql sql
COPY scripts scripts
COPY src src
RUN python scripts/gen_proto.py
