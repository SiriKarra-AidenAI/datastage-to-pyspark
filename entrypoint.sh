#!/usr/bin/env bash
set -euo pipefail

# ---- Validate required environment variables ----
: "${POSTGRES_HOST:?}"   ; : "${POSTGRES_PORT:=5432}"
: "${POSTGRES_DB:?}"     ; : "${POSTGRES_USER:?}"
: "${POSTGRES_PASSWORD:?}"

: "${SRC_DIR:=/app/data}"    ; : "${ERR_DIR:=/app/data}"
: "${RUN_DATE:=}"            ; : "${HIGH_DATE:=9999-12-31}"

# ---- Run the PySpark job ----
exec spark-submit \
    --jars /opt/spark/jars/postgresql-42.7.3.jar \
    /app/pyspark_emp_load.py
