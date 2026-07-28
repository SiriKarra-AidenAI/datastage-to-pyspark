FROM apache/spark:3.5.1-scala2.12-java17-python3-ubuntu

# ---- Download Postgres JDBC driver ----
RUN curl -sL -o /opt/spark/jars/postgresql-42.7.3.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

# ---- App directory ----
WORKDIR /app

# ---- Copy application files ----
COPY pyspark_emp_load.py .
COPY entrypoint.sh .
COPY requirements.txt .
COPY data/ ./data/

# ---- Install Python dependencies ----
# pyspark is provided by the base image; only psycopg2-binary is added here
RUN pip install --no-cache-dir -r requirements.txt

# ---- Make entrypoint executable ----
RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
