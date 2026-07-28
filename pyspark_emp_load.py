"""
pyspark_cust_sales_scd_agg.py
Generated from DataStage DSExport — JB_CUST_SALES_SCD_AGG
Target: plain PySpark + Postgres JDBC (no Databricks, no Snowflake, no dbutils)
"""
import os
import sys
from datetime import date, datetime, timedelta

import psycopg2
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DateType, DecimalType, TimestampType,
)
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# CONFIG — every runtime value from environment (no plaintext secrets)
# ---------------------------------------------------------------------------
CONFIG: dict = {
    "postgres_host":     os.environ["POSTGRES_HOST"],
    "postgres_port":     int(os.environ.get("POSTGRES_PORT", "5432")),
    "postgres_db":       os.environ["POSTGRES_DB"],
    "postgres_user":     os.environ["POSTGRES_USER"],
    "postgres_password": os.environ["POSTGRES_PASSWORD"],
    "src_dir":           os.environ["SRC_DIR"],
    "err_dir":           os.environ["ERR_DIR"],
    "run_date":          os.environ.get("RUN_DATE", date.today().isoformat()),
    "high_date":         os.environ.get("HIGH_DATE", "9999-12-31"),
}

_JDBC_URL: str = (
    f"jdbc:postgresql://{CONFIG['postgres_host']}:"
    f"{CONFIG['postgres_port']}/{CONFIG['postgres_db']}"
)
_JDBC_PROPS: dict = {
    "user":     CONFIG["postgres_user"],
    "password": CONFIG["postgres_password"],
    "driver":   "org.postgresql.Driver",
}

# ---------------------------------------------------------------------------
# SCHEMAS — from scan.json schemas[].fields[]
# ---------------------------------------------------------------------------
CUST_DELTA_SCHEMA: StructType = StructType([
    StructField("CUST_ID",       StringType(),    nullable=False),
    StructField("CUST_NAME",     StringType(),    nullable=False),
    StructField("ADDRESS",       StringType(),    nullable=True),
    StructField("CITY",          StringType(),    nullable=True),
    StructField("STATE",         StringType(),    nullable=True),
    StructField("ZIP",           StringType(),    nullable=True),
    StructField("SEGMENT",       StringType(),    nullable=True),
    StructField("SOURCE_SYSTEM", StringType(),    nullable=True),
    StructField("EFF_DATE",      DateType(),      nullable=False),
])

SALES_TXN_SCHEMA: StructType = StructType([
    StructField("TXN_ID",      StringType(),          nullable=False),
    StructField("CUST_ID",     StringType(),          nullable=False),
    StructField("PRODUCT_ID",  StringType(),          nullable=False),
    StructField("STORE_ID",    StringType(),          nullable=True),
    StructField("CHANNEL",     StringType(),          nullable=True),
    StructField("TXN_DATE",    DateType(),            nullable=False),
    StructField("QTY",         IntegerType(),         nullable=False),
    StructField("UNIT_PRICE",  DecimalType(12, 2),    nullable=False),
    StructField("AMOUNT",      DecimalType(14, 2),    nullable=False),
])

# DDL — translated from DataStage TableDefinitions, all identifiers double-quoted
# to match what Spark's PostgresDialect emits in INSERT, avoiding case-fold mismatch.
POSTGRES_DDL_CUSTOMER_DIM: str = (
    'CREATE TABLE IF NOT EXISTS "CUSTOMER_DIM"\n'
    '("CUST_KEY" INTEGER NOT NULL,\n'
    '"CUST_ID" VARCHAR(20) NOT NULL,\n'
    '"CUST_NAME" VARCHAR(100) NOT NULL,\n'
    '"ADDRESS" VARCHAR(150),\n'
    '"CITY" VARCHAR(60),\n'
    '"STATE" VARCHAR(20),\n'
    '"ZIP" VARCHAR(10),\n'
    '"SEGMENT" VARCHAR(20),\n'
    '"EFF_DATE" DATE NOT NULL,\n'
    '"EXP_DATE" DATE NOT NULL,\n'
    '"CURR_FLAG" VARCHAR(1) NOT NULL,\n'
    'PRIMARY KEY ("CUST_KEY")\n'
    ')'
)

POSTGRES_DDL_SALES_FACT: str = (
    'CREATE TABLE IF NOT EXISTS "SALES_FACT"\n'
    '("CUST_KEY" INTEGER,\n'
    '"PRODUCT_ID" VARCHAR(20),\n'
    '"STORE_ID" VARCHAR(15),\n'
    '"TXN_DATE" DATE,\n'
    '"TOTAL_QTY" INTEGER,\n'
    '"TOTAL_AMOUNT" NUMERIC(15, 2),\n'
    '"TXN_COUNT" INTEGER,\n'
    '"AVG_UNIT_PRICE" NUMERIC(12, 2),\n'
    'PRIMARY KEY ("CUST_KEY", "PRODUCT_ID", "STORE_ID", "TXN_DATE")\n'
    ')'
)

POSTGRES_DDL_SEQUENCE: str = (
    'CREATE SEQUENCE IF NOT EXISTS "SEQ_CUSTOMER_DIM_KEY"'
)

# ---------------------------------------------------------------------------
# PHASE 0 — idempotent DDL (ALL schema before ANY read)
# ---------------------------------------------------------------------------
def ensure_customer_dim_table_exists() -> None:
    """Create CUSTOMER_DIM if not present (idempotent)."""
    conn = psycopg2.connect(
        host=CONFIG["postgres_host"], port=CONFIG["postgres_port"],
        dbname=CONFIG["postgres_db"], user=CONFIG["postgres_user"],
        password=CONFIG["postgres_password"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(POSTGRES_DDL_CUSTOMER_DIM)
        conn.commit()
        print("[ensure_customer_dim_table_exists] CUSTOMER_DIM verified/created.")
    finally:
        conn.close()


def ensure_sales_fact_table_exists() -> None:
    """Create SALES_FACT if not present (idempotent)."""
    conn = psycopg2.connect(
        host=CONFIG["postgres_host"], port=CONFIG["postgres_port"],
        dbname=CONFIG["postgres_db"], user=CONFIG["postgres_user"],
        password=CONFIG["postgres_password"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(POSTGRES_DDL_SALES_FACT)
        conn.commit()
        print("[ensure_sales_fact_table_exists] SALES_FACT verified/created.")
    finally:
        conn.close()


def ensure_sequence_exists() -> None:
    """Create SEQ_CUSTOMER_DIM_KEY if not present (idempotent)."""
    conn = psycopg2.connect(
        host=CONFIG["postgres_host"], port=CONFIG["postgres_port"],
        dbname=CONFIG["postgres_db"], user=CONFIG["postgres_user"],
        password=CONFIG["postgres_password"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(POSTGRES_DDL_SEQUENCE)
        conn.commit()
        print("[ensure_sequence_exists] SEQ_CUSTOMER_DIM_KEY verified/created.")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# SOURCE READS
# ---------------------------------------------------------------------------
def read_customer_delta(spark: SparkSession) -> DataFrame:
    """STG_SRC_CUSTOMER_DELTA — CSV sequential file read."""
    file_path = f"{CONFIG['src_dir']}/customer_delta_{CONFIG['run_date']}.dat"
    print(f"[read_customer_delta] Reading {file_path}")
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .option("nullValue", "NULL")
        .option("encoding", "UTF-8")
        .option("dateFormat", "yyyy-MM-dd")
        .schema(CUST_DELTA_SCHEMA)
        .load(file_path)
    )


def read_sales_txn(spark: SparkSession) -> DataFrame:
    """STG_SRC_SALES_TXN — CSV sequential file read."""
    file_path = f"{CONFIG['src_dir']}/sales_txn_{CONFIG['run_date']}.dat"
    print(f"[read_sales_txn] Reading {file_path}")
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .option("nullValue", "NULL")
        .option("encoding", "UTF-8")
        .option("dateFormat", "yyyy-MM-dd")
        .schema(SALES_TXN_SCHEMA)
        .load(file_path)
    )


def read_customer_dim_current(spark: SparkSession) -> DataFrame:
    """STG_REF_CUST_DIM_CURRENT — JDBC reference read of current CUSTOMER_DIM rows.
    Schema qualifier stripped from source-platform select_statement to match the
    unqualified table name used in Postgres DDL / write_postgres.
    """
    query = (
        "(SELECT CUST_KEY, CUST_ID, CUST_NAME, ADDRESS, CITY, STATE, ZIP, "
        "SEGMENT, EFF_DATE, EXP_DATE, CURR_FLAG "
        'FROM "CUSTOMER_DIM" WHERE CURR_FLAG = \'Y\') AS cust_dim_current'
    )
    print("[read_customer_dim_current] Reading current dimension snapshot via JDBC")
    return (
        spark.read.format("jdbc")
        .option("url", _JDBC_URL)
        .option("user", CONFIG["postgres_user"])
        .option("password", CONFIG["postgres_password"])
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", query)
        .load()
    )


# ---------------------------------------------------------------------------
# SURROGATE KEY — resumable DB sequence (NOT monotonically_increasing_id)
# ---------------------------------------------------------------------------
def get_next_cust_key_base() -> int:
    """GEN_SURR_KEY — fetch one nextval from Postgres SEQUENCE.
    Returns the base value; the caller distributes per-row offsets to make
    every row's key unique (never broadcast a single value to a batch).
    """
    conn = psycopg2.connect(
        host=CONFIG["postgres_host"], port=CONFIG["postgres_port"],
        dbname=CONFIG["postgres_db"], user=CONFIG["postgres_user"],
        password=CONFIG["postgres_password"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT nextval(\'"SEQ_CUSTOMER_DIM_KEY"\')')
            val = cur.fetchone()[0]
        conn.commit()
        print(f"[get_next_cust_key_base] Base key = {val}")
        return int(val)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# TRANSFORM LAYER
# ---------------------------------------------------------------------------
def apply_customer_enrichment(
    df_sales_txn: DataFrame,
    df_cust_dim_current: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """LKP_CUSTOMER_ENRICH — broadcast left join on CUST_ID.
    Returns (df_enriched, df_enrich_reject).
    Uses the string form of join (on="CUST_ID") so Spark deduplicates the
    join key automatically — no ambiguous-column error downstream.
    """
    joined = df_sales_txn.join(
        F.broadcast(df_cust_dim_current), on="CUST_ID", how="left"
    )
    # Reject: unmatched rows (CUST_KEY is null after left join)
    df_enrich_reject = (
        joined.filter(F.col("CUST_KEY").isNull())
        .select(
            "TXN_ID", "CUST_ID",
            F.lit("CUSTOMER_NOT_FOUND_IN_DIMENSION").alias("ERROR_REASON"),
        )
    )
    # Main output: matched rows
    df_enriched = joined.filter(F.col("CUST_KEY").isNotNull())
    return df_enriched, df_enrich_reject


def apply_dq_validation(
    df_enriched: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """XFM_DQ_VALIDATION — business constraint filter.
    Returns (df_dq_pass, df_dq_reject).
    Pass:  CUST_ID not null AND AMOUNT > 0 AND QTY > 0 AND TXN_DATE not null
    Reject: CUST_ID is null OR AMOUNT <= 0 OR QTY <= 0 OR TXN_DATE is null
    """
    pass_cond = (
        F.col("CUST_ID").isNotNull()
        & (F.col("AMOUNT") > 0)
        & (F.col("QTY") > 0)
        & F.col("TXN_DATE").isNotNull()
    )
    df_dq_pass = df_enriched.filter(pass_cond)

    reject_cond = (
        F.col("CUST_ID").isNull()
        | (F.col("AMOUNT") <= 0)
        | (F.col("QTY") <= 0)
        | F.col("TXN_DATE").isNull()
    )
    df_dq_reject = (
        df_enriched.filter(reject_cond)
        .select(
            "TXN_ID", "CUST_ID",
            F.when(F.col("CUST_ID").isNull(), F.lit("MISSING_CUST_ID"))
            .when(F.col("AMOUNT") <= 0, F.lit("NON_POSITIVE_AMOUNT"))
            .when(F.col("QTY") <= 0, F.lit("NON_POSITIVE_QTY"))
            .otherwise(F.lit("MISSING_TXN_DATE"))
            .alias("ERROR_REASON"),
        )
    )
    return df_dq_pass, df_dq_reject


def apply_change_capture(
    df_cust_delta: DataFrame,
    df_cust_dim_current: DataFrame,
) -> DataFrame:
    """CHG_CUST_SCD_COMPARE — full_outer join on CUST_ID, classify each row.
    CHANGE_CODE: 0=Copy (unchanged, dropped), 1=Insert (new customer),
    3=Edit (attribute change). DropOutputForCode=Delete means type 2 (Copy) is filtered out.
    """
    value_cols = ["CUST_NAME", "ADDRESS", "CITY", "STATE", "ZIP", "SEGMENT"]

    joined = df_cust_delta.alias("after").join(
        df_cust_dim_current.alias("before"), on="CUST_ID", how="full_outer"
    )

    # Classification:
    # - If before.CUST_KEY is null → new customer → Insert (1)
    # - Else if any value column changed → Edit (3)
    # - Else → Copy (0), dropped
    change_cond = F.lit(0)  # default Copy
    for col_name in value_cols:
        change_cond = F.when(
            F.col(f"after.{col_name}") != F.col(f"before.{col_name}"),
            F.lit(3),
        ).otherwise(change_cond)

    result = (
        joined
        .withColumn(
            "CHANGE_CODE",
            F.when(F.col("before.CUST_KEY").isNull(), F.lit(1))
            .otherwise(change_cond),
        )
        .select(
            "CHANGE_CODE",
            F.coalesce(F.col("after.CUST_ID"), F.col("before.CUST_ID")).alias("CUST_ID"),
            F.col("after.CUST_NAME").alias("CUST_NAME"),
            F.col("after.ADDRESS").alias("ADDRESS"),
            F.col("after.CITY").alias("CITY"),
            F.col("after.STATE").alias("STATE"),
            F.col("after.ZIP").alias("ZIP"),
            F.col("after.SEGMENT").alias("SEGMENT"),
            F.col("before.CUST_KEY").alias("EXISTING_CUST_KEY"),
        )
        .filter(F.col("CHANGE_CODE") != F.lit(0))   # DropOutputForCode=Delete drops Copy (0)
        .filter(F.col("CUST_ID").isNotNull())        # safety: only rows with a business key
    )
    return result


def apply_scd2_rules(
    df_change_out: DataFrame,
    base_key: int,
) -> tuple[DataFrame, DataFrame]:
    """XFM_SCD_APPLY — SCD Type 2 expire/insert logic.

    Returns (df_expire, df_new_version).

    Critical: row_number() is assigned ONCE across the combined set of all rows
    that need new keys (Insert + Edit), never independently per sub-group.
    Then each row gets base_key + row_number - 1 as its unique surrogate.
    """
    run_date_val = CONFIG["run_date"]
    high_date_val = CONFIG["high_date"]

    # Compute expire date: RUN_DATE minus 1 day
    run_dt = datetime.strptime(run_date_val, "%Y-%m-%d")
    expire_dt = run_dt - timedelta(days=1)
    expire_date_str = expire_dt.strftime("%Y-%m-%d")

    # ---- Expire rows: CHANGE_CODE=3 (Edit) - expire the existing row ----
    df_expire = (
        df_change_out
        .filter(F.col("CHANGE_CODE") == F.lit(3))
        .select(
            F.col("EXISTING_CUST_KEY").alias("CUST_KEY"),
            F.lit("N").alias("CURR_FLAG"),
            F.lit(expire_date_str).cast(DateType()).alias("EXP_DATE"),
        )
    )

    # ---- New version rows: CHANGE_CODE=1 (Insert) OR CHANGE_CODE=3 (Edit) ----
    # Combine ALL rows needing new keys first, THEN assign row_number once.
    df_needs_key = df_change_out.filter(
        (F.col("CHANGE_CODE") == F.lit(1)) | (F.col("CHANGE_CODE") == F.lit(3))
    )

    # Assign position once across the combined set
    key_window = Window.orderBy("CUST_ID")
    df_with_pos = df_needs_key.withColumn("_pos", F.row_number().over(key_window))

    df_new_version = df_with_pos.select(
        (F.lit(base_key) + F.col("_pos") - F.lit(1)).cast(IntegerType()).alias("CUST_KEY"),
        "CUST_ID", "CUST_NAME", "ADDRESS", "CITY", "STATE", "ZIP", "SEGMENT",
        F.col("EXISTING_CUST_KEY"),   # carried through, may be null for Insert
        F.lit("Y").alias("CURR_FLAG"),
        F.lit(run_date_val).cast(DateType()).alias("EFF_DATE"),
        F.lit(high_date_val).cast(DateType()).alias("EXP_DATE"),
    )

    return df_expire, df_new_version


def sort_for_aggregation(df_dq_pass: DataFrame) -> DataFrame:
    """SRT_SALES_ENRICHED — sort on grouping keys ahead of aggregation."""
    return df_dq_pass.orderBy(
        F.col("CUST_KEY").asc(),
        F.col("PRODUCT_ID").asc(),
        F.col("STORE_ID").asc(),
    )


def aggregate_sales(df_sorted: DataFrame) -> DataFrame:
    """AGG_SALES_SUMMARY — groupBy().agg() to customer/product/store/day grain."""
    return (
        df_sorted
        .groupBy("CUST_KEY", "PRODUCT_ID", "STORE_ID", "TXN_DATE")
        .agg(
            F.sum("QTY").alias("TOTAL_QTY"),
            F.sum("AMOUNT").alias("TOTAL_AMOUNT"),
            F.count("TXN_ID").alias("TXN_COUNT"),
            F.avg("UNIT_PRICE").alias("AVG_UNIT_PRICE"),
        )
    )


def funnel_errors(
    df_enrich_reject: DataFrame,
    df_dq_reject: DataFrame,
) -> DataFrame:
    """FUN_ERROR_FUNNEL — union of two reject streams + audit columns."""
    # Normalise both to same column list
    cols = ["TXN_ID", "CUST_ID", "ERROR_REASON"]

    enriched_norm = df_enrich_reject.select(*cols)
    dq_norm = df_dq_reject.select(*cols)

    funneled = enriched_norm.unionByName(dq_norm).select(
        "*",
        F.current_timestamp().alias("ERROR_TIMESTAMP"),
        F.lit(CONFIG["run_date"]).cast(DateType()).alias("RUN_DATE"),
    )
    return funneled


# ---------------------------------------------------------------------------
# WRITE LAYER
# ---------------------------------------------------------------------------
def write_customer_dim(
    df_expire: DataFrame,
    df_new_version: DataFrame,
) -> None:
    """TGT_CUSTOMER_DIM — SCD2 upsert (redesign: manual MERGE logic).

    Phase A: EXPIRE existing rows. UPDATE "CUSTOMER_DIM" SET
    "CURR_FLAG"='N', "EXP_DATE"=<run_date-1> WHERE "CUST_KEY"=<existing_key>.
    Phase B: INSERT new/version rows via JDBC append.
    """
    # --- Phase A: Expire (UPDATE) via psycopg2 ---
    expire_rows = df_expire.collect()
    if expire_rows:
        conn = psycopg2.connect(
            host=CONFIG["postgres_host"], port=CONFIG["postgres_port"],
            dbname=CONFIG["postgres_db"], user=CONFIG["postgres_user"],
            password=CONFIG["postgres_password"],
        )
        try:
            with conn.cursor() as cur:
                for row in expire_rows:
                    cur.execute(
                        'UPDATE "CUSTOMER_DIM" '
                        'SET "CURR_FLAG" = %s, "EXP_DATE" = %s '
                        'WHERE "CUST_KEY" = %s',
                        (row["CURR_FLAG"], row["EXP_DATE"], row["CUST_KEY"]),
                    )
            conn.commit()
            print(f"[write_customer_dim] Expired {len(expire_rows)} existing rows.")
        finally:
            conn.close()

    # --- Phase B: Insert new versions (JDBC append) ---
    n_new = df_new_version.count()
    if n_new == 0:
        print("[write_customer_dim] No new rows to insert.")
        return

    insert_cols = [
        "CUST_KEY", "CUST_ID", "CUST_NAME", "ADDRESS", "CITY", "STATE",
        "ZIP", "SEGMENT", "EFF_DATE", "EXP_DATE", "CURR_FLAG",
    ]
    (
        df_new_version
        .select(*insert_cols)
        .write.format("jdbc")
        .option("url", _JDBC_URL)
        .option("user", CONFIG["postgres_user"])
        .option("password", CONFIG["postgres_password"])
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", '"CUSTOMER_DIM"')
        .option("batchsize", "10000")
        .mode("append")
        .save()
    )
    print(f"[write_customer_dim] Inserted {n_new} new version rows.")


def write_sales_fact(df_aggregated: DataFrame) -> None:
    """TGT_SALES_FACT — JDBC append (write_mode=insert → .mode("append"))."""
    n = df_aggregated.count()
    if n == 0:
        print("[write_sales_fact] No rows to write.")
        return

    insert_cols = [
        "CUST_KEY", "PRODUCT_ID", "STORE_ID", "TXN_DATE",
        "TOTAL_QTY", "TOTAL_AMOUNT", "TXN_COUNT", "AVG_UNIT_PRICE",
    ]
    (
        df_aggregated
        .select(*insert_cols)
        .write.format("jdbc")
        .option("url", _JDBC_URL)
        .option("user", CONFIG["postgres_user"])
        .option("password", CONFIG["postgres_password"])
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", '"SALES_FACT"')
        .option("batchsize", "10000")
        .mode("append")
        .save()
    )
    print(f"[write_sales_fact] Appended {n} rows.")


def write_error_log(df_funneled: DataFrame) -> None:
    """TGT_ERROR_LOG — CSV overwrite to error directory."""
    file_path = f"{CONFIG['err_dir']}/sales_errors_{CONFIG['run_date']}.dat"
    n = df_funneled.count()
    (
        df_funneled
        .write.format("csv")
        .option("header", "true")
        .mode("overwrite")
        .save(file_path)
    )
    print(f"[write_error_log] Wrote {n} error rows to {file_path}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main() -> None:
    spark = (
        SparkSession.builder
        .appName("JB_CUST_SALES_SCD_AGG")
        .getOrCreate()
    )

    # ---- Phase 0: Provision ALL schema before any reads -------------------
    ensure_sequence_exists()
    ensure_customer_dim_table_exists()
    ensure_sales_fact_table_exists()

    # ---- Phase 1: Read all sources ----------------------------------------
    df_cust_delta = read_customer_delta(spark)
    df_sales_txn = read_sales_txn(spark)
    df_cust_dim_current = read_customer_dim_current(spark)

    # ---- Phase 2: SCD2 path — ChangeCapture + SurrogateKey + SCD Apply ---
    df_change_out = apply_change_capture(df_cust_delta, df_cust_dim_current)
    n_needs_key = df_change_out.filter(
        (F.col("CHANGE_CODE") == F.lit(1)) | (F.col("CHANGE_CODE") == F.lit(3))
    ).count()
    base_key = get_next_cust_key_base() if n_needs_key > 0 else 0
    df_expire, df_new_version = apply_scd2_rules(df_change_out, base_key)

    # ---- Phase 3: Sales aggregation path ----------------------------------
    df_enriched, df_enrich_reject = apply_customer_enrichment(
        df_sales_txn, df_cust_dim_current
    )
    df_dq_pass, df_dq_reject = apply_dq_validation(df_enriched)
    df_sorted = sort_for_aggregation(df_dq_pass)
    df_aggregated = aggregate_sales(df_sorted)

    # ---- Phase 4: Error funnel path ---------------------------------------
    df_funneled = funnel_errors(df_enrich_reject, df_dq_reject)

    # ---- Phase 5: Write sinks ---------------------------------------------
    write_customer_dim(df_expire, df_new_version)
    write_sales_fact(df_aggregated)
    write_error_log(df_funneled)

    print("[main] Job complete.")
    spark.stop()


if __name__ == "__main__":
    main()
