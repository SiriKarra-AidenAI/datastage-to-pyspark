# DataStage → PySpark Mapping

> **Stage:** Source-Target Mapping (design / source-target-mapping)  
> **Date:** 2026-07-28  
> **Source:** IBM DataStage (DSExport XML)  
> **Target:** Plain PySpark + Postgres JDBC  

---

## 1. Classification legend

| Band | Tier | Disposition | Effort | Description |
|---|---|---|---|---|
| 🟢 DIRECT | 1 (verify) | auto | low | 1:1 PySpark equivalent, mechanical conversion |
| 🟡 WORKAROUND | 2 (review) | assisted | medium | Composite/non-trivial config, agentic reasoning required |
| 🔴 REDESIGN | 3 (manual) | manual | high | No clean PySpark equivalent; requires merge/UDF/custom logic |

---

## 2. Stage classification

| Op | Count | Band | Tier | Score | Target | How |
|---|---|---|---|---|---|---|
| PxSequentialFile | 3 | 🟢 DIRECT | 1 | 0.95 | `spark.read.format('csv')` | Map params: delimiter, header=true, nullValue, encoding, dateFormat. Apply schema from schemas[].fields[]. |
| PxDB2Connector | 3 | 🟢 DIRECT | 1 | 0.88 | `df.write.format('jdbc')` (sink) / `spark.read.format('jdbc')` (source) | Sink: DDL from TableDefinitions → CREATE TABLE IF NOT EXISTS, JDBC write. Source/reference: JDBC read, broadcast for lookup. |
| PxLookup | 1 | 🟡 WORKAROUND | 2 | 0.72 | broadcast `.join(ref_df, condition, 'left')` + reject routing | Broadcast-join primary stream against reference DF on key columns. OnNotFound=Reject → split null-match rows to reject link. |
| PxTransformer | 2 | 🟡 WORKAROUND | 2 | 0.70 | `withColumn` / `select` / `expr` | Simple derivations → withColumn+expr. Complex expressions (regex, date arithmetic) may need UDF. |
| PxChangeCapture | 1 | 🟡 WORKAROUND | 2 | 0.65 | `before_df.join(after_df, KeyColumns, 'full_outer')` + `when/otherwise` | Full-outer join Before/After on CUST_ID; compare value columns → CHANGE_CODE (0=Copy, 1=Insert, 3=Edit). DropOutputForCode=Delete means type-2 is filtered out. |
| PxSurrogateKeyGenerator | 1 | 🟡 WORKAROUND | 2 | 0.68 | Resumable DB sequence / high-water-mark table | NOT monotonically_increasing_id(). Read next value from `EDW.SEQ_CUSTOMER_DIM_KEY` DB sequence, resumable across runs. |
| PxSort | 1 | 🟢 DIRECT | 1 | 0.92 | `.orderBy(cols)` | Map sort keys and ascending/descending flags. |
| PxAggregator | 1 | 🟡 WORKAROUND | 2 | 0.75 | `.groupBy().agg()` | Map group keys and aggregation functions (Sum, Count, Average). |
| PxFunnel | 1 | 🟢 DIRECT | 1 | 0.88 | `.union()` | Union multiple input DataFrames into one stream. |

---

## 3. Node to method mapping

### 1:2 split: PxDB2Connector as sink

A **PxDB2Connector sink stage** maps to TWO PySpark methods:

| Method | Purpose |
|---|---|
| `ensure_table_exists()` | DDL creation — `CREATE TABLE IF NOT EXISTS` derived from TableDefinitions record. Idempotent. |
| `write_postgres()` | DML data write — `df.write.format("jdbc").mode(...).options(...).save()` |

### 1:1 mapping: PxDB2Connector as source/reference

A **PxDB2Connector source/reference stage** maps to ONE method:

| Method | Purpose |
|---|---|
| `read_reference()` | JDBC read via `spark.read.format("jdbc")` using connection_name + table/select_statement. Broadcast for downstream lookup. |

### All stage → method mappings

| Stage | Op | Role | Method(s) |
|---|---|---|---|
| STG_SRC_CUSTOMER_DELTA | PxSequentialFile | source | `read_source()` |
| STG_SRC_SALES_TXN | PxSequentialFile | source | `read_source()` |
| STG_REF_CUST_DIM_CURRENT | PxDB2Connector | source | `read_reference()` |
| GEN_SURR_KEY | PxSurrogateKeyGenerator | source | `generate_surrogate_key()` |
| LKP_CUSTOMER_ENRICH | PxLookup | transform | `apply_lookup()` |
| XFM_DQ_VALIDATION | PxTransformer | transform | `apply_transformations()` |
| CHG_CUST_SCD_COMPARE | PxChangeCapture | transform | `apply_change_capture()` |
| XFM_SCD_APPLY | PxTransformer | transform | `apply_transformations()` |
| SRT_SALES_ENRICHED | PxSort | transform | `apply_transformations()` |
| AGG_SALES_SUMMARY | PxAggregator | transform | `apply_transformations()` |
| FUN_ERROR_FUNNEL | PxFunnel | transform | `apply_transformations()` |
| TGT_CUSTOMER_DIM | PxDB2Connector | sink | `ensure_table_exists()`, `write_postgres()` |
| TGT_SALES_FACT | PxDB2Connector | sink | `ensure_table_exists()`, `write_postgres()` |
| TGT_ERROR_LOG | PxSequentialFile | sink | `write_output()` |

---

## 4. Type mapping

| DataStage type | Spark type | Postgres DDL |
|---|---|---|
| VARCHAR(1) | StringType() | VARCHAR(1) |
| VARCHAR(10) | StringType() | VARCHAR(10) |
| VARCHAR(15) | StringType() | VARCHAR(15) |
| VARCHAR(20) | StringType() | VARCHAR(20) |
| VARCHAR(25) | StringType() | VARCHAR(25) |
| VARCHAR(50) | StringType() | VARCHAR(50) |
| VARCHAR(60) | StringType() | VARCHAR(60) |
| VARCHAR(100) | StringType() | VARCHAR(100) |
| VARCHAR(150) | StringType() | VARCHAR(150) |
| INTEGER | IntegerType() | INTEGER |
| DATE | DateType() | DATE |
| TIMESTAMP | TimestampType() | TIMESTAMP |
| DECIMAL(12,2) | DecimalType(12,2) | NUMERIC(12,2) |
| DECIMAL(14,2) | DecimalType(14,2) | NUMERIC(14,2) |
| DECIMAL(15,2) | DecimalType(15,2) | NUMERIC(15,2) |

**58 fields across 7 schemas — all types covered.**

---

## 5. DDL translation

### Connection: TGT_CUSTOMER_DIM (EDW.CUSTOMER_DIM)

DDL derived from `TableDefinitions DB2\CUSTOMER_DIM` — no explicit CreateStatement in source.

```sql
CREATE TABLE IF NOT EXISTS "CUSTOMER_DIM"
("CUST_KEY" INTEGER NOT NULL,
 "CUST_ID" VARCHAR(20) NOT NULL,
 "CUST_NAME" VARCHAR(100) NOT NULL,
 "ADDRESS" VARCHAR(150),
 "CITY" VARCHAR(60),
 "STATE" VARCHAR(20),
 "ZIP" VARCHAR(10),
 "SEGMENT" VARCHAR(20),
 "EFF_DATE" DATE NOT NULL,
 "EXP_DATE" DATE NOT NULL,
 "CURR_FLAG" VARCHAR(1) NOT NULL,
 PRIMARY KEY ("CUST_KEY"))
```

### Connection: TGT_SALES_FACT (EDW.SALES_FACT)

DDL derived from `TableDefinitions DB2\SALES_FACT` — no explicit CreateStatement in source.

```sql
CREATE TABLE IF NOT EXISTS "SALES_FACT"
("CUST_KEY" INTEGER,
 "PRODUCT_ID" VARCHAR(20),
 "STORE_ID" VARCHAR(15),
 "TXN_DATE" DATE,
 "TOTAL_QTY" INTEGER,
 "TOTAL_AMOUNT" NUMERIC(15, 2),
 "TXN_COUNT" INTEGER,
 "AVG_UNIT_PRICE" NUMERIC(12, 2),
 PRIMARY KEY ("CUST_KEY", "PRODUCT_ID", "STORE_ID", "TXN_DATE"))
```

### Connection: TGT_ERROR_LOG (file)

No DDL — file sink (`#ERR_DIR#/sales_errors_#RUN_DATE#.dat`). Schema from `TableDefinitions SEQ\SALES_ERRORS`: TXN_ID VARCHAR(25), CUST_ID VARCHAR(20), ERROR_REASON VARCHAR(50), ERROR_TIMESTAMP TIMESTAMP, RUN_DATE DATE.

---

## 6. Write mode

| Connection | Write mode | PySpark mode | Disposition | Reason |
|---|---|---|---|---|
| TGT_CUSTOMER_DIM | upsert | UNSUPPORTED (merge/MERGE-INTO) | 🔴 manual | No direct PySpark equivalent; requires MERGE on CUST_KEY |
| TGT_SALES_FACT | insert | append (`.mode("append")`) | 🟢 auto | Direct JDBC append |
| TGT_ERROR_LOG | overwrite | overwrite (`.mode("overwrite")`) | 🟢 auto | Direct CSV overwrite |

---

## 7. Read options

### Source: STG_SRC_CUSTOMER_DELTA (PxSequentialFile, CSV)

```python
df_cust_delta = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("nullValue", "NULL") \
    .option("encoding", "UTF-8") \
    .option("dateFormat", "yyyy-MM-dd") \
    .schema(cust_delta_schema) \
    .load(f"{SRC_DIR}/customer_delta_{RUN_DATE}.dat")
```

### Source: STG_SRC_SALES_TXN (PxSequentialFile, CSV)

```python
df_sales_txn = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("nullValue", "NULL") \
    .option("encoding", "UTF-8") \
    .option("dateFormat", "yyyy-MM-dd") \
    .schema(sales_txn_schema) \
    .load(f"{SRC_DIR}/sales_txn_{RUN_DATE}.dat")
```

### Source: STG_REF_CUST_DIM_CURRENT (PxDB2Connector, JDBC reference)

```python
df_cust_dim_current = spark.read.format("jdbc") \
    .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}") \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PASSWORD) \
    .option("dbtable", """(SELECT CUST_KEY, CUST_ID, CUST_NAME, ADDRESS, CITY, STATE, ZIP,
                           SEGMENT, EFF_DATE, EXP_DATE, CURR_FLAG
                           FROM EDW.CUSTOMER_DIM WHERE CURR_FLAG = 'Y') AS cust_dim_current""") \
    .load()
```

### Source: GEN_SURR_KEY (PxSurrogateKeyGenerator)

- **Key source:** `Db2Sequence` → `EDW.SEQ_CUSTOMER_DIM_KEY`
- **Output column:** `NEW_CUST_KEY`
- Reads next value from DB sequence (resumable); NOT `monotonically_increasing_id()`.

---

## 8. Redesign queue

| Item | Trigger | Required action |
|---|---|---|
| TGT_CUSTOMER_DIM | write_mode=upsert | No direct PySpark equivalent. Implement **MERGE/MERGE-INTO** on `CUST_KEY`: (1) UPDATE existing rows (SET EXP_DATE, CURR_FLAG='N'), (2) INSERT new version rows. Requires Postgres JDBC merge or application-level upsert logic. |
| PxChangeCapture + SCD2 chain | SCD Type 2 composition | The CHG_CUST_SCD_COMPARE → GEN_SURR_KEY → XFM_SCD_APPLY → TGT_CUSTOMER_DIM chain is a compound pattern. Full-outer join + key generation + expire/insert split — no single PySpark primitive. |
| Surrogate Key Generator | DB sequence dependency | `SEQ_CUSTOMER_DIM_KEY` must exist in Postgres as a native sequence. Resumability across runs requires checkpointing the high-water mark. |

---

## Reconciliation

| Metric | scan.json | This document | Match |
|---|---|---|---|
| stage types classified | 9 | 9 | &check; |
| stages mapped | 14 | 14 | &check; |
| links mapped | 16 | 16 | &check; |
| connections | 3 | 3 | &check; |
| schemas | 7 | 7 | &check; |
| fields | 58 | 58 | &check; |
| redesign items | 3 | 3 | &check; |
