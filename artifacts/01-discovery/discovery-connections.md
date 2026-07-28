# Discovery: Connections

> **Stage:** Data Flows (discovery / data-flows)  
> **Date:** 2026-07-28  
> **Source platform:** IBM DataStage  
> **Target platform:** Postgres PySpark  

**Total connections:** 3 (2 database, 1 file)

---

## Connection: TGT_CUSTOMER_DIM (PxDB2Connector)

| Property | Value |
|---|---|
| Kind | database |
| Target table | EDW.CUSTOMER_DIM |
| Write mode | upsert &rarr; PySpark disposition: **UNSUPPORTED** (requires merge/MERGE-INTO logic, redesign band) |
| Upsert key columns | `CUST_KEY` &mdash; **flag in redesign queue**, no direct PySpark equivalent |
| Connection (placeholder) | `#DB_CONNECTION#` &mdash; parameterized DataStage job parameter; resolve to env vars at build time: `POSTGRES_HOST` / `POSTGRES_PORT` / `POSTGRES_USER` / `POSTGRES_PASSWORD` |
| PySpark methods | `ensure_table_exists()`, `write_postgres()` |
| Password | `env: POSTGRES_PASSWORD` (never store or decode any encrypted credential) |

### Postgres DDL

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
PRIMARY KEY ("CUST_KEY")
)
```

*DDL derived from `TableDefinitions DB2\CUSTOMER_DIM` - no explicit CreateStatement in source.*

---

## Connection: TGT_SALES_FACT (PxDB2Connector)

| Property | Value |
|---|---|
| Kind | database |
| Target table | EDW.SALES_FACT |
| Write mode | insert &rarr; PySpark disposition: `.mode("append")` |
| Upsert key columns | (none) |
| Connection (placeholder) | `#DB_CONNECTION#` &mdash; parameterized DataStage job parameter; resolve to env vars at build time: `POSTGRES_HOST` / `POSTGRES_PORT` / `POSTGRES_USER` / `POSTGRES_PASSWORD` |
| PySpark methods | `ensure_table_exists()`, `write_postgres()` |
| Password | `env: POSTGRES_PASSWORD` (never store or decode any encrypted credential) |

### Postgres DDL

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
PRIMARY KEY ("CUST_KEY", "PRODUCT_ID", "STORE_ID", "TXN_DATE")
)
```

*DDL derived from `TableDefinitions DB2\SALES_FACT` - no explicit CreateStatement in source.*

---

## Connection: TGT_ERROR_LOG (PxSequentialFile)

| Property | Value |
|---|---|
| Kind | file |
| Target file | `#ERR_DIR#/sales_errors_#RUN_DATE#.dat` |
| Write mode | overwrite &rarr; PySpark disposition: `.mode("overwrite")` |
| PySpark methods | `write_output()` |
| Password | N/A (file sink) |

### Schema (from TableDefinitions SEQ\SALES_ERRORS)

| Field | DataStage type | Spark type |
|---|---|---|
| TXN_ID | VARCHAR(25) | StringType() |
| CUST_ID | VARCHAR(20) | StringType() |
| ERROR_REASON | VARCHAR(50) | StringType() |
| ERROR_TIMESTAMP | TIMESTAMP | TimestampType() |
| RUN_DATE | DATE | DateType() |

---

## Reconciliation

| Metric | scan.json | This document | Match |
|---|---|---|---|
| connections | 3 | 3 | &check; |
| database sinks | 2 | 2 | &check; |
| file sinks | 1 | 1 | &check; |
