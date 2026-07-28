# Migration Report — JB_CUST_SALES_SCD_AGG

> **Stage:** Generate PySpark Script (build / build-script)  
> **Date:** 2026-07-28  
> **Source:** IBM DataStage 11.7.1  
> **Target:** Plain PySpark + Postgres JDBC  

---

## Headline

| Metric | Value |
|---|---|
| Components built | 17 PySpark functions |
| Avg confidence | 75% |
| 🟢 verify (auto) | 22 of 30 fileMapping rows |
| 🟡 review (assisted) | 7 of 30 fileMapping rows |
| 🔴 manual (redesign) | 1 of 30 fileMapping rows |

---

## Component mapping table

| Source | Type | PySpark target | Disposition | Tier | Confidence |
|---|---|---|---|---|---|
| STG_SRC_CUSTOMER_DELTA | PxSequentialFile | `read_customer_delta()` | auto | verify | 95% |
| STG_SRC_SALES_TXN | PxSequentialFile | `read_sales_txn()` | auto | verify | 95% |
| STG_REF_CUST_DIM_CURRENT | PxDB2Connector | `read_customer_dim_current()` | auto | verify | 88% |
| GEN_SURR_KEY | PxSurrogateKeyGenerator | `get_next_cust_key_base()` | assisted | review | 68% |
| LKP_CUSTOMER_ENRICH | PxLookup | `apply_customer_enrichment()` | assisted | review | 72% |
| XFM_DQ_VALIDATION | PxTransformer | `apply_dq_validation()` | assisted | review | 70% |
| CHG_CUST_SCD_COMPARE | PxChangeCapture | `apply_change_capture()` | assisted | review | 65% |
| XFM_SCD_APPLY | PxTransformer | `apply_scd2_rules()` | assisted | review | 70% |
| SRT_SALES_ENRICHED | PxSort | `sort_for_aggregation()` | auto | verify | 92% |
| AGG_SALES_SUMMARY | PxAggregator | `aggregate_sales()` | assisted | review | 75% |
| FUN_ERROR_FUNNEL | PxFunnel | `funnel_errors()` | auto | verify | 88% |
| TGT_CUSTOMER_DIM | PxDB2Connector | `write_customer_dim()` | **manual** | **manual** | 50% |
| TGT_SALES_FACT | PxDB2Connector | `write_sales_fact()` | auto | verify | 88% |
| TGT_ERROR_LOG | PxSequentialFile | `write_error_log()` | auto | verify | 95% |

Plus 16 link-level DataFrame passes (all auto/verify, 90% confidence each).

---

## Per-construct detail

### Stage mappings

| Stage | Method(s) | What the generated code does |
|---|---|---|
| STG_SRC_CUSTOMER_DELTA | `read_source()` | `spark.read.csv` with schema `CUST_DELTA_SCHEMA`, delimiter `,`, header, nullValue `NULL`, dateFormat `yyyy-MM-dd` |
| STG_SRC_SALES_TXN | `read_source()` | `spark.read.csv` with schema `SALES_TXN_SCHEMA`, same CSV options |
| STG_REF_CUST_DIM_CURRENT | `read_reference()` | `spark.read.jdbc` with subquery `SELECT ... FROM "CUSTOMER_DIM" WHERE CURR_FLAG = 'Y'` |
| GEN_SURR_KEY | `generate_surrogate_key()` | `psycopg2` fetches `nextval('"SEQ_CUSTOMER_DIM_KEY"')` → base key; `row_number()` over `Window.orderBy("CUST_ID")` across combined Insert+Edit set |
| LKP_CUSTOMER_ENRICH | `apply_lookup()` | Broadcast left join on `CUST_ID`; unmatched → reject with `ERROR_REASON` |
| XFM_DQ_VALIDATION | `apply_transformations()` | Pass filter (CUST_ID not null, AMOUNT>0, QTY>0, TXN_DATE not null); reject with derived `ERROR_REASON` |
| CHG_CUST_SCD_COMPARE | `apply_change_capture()` | Full-outer join on `CUST_ID` → `CHANGE_CODE` (0=Copy dropped, 1=Insert, 3=Edit); carries `EXISTING_CUST_KEY` |
| XFM_SCD_APPLY | `apply_transformations()` | Expire rows (CHANGE_CODE=3): `CURR_FLAG='N'`, `EXP_DATE=RUN_DATE-1`. New version rows (CHANGE_CODE=1 or 3): unique key = `base_key + row_number - 1`, `CURR_FLAG='Y'`, `EFF_DATE=RUN_DATE`, `EXP_DATE=HIGH_DATE` |
| SRT_SALES_ENRICHED | `apply_transformations()` | `.orderBy(CUST_KEY.asc, PRODUCT_ID.asc, STORE_ID.asc)` |
| AGG_SALES_SUMMARY | `apply_transformations()` | `.groupBy(CUST_KEY, PRODUCT_ID, STORE_ID, TXN_DATE).agg(SUM QTY, SUM AMOUNT, COUNT TXN_ID, AVG UNIT_PRICE)` |
| FUN_ERROR_FUNNEL | `apply_transformations()` | `.unionByName()` of both reject streams; adds `ERROR_TIMESTAMP=current_timestamp()`, `RUN_DATE` |
| TGT_CUSTOMER_DIM (sink) | `ensure_table_exists()` | DDL: `CREATE TABLE IF NOT EXISTS "CUSTOMER_DIM"` (11 cols, PK `"CUST_KEY"`), all identifiers double-quoted |
| | `write_postgres()` | **Manual redesign:** Phase A: `psycopg2` UPDATE expire. Phase B: JDBC `.mode("append")` insert new versions |
| TGT_SALES_FACT (sink) | `ensure_table_exists()` | DDL: `CREATE TABLE IF NOT EXISTS "SALES_FACT"` (8 cols, composite PK) |
| | `write_postgres()` | JDBC `.mode("append")` — direct append |
| TGT_ERROR_LOG (sink) | `write_output()` | `spark.write.csv` `.mode("overwrite")` to `sales_errors_<RUN_DATE>.dat` |

### Link-level DataFrame passes

All 16 links are DataFrame transformations — source stages produce DataFrames, transform stages consume/produce DataFrames, sink stages write DataFrames. The `lnk_Enrich_Reject` reject link is handled inside `apply_customer_enrichment()` as a filter-and-select on unmatched rows.

---

## Type mapping summary

| Category | Types |
|---|---|
| String types | `VARCHAR(1..150)` → `StringType()` → `VARCHAR(n)` (7 variants) |
| Integer types | `INTEGER` → `IntegerType()` → `INTEGER` |
| Date types | `DATE` → `DateType()` → `DATE` |
| Timestamp types | `TIMESTAMP` → `TimestampType()` → `TIMESTAMP` |
| Decimal types | `DECIMAL(p,s)` → `DecimalType(p,s)` → `NUMERIC(p,s)` (3 variants: 12,2 / 14,2 / 15,2) |

---

## Write-mode summary

| Target | DataStage | PySpark | Disposition |
|---|---|---|---|
| CUSTOMER_DIM | `upsert` | manual MERGE | redesign |
| SALES_FACT | `insert` | `.mode("append")` | direct |
| ERROR_LOG | `overwrite` | `.mode("overwrite")` (CSV) | direct |

---

## DDL summary

Two tables + one sequence provisioned idempotently in Phase 0:

- `"CUSTOMER_DIM"` — 11 columns, PK `"CUST_KEY"`
- `"SALES_FACT"` — 8 columns, composite PK `("CUST_KEY", "PRODUCT_ID", "STORE_ID", "TXN_DATE")`
- `"SEQ_CUSTOMER_DIM_KEY"` — Postgres SEQUENCE for resumable surrogate keys

All identifiers double-quoted to match Spark `PostgresDialect` INSERT quoting, preventing Postgres case-folding mismatch.
