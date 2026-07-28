# Build Report — JB_CUST_SALES_SCD_AGG

> **Stage:** Generate PySpark Script (build / build-script)  
> **Date:** 2026-07-28  
> **Script:** `pyspark_emp_load.py`  

---

## 1. Facts driving the build

| Fact source | Key data used |
|---|---|
| `scan.json` stages | 14 stage definitions with op/role/pyspark_methods |
| `scan.json` links | 16 DAG edges with link_type Reject annotation |
| `scan.json` schemas | 7 schemas, 58 fields with resolved spark_types |
| `scan.json` connections | 3 sinks: CUSTOMER_DIM (upsert), SALES_FACT (insert), error log (overwrite) |
| `scan.json` table_definitions | 5 table records: CUSTOMER_DELTA, SALES_TXN, CUSTOMER_DIM, SALES_FACT, SALES_ERRORS |
| `get_source_params` | CSV read params (delimiter, header, nullValue, encoding, dateFormat) + JDBC select_statement + surrogate key sequence |
| `get_global_formats` | DataStage `%yyyy-%mm-%dd` → Spark `yyyy-MM-dd` |
| `DATASTAGE-TO-PYSPARK-MAPPING.md` | Stage bands, node→method mapping, write-mode disposition |
| `ESTATE-TARGET-MAP.md` | Function topology + main() call order |

## 2. Field-to-type mapping used for schemas

| DataStage type | Spark StructField | Postgres DDL | Used in schema |
|---|---|---|---|
| VARCHAR(1) | `StringType()` | `VARCHAR(1)` | CURR_FLAG |
| VARCHAR(10) | `StringType()` | `VARCHAR(10)` | ZIP |
| VARCHAR(15) | `StringType()` | `VARCHAR(15)` | SOURCE_SYSTEM, STORE_ID, CHANNEL |
| VARCHAR(20) | `StringType()` | `VARCHAR(20)` | CUST_ID, PRODUCT_ID, STATE, SEGMENT |
| VARCHAR(25) | `StringType()` | `VARCHAR(25)` | TXN_ID |
| VARCHAR(50) | `StringType()` | `VARCHAR(50)` | ERROR_REASON |
| VARCHAR(60) | `StringType()` | `VARCHAR(60)` | CITY |
| VARCHAR(100) | `StringType()` | `VARCHAR(100)` | CUST_NAME |
| VARCHAR(150) | `StringType()` | `VARCHAR(150)` | ADDRESS |
| INTEGER | `IntegerType()` | `INTEGER` | CUST_KEY, EXISTING_CUST_KEY, CHANGE_CODE, QTY, TOTAL_QTY, TXN_COUNT |
| DATE | `DateType()` | `DATE` | EFF_DATE, EXP_DATE, TXN_DATE, RUN_DATE |
| TIMESTAMP | `TimestampType()` | `TIMESTAMP` | ERROR_TIMESTAMP |
| DECIMAL(12,2) | `DecimalType(12,2)` | `NUMERIC(12,2)` | UNIT_PRICE, AVG_UNIT_PRICE |
| DECIMAL(14,2) | `DecimalType(14,2)` | `NUMERIC(14,2)` | AMOUNT |
| DECIMAL(15,2) | `DecimalType(15,2)` | `NUMERIC(15,2)` | TOTAL_AMOUNT |

## 3. DDL translation

### CUSTOMER_DIM
**Source:** `TableDefinitions DB2\CUSTOMER_DIM` (11 columns, PK CUST_KEY).  
**Translation:** All identifiers double-quoted to match Spark PostgresDialect INSERT quoting. `INTEGER` → `INTEGER`, `VARCHAR` → `VARCHAR(n)`, `DATE` → `DATE`. `Nullable=0` → `NOT NULL`.  
**Output:** `CREATE TABLE IF NOT EXISTS "CUSTOMER_DIM" (...) PRIMARY KEY ("CUST_KEY")`.

### SALES_FACT
**Source:** `TableDefinitions DB2\SALES_FACT` (8 columns, composite PK on 4 columns).  
**Translation:** Same rules. `NUMERIC` preserves precision/scale.  
**Output:** `CREATE TABLE IF NOT EXISTS "SALES_FACT" (...) PRIMARY KEY ("CUST_KEY", "PRODUCT_ID", "STORE_ID", "TXN_DATE")`.

### SEQ_CUSTOMER_DIM_KEY
**Source:** `GEN_SURR_KEY` stage params (`KeySource: Db2Sequence`, `SequenceName: EDW.SEQ_CUSTOMER_DIM_KEY`).  
**Provisioning:** `CREATE SEQUENCE IF NOT EXISTS "SEQ_CUSTOMER_DIM_KEY"` — idempotent, runs before any read.

## 4. Write-mode decisions

| Target | DataStage mode | PySpark mode | Disposition | Rationale |
|---|---|---|---|---|
| CUSTOMER_DIM | upsert | manual MERGE | redesign | No direct PySpark equivalent. Phase A: UPDATE existing rows via psycopg2. Phase B: INSERT new rows via JDBC append. |
| SALES_FACT | insert | `.mode("append")` | direct | 1:1 mapping from DataStage insert. |
| ERROR_LOG | overwrite | `.mode("overwrite")` | direct | CSV file overwrite (not JDBC, truncate not applicable). |

## 5. Redesign items handled

### TGT_CUSTOMER_DIM — SCD2 upsert (MERGE)
- **Problem:** DataStage `write_mode=upsert` has no direct PySpark `.mode()`.
- **Solution:** Two-phase approach:
  - **Phase A (EXPIRE):** `psycopg2` UPDATE on existing rows matching `EXISTING_CUST_KEY`, setting `CURR_FLAG='N'` and `EXP_DATE=RUN_DATE-1`.
  - **Phase B (INSERT):** JDBC `.mode("append")` for new version rows with fresh surrogate keys from `SEQ_CUSTOMER_DIM_KEY`.

### Surrogate key uniqueness
- **Problem:** `F.lit(single_value)` broadcasts one value to all rows, violating PK when >1 row needs a new key.
- **Solution:** Fetch ONE `nextval()` base value from Postgres SEQUENCE, then assign per-row offsets via `Window.orderBy("CUST_ID")` + `F.row_number()` ONCE across the combined Insert+Edit set (never independently per sub-group). Each row receives `base_key + row_number - 1`.

### Schema qualifier consistency
- **Problem:** Source `select_statement` has `FROM EDW.CUSTOMER_DIM` but DDL creates table without schema qualifier.
- **Solution:** Stripped `EDW.` prefix from SELECT; table is `"CUSTOMER_DIM"` in both DDL and the read query.

### DDL identifier quoting
- **Problem:** Unquoted DDL column names are case-folded by Postgres (`cust_key`), but Spark's `PostgresDialect` writes quoted (`"CUST_KEY"`) — mismatch causes "column does not exist" error.
- **Solution:** Every table name and column name in DDL is double-quoted, matching Spark's JDBC INSERT quoting exactly.

### Join key deduplication
- **Problem:** Explicit Column-condition join (`df1.col == df2.col`) keeps both sides' copies of the key column; unqualified references later raise `AnalysisException: ambiguous reference`.
- **Solution:** All joins use the string form (`on="CUST_ID"`) so Spark automatically deduplicates shared-key columns.

## 6. Script structure

| Section | Lines | Content |
|---|---|---|
| CONFIG | 22-37 | `os.environ` resolution, JDBC URL build |
| Schemas | 39-77 | `StructType` definitions + DDL + SEQUENCE DDL |
| Phase 0 | 108-147 | `ensure_*_table_exists()` + `ensure_sequence_exists()` |
| Source reads | 149-218 | 3 read functions (2 CSV + 1 JDBC) |
| Surrogate key | 220-239 | DB sequence `nextval()` |
| Transform layer | 241-440 | Lookup, DQ, ChangeCapture, SCD Apply, Sort, Aggregate, Funnel |
| Write layer | 442-548 | MERGE/upsert, JDBC append, CSV overwrite |
| Main | 550-599 | Phase 0→1→2→3→4→5 orchestration |

## 7. Pre-submit verification

| Check | Result |
|---|---|
| `py_compile` (syntax) | PASS |
| SQL string literal `'Y'` quoting | PASS |
| No `EDW.` schema qualifier in SELECT | PASS |
| `CREATE SEQUENCE` present | PASS |
| Phase 0 DDL before any read | PASS |
| All DDL identifiers double-quoted | PASS |
| Surrogate key per-row uniqueness | PASS (base + row_number across combined set) |
| No `.mode("overwrite")` on JDBC without truncate | PASS (only CSV write uses overwrite) |
| Join via string form `on="CUST_ID"` | PASS |
