# Discovery: Schema Catalog

> **Stage:** Data Flows (discovery / data-flows)  
> **Date:** 2026-07-28  
> **Source platform:** IBM DataStage  
> **Target platform:** Postgres PySpark  

**Total schemas:** 7 &emsp; **Total fields:** 58

---

## Schema: lnk_CustDelta_Out (STG_SRC_CUSTOMER_DELTA &rarr; LKP_CUSTOMER_ENRICH)

| Field | DataStage type | Spark type | Postgres DDL | Nullable | PK |
|---|---|---|---|---|---|
| CUST_ID | VARCHAR | StringType() | VARCHAR(20) | no | **yes** |
| CUST_NAME | VARCHAR | StringType() | VARCHAR(100) | no | no |
| ADDRESS | VARCHAR | StringType() | VARCHAR(150) | yes | no |
| CITY | VARCHAR | StringType() | VARCHAR(60) | yes | no |
| STATE | VARCHAR | StringType() | VARCHAR(20) | yes | no |
| ZIP | VARCHAR | StringType() | VARCHAR(10) | yes | no |
| SEGMENT | VARCHAR | StringType() | VARCHAR(20) | yes | no |
| SOURCE_SYSTEM | VARCHAR | StringType() | VARCHAR(15) | yes | no |
| EFF_DATE | DATE | DateType() | DATE | no | no |

**Fields:** 9

---

## Schema: lnk_SalesTxn_Out (STG_SRC_SALES_TXN &rarr; LKP_CUSTOMER_ENRICH)

| Field | DataStage type | Spark type | Postgres DDL | Nullable | PK |
|---|---|---|---|---|---|
| TXN_ID | VARCHAR | StringType() | VARCHAR(25) | no | **yes** |
| CUST_ID | VARCHAR | StringType() | VARCHAR(20) | no | no |
| PRODUCT_ID | VARCHAR | StringType() | VARCHAR(20) | no | no |
| STORE_ID | VARCHAR | StringType() | VARCHAR(15) | yes | no |
| CHANNEL | VARCHAR | StringType() | VARCHAR(15) | yes | no |
| TXN_DATE | DATE | DateType() | DATE | no | no |
| QTY | INTEGER | IntegerType() | INTEGER | no | no |
| UNIT_PRICE | DECIMAL(12,2) | DecimalType(12,2) | NUMERIC(12,2) | no | no |
| AMOUNT | DECIMAL(14,2) | DecimalType(14,2) | NUMERIC(14,2) | no | no |

**Fields:** 9

---

## Schema: lnk_CustDimCurrent_Out (STG_REF_CUST_DIM_CURRENT &rarr; LKP_CUSTOMER_ENRICH / CHG_CUST_SCD_COMPARE)

| Field | DataStage type | Spark type | Postgres DDL | Nullable | PK |
|---|---|---|---|---|---|
| CUST_KEY | INTEGER | IntegerType() | INTEGER | no | **yes** |
| CUST_ID | VARCHAR | StringType() | VARCHAR(20) | no | no |
| CUST_NAME | VARCHAR | StringType() | VARCHAR(100) | no | no |
| ADDRESS | VARCHAR | StringType() | VARCHAR(150) | yes | no |
| CITY | VARCHAR | StringType() | VARCHAR(60) | yes | no |
| STATE | VARCHAR | StringType() | VARCHAR(20) | yes | no |
| ZIP | VARCHAR | StringType() | VARCHAR(10) | yes | no |
| SEGMENT | VARCHAR | StringType() | VARCHAR(20) | yes | no |
| EFF_DATE | DATE | DateType() | DATE | no | no |
| EXP_DATE | DATE | DateType() | DATE | no | no |
| CURR_FLAG | VARCHAR | StringType() | VARCHAR(1) | no | no |

**Fields:** 11

---

## Schema: lnk_Enriched_Out (LKP_CUSTOMER_ENRICH &rarr; XFM_DQ_VALIDATION)

| Field | DataStage type | Spark type | Postgres DDL | Nullable | PK |
|---|---|---|---|---|---|
| TXN_ID | VARCHAR | StringType() | VARCHAR(25) | yes | no |
| CUST_ID | VARCHAR | StringType() | VARCHAR(20) | yes | no |
| CUST_KEY | INTEGER | IntegerType() | INTEGER | yes | no |
| SEGMENT | VARCHAR | StringType() | VARCHAR(20) | yes | no |
| STATE | VARCHAR | StringType() | VARCHAR(20) | yes | no |
| PRODUCT_ID | VARCHAR | StringType() | VARCHAR(20) | yes | no |
| STORE_ID | VARCHAR | StringType() | VARCHAR(15) | yes | no |
| CHANNEL | VARCHAR | StringType() | VARCHAR(15) | yes | no |
| TXN_DATE | DATE | DateType() | DATE | yes | no |
| QTY | INTEGER | IntegerType() | INTEGER | yes | no |
| UNIT_PRICE | DECIMAL(12,2) | DecimalType(12,2) | NUMERIC(12,2) | yes | no |
| AMOUNT | DECIMAL(14,2) | DecimalType(14,2) | NUMERIC(14,2) | yes | no |

**Fields:** 12

---

## Schema: lnk_Enrich_Reject (LKP_CUSTOMER_ENRICH &rarr; FUN_ERROR_FUNNEL &mdash; Reject)

| Field | DataStage type | Spark type | Postgres DDL | Nullable | PK |
|---|---|---|---|---|---|
| TXN_ID | VARCHAR | StringType() | VARCHAR(25) | yes | no |
| CUST_ID | VARCHAR | StringType() | VARCHAR(20) | yes | no |
| ERROR_REASON | VARCHAR | StringType() | VARCHAR(50) | yes | no |

**Fields:** 3

---

## Schema: lnk_DQ_Reject (XFM_DQ_VALIDATION &rarr; FUN_ERROR_FUNNEL &mdash; DQ Reject)

| Field | DataStage type | Spark type | Postgres DDL | Nullable | PK |
|---|---|---|---|---|---|
| TXN_ID | VARCHAR | StringType() | VARCHAR(25) | yes | no |
| CUST_ID | VARCHAR | StringType() | VARCHAR(20) | yes | no |
| ERROR_REASON | VARCHAR | StringType() | VARCHAR(50) | yes | no |

**Fields:** 3

---

## Schema: lnk_ChangeOut (CHG_CUST_SCD_COMPARE &rarr; XFM_SCD_APPLY)

| Field | DataStage type | Spark type | Postgres DDL | Nullable | PK |
|---|---|---|---|---|---|
| CHANGE_CODE | INTEGER | IntegerType() | INTEGER | yes | no |
| CUST_ID | VARCHAR | StringType() | VARCHAR(20) | yes | no |
| CUST_NAME | VARCHAR | StringType() | VARCHAR(100) | yes | no |
| ADDRESS | VARCHAR | StringType() | VARCHAR(150) | yes | no |
| CITY | VARCHAR | StringType() | VARCHAR(60) | yes | no |
| STATE | VARCHAR | StringType() | VARCHAR(20) | yes | no |
| ZIP | VARCHAR | StringType() | VARCHAR(10) | yes | no |
| SEGMENT | VARCHAR | StringType() | VARCHAR(20) | yes | no |
| EXISTING_CUST_KEY | INTEGER | IntegerType() | INTEGER | yes | no |

**Fields:** 11 (CHANGE_CODE has precision 1, EXISTING_CUST_KEY has precision 10) &mdash; total = 9 + 9 + 11 + 12 + 3 + 3 + 11 = **58** &check;
