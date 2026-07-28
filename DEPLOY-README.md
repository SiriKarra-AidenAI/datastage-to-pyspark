# Deploy Bundle — JB_CUST_SALES_SCD_AGG

> DataStage → PySpark + Postgres migration  
> Generated: 2026-07-28  
> Target: Plain PySpark 3.5.1 + PostgreSQL via JDBC  

---

## 1. What's in this bundle

| File | Purpose |
|---|---|
| `Dockerfile` | Container image definition (Apache Spark 3.5.1 + Python 3 + Postgres JDBC driver) |
| `entrypoint.sh` | Startup script — validates env vars, runs `spark-submit` |
| `pyspark_emp_load.py` | The ETL job: 17 functions covering 3 sources, 7 transforms, 3 sinks |
| `requirements.txt` | Python dependencies (`pyspark==3.5.1`, `psycopg2-binary`) |
| `config/connection.properties` | JDBC connection placeholders (no real credentials) |
| `config/.env.example` | Local runtime env template |
| `data/customer_delta_2026-07-28.dat` | Sample CSV (5 customer rows, 1 with NULL) |
| `data/sales_txn_2026-07-28.dat` | Sample CSV (8 sales transaction rows) |
| `ci-cd/github-actions.yml` | GitHub Actions: build Docker image → push to ECR or ACR → deploy |
| `ci-cd/azure-pipelines.yml` | Azure DevOps: build + push to ACR → deploy to Container Apps |
| `deploy/aws-ecs-taskdef.json` | AWS ECS/Fargate task definition template |
| `deploy/azure-containerapp.yaml` | Azure Container Apps Job manifest |
| `DEPLOY-README.md` | This file |

---

## 2. Prerequisites in the target Postgres

The script provisions ALL required schema automatically in "Phase 0" (before any data read), using
`CREATE TABLE IF NOT EXISTS` and `CREATE SEQUENCE IF NOT EXISTS`. **No manual Postgres setup is needed**
on a fresh empty database — the script is self-bootstrapping.

Specifically, Phase 0 creates:

| Object | DDL |
|---|---|
| `"CUSTOMER_DIM"` | `CREATE TABLE IF NOT EXISTS ...` (11 columns, PK `"CUST_KEY"`) |
| `"SALES_FACT"` | `CREATE TABLE IF NOT EXISTS ...` (8 columns, composite PK) |
| `"SEQ_CUSTOMER_DIM_KEY"` | `CREATE SEQUENCE IF NOT EXISTS ...` (resumable surrogate key) |

**Prerequisites you MUST provide:**

- A running Postgres instance (any version ≥12).
- A database and user with `CREATE TABLE`, `CREATE SEQUENCE`, `INSERT`, `UPDATE` privileges.
- The `POSTGRES_*` environment variables pointing to that instance.

---

## 3. Build locally

```bash
cd artifacts/04-deploy/package
docker build -t datastage-pyspark-cust-sales-scd-agg .
```

---

## 4. Run locally

```bash
# Copy and fill in real values
cp config/.env.example config/.env
# Edit config/.env with your Postgres host, db, user, password

# Run (mounts data/ into /app/data for the sample CSVs)
docker run --rm \
  --env-file config/.env \
  -v $(pwd)/data:/app/data \
  datastage-pyspark-cust-sales-scd-agg
```

---

## 5. Deploy to AWS

### CI/CD (GitHub Actions)
Set these secrets/variables in your repo → Settings → Secrets and variables → Actions:

| Name | Type | Purpose |
|---|---|---|
| `DEPLOY_TARGET` | Variable | Set to `aws` |
| `AWS_ROLE_ARN` | Variable | IAM role for GitHub OIDC |
| `AWS_REGION` | Variable | e.g. `us-east-1` |
| `AWS_ACCOUNT_ID` | Variable | AWS account ID |
| `ECS_CLUSTER` | Variable | Fargate cluster name |
| `ECS_SUBNETS` | Variable | Subnet IDs (comma-separated) |
| `ECS_SECURITY_GROUPS` | Variable | Security group IDs |
| `POSTGRES_HOST` | Secret | Target Postgres host |
| `POSTGRES_DB` | Secret | Database name |
| `POSTGRES_USER` | Secret | DB user |
| `POSTGRES_PASSWORD` | Secret | DB password |
| `RUN_DATE` | Variable | Business run date (YYYY-MM-DD) |

### Manual deploy
1. Fill placeholders in `deploy/aws-ecs-taskdef.json` (`<ACCOUNT_ID>`, `<REGION>`, `<TAG>`, `<POSTGRES_*>`).
2. Register: `aws ecs register-task-definition --cli-input-json file://deploy/aws-ecs-taskdef.json`
3. Run (one-off Fargate task): `aws ecs run-task --cluster <cluster> --task-definition datastage-pyspark-cust-sales-scd-agg --launch-type FARGATE --network-configuration "awsvpcConfiguration={subnets=[...],securityGroups=[...],assignPublicIp=DISABLED}"`

---

## 6. Deploy to Azure

### CI/CD (Azure Pipelines / GitHub Actions)
Set these variables in the pipeline/library group:

| Name | Type | Purpose |
|---|---|---|
| `DEPLOY_TARGET` | Variable | Set to `azure` |
| `ACR_REGISTRY` | Variable | ACR registry name |
| `DOCKER_REGISTRY_SERVICE_CONNECTION` | Service Conn | ACR Docker registry |
| `AZURE_SERVICE_CONNECTION` | Service Conn | Azure RM |
| `CONTAINER_APP_JOB` | Variable | Container App Job name |
| `RESOURCE_GROUP` | Variable | Azure resource group |
| `POSTGRES_HOST` | Secret | Target Postgres host |
| `POSTGRES_DB` | Secret | Database name |
| `POSTGRES_USER` | Secret | DB user |
| `POSTGRES_PASSWORD` | Secret | DB password (or Key Vault ref) |
| `POSTGRES_PASSWORD_KEYVAULT_REF` | Secret | Key Vault secret URI |

### Manual deploy (CLI)
1. Fill placeholders in `deploy/azure-containerapp.yaml`.
2. Create/update: `az containerapp job create --name <job-name> --resource-group <rg> --yaml deploy/azure-containerapp.yaml`
3. Trigger a run: `az containerapp job start --name <job-name> --resource-group <rg>`

---

## 7. Required secrets/env

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `POSTGRES_HOST` | Yes | — | Postgres server hostname |
| `POSTGRES_PORT` | No | `5432` | Postgres port |
| `POSTGRES_DB` | Yes | — | Database name |
| `POSTGRES_USER` | Yes | — | Database user |
| `POSTGRES_PASSWORD` | Yes | — | Database password (**never** a decoded `{dsmenc}` value) |
| `SRC_DIR` | Yes | — | Directory containing `customer_delta_<RUN_DATE>.dat` and `sales_txn_<RUN_DATE>.dat` |
| `ERR_DIR` | Yes | — | Directory for error output (`sales_errors_<RUN_DATE>.dat`) |
| `RUN_DATE` | No | today's date | Business run date (YYYY-MM-DD) |
| `HIGH_DATE` | No | `9999-12-31` | Open-ended SCD expiry date |

### Cloud secret storage

| Cloud | Secret storage |
|---|---|
| **AWS** | `POSTGRES_PASSWORD` → AWS Secrets Manager or SSM Parameter Store (SecureString). In `aws-ecs-taskdef.json`, point `secrets[].valueFrom` to the ARN. Other env vars can be plain in the task definition's `environment` block. |
| **Azure** | `POSTGRES_PASSWORD` → Azure Key Vault (`secretRef` in Container App manifest). Other env vars can be plain `value` strings. |

---

## 8. Verify the load

After a successful run, connect to the target Postgres and verify:

```sql
-- Check SCD2 dimension (should have at least as many rows as customer delta rows)
SELECT COUNT(*) AS customer_dim_count FROM "CUSTOMER_DIM";

-- Check fact table (depends on aggregation of valid sales rows)
SELECT COUNT(*) AS sales_fact_count FROM "SALES_FACT";

-- Check error log output file
-- Any rejected transactions (missing customer ref, DQ failures) appear in:
--   <ERR_DIR>/sales_errors_<RUN_DATE>.dat
```

**Expected row counts** (from sample data):
- `"CUSTOMER_DIM"`: ≥ 5 rows (one per customer delta row; CUST001-CUST005 on first run; CUST002 had EFF_DATE update in delta so may have 2 versions). On a fresh Postgres, all 5 customers are new — expect 5 rows.
- `"SALES_FACT"`: Aggregated by CUST_KEY + PRODUCT_ID + STORE_ID + TXN_DATE. With sample data: ~6-7 rows (TXN-008 has NULL STORE_ID, excluded from aggregation grouping, so it contributes to a NULL-group row).
- `sales_errors_<RUN_DATE>.dat`: Transactions with missing customer in dimension reference (e.g., if CUSTOMER_DIM has no matching CURR_FLAG='Y' row for a given CUST_ID), plus any that fail DQ (negative AMOUNT, null TXN_DATE, etc.).
