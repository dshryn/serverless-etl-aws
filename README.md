# Serverless ETL with AWS


## Overview

A cost-conscious, production-ready, serverless ETL pipeline that receives nested JSON files in S3, transforms and stores them as partitioned Parquet using AWS Lambda, publishes metadata to the AWS Glue Data Catalog (without a Glue Crawler), and serves analytical queries through Amazon Athena for consumption by Power BI.


## Design Principles

- **Serverless-first**: Used managed services (S3, Lambda, Glue, Athena) to avoid server maintenance
- **Cost-aware**: Used Parquet + partitioning to minimize Athena data scanned
- **Console-operable**: All operations via AWS Management Console, no CLI required
  

## Architecture & Data Flow

1. **Ingest**- Upload nested JSON file to S3 under `raw/`. Find JSON file named `airlines.json` at repo root
2. **Trigger**- S3 PUT event triggers Lambda function
3. **Transform**- Lambda:
   - Reads JSON (array or NDJSON)
   - Flattens nested fields into relational columns
   - Splits rows by partition keys (year, month)
   - Writes Parquet files to `processed/parquet/year=YYYY/month=MM/`
   - Ensures Glue table exists
   - Registers partitions in Glue Data Catalog
4. **Catalog**- Glue stores table schema and partition metadata
5. **Query**- Athena executes SQL with partition pruning
6. **Consume**- Receive data on PowerBI through ODBC Driver


## Data Model & Partitioning

**Flattened Schema:**
- `airport_code`, `airport_name`, `time_label`, `month_name`
- `flights_total`, `flights_delayed`, `minutes_total`, `minutes_weather`
- `carriers_names` (comma-separated), `carriers_total`

**Partitions:**
- `year` (int) and `month` (int)
- File path: `processed/parquet/year=YYYY/month=MM/x.parquet`


## Operational Setup (Console-Centric)

### S3 Setup
1. Create S3 bucket (e.g., `sl-etl-dshryn`)
2. Create prefixes:
   - `raw/` - JSON upload location (trigger)
   - `processed/parquet/` - Parquet destination
   - `processed/athena-query-results/` - Athena results

### Lambda Setup
1. Create Lambda function (Python 3.x)
2. Upload ETL script from `lambda/` folder
3. Set environment variables:
   - `GLUE_DATABASE = airport_db`
   - `GLUE_TABLE = airport_delays_parquet`
   - `PROCESSED_PREFIX = processed/parquet/`
4. Configure IAM role with permissions for:
   - S3 read/write to bucket
   - Glue table/partition operations
   - CloudWatch Logs
5. Add S3 event notification on PUT to `raw/`

### Glue Setup
1. Create Glue Database: `airport_db`
2. Create table manually:
   - Path: `s3://sl-etl-dshryn/processed/parquet/`
   - Format: Parquet
   - Add columns matching flattened schema
   - Partition keys: `year` (int), `month` (int)

### Athena Setup
1. Set Query result location
2. Create views from SQL files in `athena/` folder

### Power BI Setup
1. Use ODBC driver to connect Athena
2. Set region matching AWS deployment
3. Set S3 Output Location
4. Authenticate with IAM user/role with minimal permissions

## Testing & Verification

1. **Upload sample**: Put `airlines.json` to `raw/`
2. **Check Lambda**: Review CloudWatch Logs for:
   - `Wrote parquet to s3://...`
   - `Registered partition ['YYYY', 'MM']`
3. **Verify S3**: Confirm Parquet files in processed location
4. **Verify Glue**: Check partitions in Glue Console
5. **Verify Athena**:
   ```sql
   SHOW PARTITIONS airport_delays_parquet;
   SELECT COUNT(*) FROM airport_delays_parquet WHERE year = 2003 AND month = 6
   ```
6. **Verify Power BI**: Connect through ODBC driver, setup credentials and import views


## Troubleshooting

- Handler not found: Check Lambda Handler matches file/function name
- Missing pandas/pyarrow: Use Lambda Layer or container image with dependencies
- AccessDenied on Glue: FIx IAM role with Glue permissions
- Partitions missing: Run `MSCK REPAIR TABLE` or register via Lambda
- Athena scans too much data: Always filter by year/month, use CTAS

## Pipeline Reset (Console-Only)

1. **Glue**: Delete partitions or entire table
2. **S3**: Delete `processed/parquet/` folder  
3. **S3**: Clear `processed/athena-query-results/`
4. Upload JSON to `raw/` to re-trigger Lambda

## FAQ

**Do I need a Glue Crawler?**  
No. Lambda registers partitions directly.

**What if JSON contains multiple years/months?**  
Lambda splits records by (year, month) and writes separate Parquet files.

**Will Power BI be charged by Athena?**  
Yes, Power BI triggers Athena queries charged by bytes scanned. Use partitioning and CTAS to minimize costs.


## Change Log

**v1.0**- Pipeline implemented: Lambda ETL, Glue table creation, Athena views, Power BI integration