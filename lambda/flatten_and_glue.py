# lambda_function.py
import json
import boto3
import io
import pandas as pd
from datetime import datetime, timezone
import os
import traceback

S3 = boto3.client("s3")
GLUE = boto3.client("glue")

GLUE_DATABASE = os.environ.get("GLUE_DATABASE", "airport_db")
GLUE_TABLE = os.environ.get("GLUE_TABLE", "airport_delays_parquet")
PROCESSED_PREFIX = os.environ.get("PROCESSED_PREFIX", "processed/parquet/")
PARTITION_KEYS = [{"Name": "year", "Type": "int"}, {"Name": "month", "Type": "int"}]

def flatten_records(data):
    rows = []
    for rec in data:
        airport = rec.get("Airport", {}) or {}
        time = rec.get("Time", {}) or {}
        stats = rec.get("Statistics", {}) or {}

        delays = stats.get("# of Delays", {}) or {}
        carriers_info = stats.get("Carriers", {}) or {}
        flights = stats.get("Flights", {}) or {}
        minutes = stats.get("Minutes Delayed", {}) or {}

        # numeric to ints if possible else None
        def to_int(v):
            try:
                return int(v) if v is not None else None
            except Exception:
                return None

        row = {
            "airport_code": airport.get("Code"),
            "airport_name": airport.get("Name"),
            "time_label": time.get("Label"),
            "year": to_int(time.get("Year")),
            "month": to_int(time.get("Month")),
            "month_name": time.get("Month Name"),

            # delays
            "delay_carrier_count": to_int(delays.get("Carrier")),
            "delay_late_aircraft_count": to_int(delays.get("Late Aircraft")),
            "delay_national_system_count": to_int(delays.get("National Aviation System")),
            "delay_security_count": to_int(delays.get("Security")),
            "delay_weather_count": to_int(delays.get("Weather")),

            # carriers
            "carriers_names": carriers_info.get("Names"),
            "carriers_total": to_int(carriers_info.get("Total")),

            # flights
            "flights_cancelled": to_int(flights.get("Cancelled")),
            "flights_delayed": to_int(flights.get("Delayed")),
            "flights_diverted": to_int(flights.get("Diverted")),
            "flights_on_time": to_int(flights.get("On Time")),
            "flights_total": to_int(flights.get("Total")),

            # minutes
            "minutes_carrier": to_int(minutes.get("Carrier")),
            "minutes_late_aircraft": to_int(minutes.get("Late Aircraft")),
            "minutes_national_system": to_int(minutes.get("National Aviation System")),
            "minutes_security": to_int(minutes.get("Security")),
            "minutes_total": to_int(minutes.get("Total")),
            "minutes_weather": to_int(minutes.get("Weather")),
        }
        rows.append(row)
    df = pd.DataFrame(rows)
    return df

def ensure_glue_table(bucket_name, table_location_s3):

    try:
        GLUE.get_table(DatabaseName=GLUE_DATABASE, Name=GLUE_TABLE)
        return
    except GLUE.exceptions.EntityNotFoundException:
        pass

    sd = {
        "Columns": [
            {"Name": "airport_code", "Type": "string"},
            {"Name": "airport_name", "Type": "string"},
            {"Name": "time_label", "Type": "string"},
            {"Name": "month_name", "Type": "string"},
            {"Name": "delay_carrier_count", "Type": "int"},
            {"Name": "delay_late_aircraft_count", "Type": "int"},
            {"Name": "delay_national_system_count", "Type": "int"},
            {"Name": "delay_security_count", "Type": "int"},
            {"Name": "delay_weather_count", "Type": "int"},
            {"Name": "carriers_names", "Type": "string"},
            {"Name": "carriers_total", "Type": "int"},
            {"Name": "flights_cancelled", "Type": "int"},
            {"Name": "flights_delayed", "Type": "int"},
            {"Name": "flights_diverted", "Type": "int"},
            {"Name": "flights_on_time", "Type": "int"},
            {"Name": "flights_total", "Type": "int"},
            {"Name": "minutes_carrier", "Type": "bigint"},
            {"Name": "minutes_late_aircraft", "Type": "bigint"},
            {"Name": "minutes_national_system", "Type": "bigint"},
            {"Name": "minutes_security", "Type": "bigint"},
            {"Name": "minutes_total", "Type": "bigint"},
            {"Name": "minutes_weather", "Type": "bigint"}
        ],
        "Location": table_location_s3,
        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        "SerdeInfo": {
            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            "Parameters": {}
        },
        "Parameters": {}
    }

    table_input = {
        "Name": GLUE_TABLE,
        "Description": "Airport delays stored as Parquet partitioned by year/month",
        "TableType": "EXTERNAL_TABLE",
        "PartitionKeys": PARTITION_KEYS,
        "StorageDescriptor": sd,
        "Parameters": {"classification": "parquet"}
    }

    GLUE.create_table(DatabaseName=GLUE_DATABASE, TableInput=table_input)
    print(f"Created Glue table {GLUE_TABLE} in DB {GLUE_DATABASE}")

def partition_exists(db, table, values):
    try:
        resp = GLUE.get_partition(DatabaseName=db, TableName=table, PartitionValues=values)
        return True
    except GLUE.exceptions.EntityNotFoundException:
        return False
    except Exception as e:
        
        print("partition_exists check error:", e)

        # other errors, attempt to create and batch_create_partition raises if needed
        return False

def register_partition_safe(bucket_name, year, month, partition_s3_prefix):

    # register partition if missing
    partition_values = [str(int(year)), str(int(month))]
    if partition_exists(GLUE_DATABASE, GLUE_TABLE, partition_values):
        print(f"Partition {partition_values} already exists. Skipping registration.")
        return

    partition_input = {
        "Values": partition_values,
        "StorageDescriptor": {
            "Columns": [],  # glue will take from table
            "Location": partition_s3_prefix,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {}
            },
            "Parameters": {}
        },
        "Parameters": {}
    }

    try:
        GLUE.batch_create_partition(
            DatabaseName=GLUE_DATABASE,
            TableName=GLUE_TABLE,
            PartitionInputList=[partition_input]
        )
        print(f"Registered partition {partition_values} -> {partition_s3_prefix}")
    except Exception as e:
        # partition created concurrently, ignore
        print("Error creating partition (may already exist):", e)

def handler(event, context):
    try:
        record = event['Records'][0]
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        obj = S3.get_object(Bucket=bucket, Key=key)
        raw = obj['Body'].read().decode("utf-8")

        # parse
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                data = [data]
        except Exception:
            # fallback
            lines = [l for l in raw.splitlines() if l.strip()]
            data = [json.loads(l) for l in lines]

        df = flatten_records(data)

        if df.empty:
            return {"statusCode": 200, "body": json.dumps({"message": "No records to process"})}

        # validate that year/month exist for each row
        valid = df.dropna(subset=["year", "month"])
        if valid.empty:
            return {"statusCode": 400, "body": json.dumps({"message": "No valid year/month partitions in data"})}

        # ensure glue table exists (root loc)
        table_location_s3 = f"s3://{bucket}/{PROCESSED_PREFIX}"
        ensure_glue_table(bucket, table_location_s3)

        results = []
        # group by partition year, month and write separate parquet for each
        grouped = valid.groupby(["year", "month"])
        for (year, month), group in grouped:
            # create parquet buffer
            parquet_buffer = io.BytesIO()
            group = group.drop(columns=[])
            group.to_parquet(parquet_buffer, index=False, engine="pyarrow", compression="snappy")

            partition_prefix = f"{PROCESSED_PREFIX}year={int(year):04d}/month={int(month):02d}/"
            ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
            out_key = f"{partition_prefix}airport_delays_{ts}.parquet"

            # upload parquet to s3
            S3.put_object(Bucket=bucket, Key=out_key, Body=parquet_buffer.getvalue())
            s3_path = f"s3://{bucket}/{out_key}"
            print(f"Wrote parquet to {s3_path}")

            # register partition pointing to folder
            partition_s3_prefix = f"s3://{bucket}/{partition_prefix}"
            register_partition_safe(bucket, year, month, partition_s3_prefix)

            results.append({"year": int(year), "month": int(month), "s3_key": out_key})

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Success", "partitions_written": results})
        }

    except Exception as e:
        traceback.print_exc()
        return {"statusCode": 500, "body": json.dumps({"message": "Error", "error": str(e)})}
