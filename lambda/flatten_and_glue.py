import pandas as pd
import json
import boto3
import io
from datetime import datetime, timezone

def flatten_records(data):  
    rows = []
    for rec in data:
        airport_code = rec.get("Airport", {}).get("Code")
        airport_name = rec.get("Airport", {}).get("Name")
        time_label = rec.get("Time", {}).get("Label")
        year = rec.get("Time", {}).get("Year")
        month = rec.get("Time", {}).get("Month")
        month_name = rec.get("Time", {}).get("Month Name")

        stats = rec.get("Statistics", {})

        delays = stats.get("# of Delays", {})
        carriers_info = stats.get("Carriers", {})
        flights = stats.get("Flights", {})
        minutes = stats.get("Minutes Delayed", {})

        row = {
            "airport_code": airport_code,
            "airport_name": airport_name,
            "time_label": time_label,
            "year": year,
            "month": month,
            "month_name": month_name,
            # delays
            "delay_carrier_count": delays.get("Carrier"),
            "delay_late_aircraft_count": delays.get("Late Aircraft"),
            "delay_national_system_count": delays.get("National Aviation System"),
            "delay_security_count": delays.get("Security"),
            "delay_weather_count": delays.get("Weather"),
            # carriers
            "carriers_names": carriers_info.get("Names"),
            "carriers_total": carriers_info.get("Total"),
            # flights
            "flights_cancelled": flights.get("Cancelled"),
            "flights_delayed": flights.get("Delayed"),
            "flights_diverted": flights.get("Diverted"),
            "flights_on_time": flights.get("On Time"),
            "flights_total": flights.get("Total"),
            # minutes delayed
            "minutes_carrier": minutes.get("Carrier"),
            "minutes_late_aircraft": minutes.get("Late Aircraft"),
            "minutes_national_system": minutes.get("National Aviation System"),
            "minutes_security": minutes.get("Security"),
            "minutes_total": minutes.get("Total"),
            "minutes_weather": minutes.get("Weather"),
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    return df

def lambda_handler(event, context):
    # get bucket and key from s3 event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    s3 = boto3.client('s3')
    glue = boto3.client('glue')

    # read json from S3
    resp = s3.get_object(Bucket=bucket, Key=key)
    content = resp['Body'].read().decode('utf-8')
    data = json.loads(content)

    df = flatten_records(data)

    # make parquet
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow', compression='snappy')

    # timestamped key for output
    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y%m%d_%H%M%S")
    output_key = f"processed/parquet/airport_delays_{ts}.parquet"

    s3.put_object(Bucket=bucket, Key=output_key, Body=parquet_buffer.getvalue())

    # trigger glue crawler (opt)
    crawler_name = ""
    try:
        glue.start_crawler(Name=crawler_name)
    except Exception as e:
        print(f"!! Could not start crawler {crawler_name}: {e}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Success",
            "records_processed": len(df),
            "output_key": output_key
        })
    }
