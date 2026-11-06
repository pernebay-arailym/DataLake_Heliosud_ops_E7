import json
from datetime import datetime
import boto3
from botocore.config import Config
from kafka import KafkaConsumer

# Configuration
KAFKA_BOOTSTRAP = 'localhost:9092'
TOPIC_NAME = 'heliosud-telemetry'
S3_ENDPOINT = 'http://localhost:8333'
BUCKET_NAME = 'raw'

def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id='any',
        aws_secret_access_key='any',
        config=Config(s3={'addressing_style': 'path'}),
        region_name='us-east-1'
    )

def main():
    s3 = get_s3_client()
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='raw-ingestion-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    print(f"--- Listening for messages on Redpanda topic '{TOPIC_NAME}' ---")

    count = 0
    for message in consumer:
        payload = message.value
        
        # Identify vendor based on schema structure
        if 'device_id' in payload:
            vendor = 'solartech'
            ts_str = payload.get('ts', datetime.utcnow().isoformat())
        else:
            vendor = 'voltaris'
            ts_str = payload.get('time', datetime.utcnow().isoformat())
            
        # Parse date for Hive partitioning
        try:
            dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
        except Exception:
            dt = datetime.utcnow()
            
        year = dt.strftime('%Y')
        month = dt.strftime('%m')
        day = dt.strftime('%d')
        
        # Construct raw S3 Key path
        device_id = payload.get('device_id') or payload.get('inverter', {}).get('id', 'unknown')
        timestamp_ms = int(datetime.utcnow().timestamp() * 1000)
        s3_key = f"telemetry/vendor={vendor}/year={year}/month={month}/day={day}/{device_id}_{timestamp_ms}.json"
        
        # Write to SeaweedFS raw bucket
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(payload).encode('utf-8')
        )
        
        count += 1
        print(f"[{count}] Saved payload to s3://{BUCKET_NAME}/{s3_key}")
        
        # For testing, break after 12 messages
        if count >= 12:
            print("\n[SUCCESS] Micro-batch of 12 messages written to raw bucket!")
            break

if __name__ == '__main__':
    main()