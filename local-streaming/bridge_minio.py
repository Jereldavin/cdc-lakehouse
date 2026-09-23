import boto3
import json
from datetime import datetime
from kafka import KafkaConsumer

# Koneksi ke MinIO (S3-compatible)
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin'
)

BUCKET = 'cdc-raw'
import uuid

# Koneksi ke Kafka
consumer = KafkaConsumer(
    'dbserver1.inventory.products',
    'dbserver1.inventory.customers',
    'dbserver1.inventory.orders',
    bootstrap_servers=['localhost:9092'],
    group_id=f'minio-bridge-{uuid.uuid4().hex[:8]}',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    consumer_timeout_ms=5000
)

# Buat bucket kalau belum ada
try:
    s3.create_bucket(Bucket=BUCKET)
    print(f"[OK] Bucket '{BUCKET}' created")
except Exception:
    print(f"[OK] Bucket '{BUCKET}' already exists")

# Maintain separate batches for each topic
batches = {
    'dbserver1.inventory.products': [],
    'dbserver1.inventory.customers': [],
    'dbserver1.inventory.orders': []
}
file_count = 0

print("Listening to Kafka... (trigger a PostgreSQL update to see data flow)")

for message in consumer:
    topic = message.topic
    
    # We only care about topics we track
    if topic in batches:
        batches[topic].append(message.value)
        
        # When a specific topic hits 50 records, flush it
        if len(batches[topic]) >= 50:
            table_name = topic.split('.')[-1]  # e.g. 'products'
            prefix = f"{table_name}/"          # separate folder per table
            
            body = "\n".join(json.dumps(obj) for obj in batches[topic])
            key = f"{prefix}cdc_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{file_count}.jsonl"
            
            s3.put_object(Bucket=BUCKET, Key=key, Body=body)
            print(f"  [UPLOADED] s3://{BUCKET}/{key} ({len(batches[topic])} events)")
            
            batches[topic] = []
            file_count += 1

# Write sisa batch for any topic that still has data
for topic, batch in batches.items():
    if batch:
        table_name = topic.split('.')[-1]
        prefix = f"{table_name}/"
        
        body = "\n".join(json.dumps(obj) for obj in batch)
        key = f"{prefix}cdc_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{file_count}.jsonl"
        
        s3.put_object(Bucket=BUCKET, Key=key, Body=body)
        print(f"  [UPLOADED] s3://{BUCKET}/{key} ({len(batch)} events)")
        file_count += 1

print(f"\nDone. Total files uploaded: {file_count}")