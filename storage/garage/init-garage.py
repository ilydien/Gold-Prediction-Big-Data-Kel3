import os
import time

import boto3

GARAGE_ENDPOINT = os.getenv("GARAGE_ENDPOINT", "http://localhost:3900")
BUCKETS = ["raw-data", "processed-data", "features", "models"]

print(f"Initializing Garage at {GARAGE_ENDPOINT}...")

for attempt in range(30):
    try:
        client = boto3.client(
            "s3",
            endpoint_url=GARAGE_ENDPOINT,
            aws_access_key_id="",
            aws_secret_access_key="",
            region_name="us-east-1",
            use_ssl=False,
            config=boto3.session.Config(signature_version="s3v4"),
        )
        client.list_buckets()
        print("Garage is ready!")
        break
    except Exception as e:
        print(f"Waiting for Garage ({attempt+1}/30): {e}")
        time.sleep(2)
else:
    print("Garage did not become ready in time.")
    exit(1)

for bucket in BUCKETS:
    try:
        client.head_bucket(Bucket=bucket)
        print(f"Bucket '{bucket}' already exists")
    except Exception:
        client.create_bucket(Bucket=bucket)
        print(f"Created bucket '{bucket}'")

print("\n--- Garage Setup Complete ---")
print("To create an access key, run in the garage container:")
print("  garage key create gold-prediction-key")
print("")
print("Then update your .env with the Access Key and Secret Key.")
