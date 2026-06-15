import json
import os
import subprocess
import time

import boto3
import urllib.request

GARAGE_ENDPOINT = os.getenv("GARAGE_ENDPOINT", "http://localhost:3900")
GARAGE_HOST = os.getenv("GARAGE_HOST", "localhost")
GARAGE_CONFIG = "/etc/garage.toml"
BUCKETS = ["raw-data", "processed-data", "features", "models"]
KEY_NAME = "gold-prediction-key"

ACCESS_KEY_ID = None
SECRET_KEY = None


def gar(*args):
    result = subprocess.run(
        ["garage", "-c", GARAGE_CONFIG] + list(args),
        capture_output=True, text=True, timeout=15,
    )
    if result.returncode != 0:
        raise RuntimeError(f"garage {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def wait_for_garage():
    import socket
    host = GARAGE_HOST
    port = 3900
    for attempt in range(30):
        try:
            s = socket.create_connection((host, port), timeout=5)
            s.close()
            print("Garage is ready!")
            return
        except Exception as e:
            print(f"Waiting for Garage ({attempt+1}/30): {e}")
            time.sleep(2)
    print("Garage did not become ready.")
    exit(1)


def ensure_layout():
    for attempt in range(30):
        try:
            output = gar("status")
            print(f"Garage status OK")
            break
        except RuntimeError as e:
            print(f"Waiting for garage status ({attempt+1}/30): {e}")
            time.sleep(2)
    else:
        print("Garage status never succeeded. Exiting.")
        exit(1)

    try:
        layout = gar("layout", "show")
    except RuntimeError:
        layout = ""

    version = 0
    for line in layout.splitlines():
        if "layout version:" in line.lower():
            version = int(line.split(":")[-1].strip())

    if version == 0:
        print("Layout not configured. Configuring single-node layout...")
        try:
            node_full = gar("node", "id")
            node_id = node_full.split("@")[0]
            print(f"Node ID: {node_id}")
            gar("layout", "assign", "-z", "dc1", "-c", "10G", node_id)
            gar("layout", "apply", "--version", "1")
            print("Layout configured successfully!")
        except RuntimeError as e:
            print(f"Layout configuration failed: {e}")
            exit(1)
    else:
        print(f"Layout already configured (version {version}).")


def create_key():
    global ACCESS_KEY_ID, SECRET_KEY

    try:
        output = gar("key", "create", KEY_NAME)
        for line in output.splitlines():
            stripped = line.strip()
            if stripped.startswith("Key ID:"):
                ACCESS_KEY_ID = stripped.split(":", 1)[1].strip()
            elif stripped.startswith("Secret key:"):
                SECRET_KEY = stripped.split(":", 1)[1].strip()
        if ACCESS_KEY_ID and SECRET_KEY:
            print(f"Created key '{KEY_NAME}' — AK: {ACCESS_KEY_ID}")
            gar("key", "allow", "--create-bucket", ACCESS_KEY_ID)
            print(f"Granted create-bucket permission to {ACCESS_KEY_ID}")
            return
        print(f"Unexpected key create output: {output}")
        exit(1)
    except RuntimeError as e:
        if "already exists" in str(e).lower():
            print(f"Key '{KEY_NAME}' already exists, fetching...")
        else:
            print(f"garage key create failed: {e}")
            exit(1)

    try:
        output = gar("key", "info", KEY_NAME)
        for line in output.splitlines():
            stripped = line.strip()
            if stripped.startswith("Key ID:"):
                ACCESS_KEY_ID = stripped.split(":", 1)[1].strip()
            elif stripped.startswith("Secret key:"):
                SECRET_KEY = stripped.split(":", 1)[1].strip()
        if ACCESS_KEY_ID:
            gar("key", "allow", "--create-bucket", ACCESS_KEY_ID)
            print(f"Found existing key — AK: {ACCESS_KEY_ID}")
            return
    except RuntimeError as e:
        print(f"garage key info failed: {e}")
        exit(1)

    print(f"Key '{KEY_NAME}' not found. Exiting.")
    exit(1)


def get_bucket_global_id(bucket_name):
    out = gar("bucket", "list")
    for line in out.splitlines():
        stripped = line.strip()
        if stripped.startswith(f"{ACCESS_KEY_ID}:{bucket_name}") or stripped.startswith(bucket_name):
            parts = stripped.split()
            if len(parts) >= 2:
                return parts[1]
    return None


def create_buckets():
    client = boto3.client(
        "s3",
        endpoint_url=GARAGE_ENDPOINT,
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=SECRET_KEY,
        region_name="us-east-1",
        use_ssl=False,
        config=boto3.session.Config(signature_version="s3v4"),
    )

    for bucket in BUCKETS:
        try:
            client.head_bucket(Bucket=bucket)
            print(f"Bucket '{bucket}' already exists")
        except Exception:
            client.create_bucket(Bucket=bucket)
            print(f"Created bucket '{bucket}'")
        gid = get_bucket_global_id(bucket)
        if gid:
            gar("bucket", "allow", "--read", "--write", "--owner", "--key", ACCESS_KEY_ID, gid)
            print(f"Granted read/write/owner on '{bucket}' to key {ACCESS_KEY_ID}")
        else:
            print(f"Warning: could not find global ID for bucket '{bucket}'")


print(f"Initializing Garage at {GARAGE_ENDPOINT}...")

wait_for_garage()
ensure_layout()
create_key()
create_buckets()

print()
print("=" * 50)
print("  GARAGE SETUP COMPLETE")
print("=" * 50)
print(f"  Access Key ID: {ACCESS_KEY_ID}")
print(f"  Secret Key:    {SECRET_KEY}")
print("=" * 50)
print("  Tambahkan ke .env:")
print(f"  GARAGE_ACCESS_KEY={ACCESS_KEY_ID}")
print(f"  GARAGE_SECRET_KEY={SECRET_KEY}")
print("=" * 50)
