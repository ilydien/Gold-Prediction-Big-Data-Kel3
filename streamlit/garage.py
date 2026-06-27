import json
import os

import boto3

GARAGE_ENDPOINT = os.getenv("GARAGE_ENDPOINT", "http://localhost:3900")
GARAGE_ACCESS_KEY = os.getenv("GARAGE_ACCESS_KEY", "")
GARAGE_SECRET_KEY = os.getenv("GARAGE_SECRET_KEY", "")


def _get_s3():
    return boto3.client(
        "s3",
        endpoint_url=GARAGE_ENDPOINT,
        aws_access_key_id=GARAGE_ACCESS_KEY,
        aws_secret_access_key=GARAGE_SECRET_KEY,
        region_name="us-east-1",
        use_ssl=False,
        config=boto3.session.Config(signature_version="s3v4"),
    )


def test_connection():
    try:
        s3 = _get_s3()
        s3.list_buckets()
        return True, "Connected"
    except Exception as e:
        return False, str(e)


def list_buckets():
    s3 = _get_s3()
    resp = s3.list_buckets()
    return [b["Name"] for b in resp["Buckets"]]


def get_bucket_stats():
    s3 = _get_s3()
    stats = {}
    for bucket in ["raw-data", "processed-data", "hourly-history", "features", "models"]:
        try:
            count = 0
            total_size = 0
            last_modified = None
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket):
                for obj in page.get("Contents", []):
                    count += 1
                    total_size += obj["Size"]
                    if last_modified is None or obj["LastModified"] > last_modified:
                        last_modified = obj["LastModified"]
            stats[bucket] = {
                "count": count,
                "size_mb": round(total_size / (1024 * 1024), 2),
                "last_modified": last_modified,
            }
        except Exception as e:
            stats[bucket] = {"count": 0, "size_mb": 0, "last_modified": None, "error": str(e)}
    return stats


def get_champion_metadata(horizon):
    try:
        s3 = _get_s3()
        resp = s3.get_object(Bucket="models", Key=f"h={horizon}/champion/metadata.json")
        return json.loads(resp["Body"].read().decode())
    except Exception:
        return None


def get_all_champions():
    try:
        s3 = _get_s3()
        resp = s3.list_objects_v2(Bucket="models", Prefix="h=", Delimiter="/")
        horizons = []
        for prefix in resp.get("CommonPrefixes", []):
            h_str = prefix["Prefix"].replace("h=", "").replace("/", "")
            try:
                horizons.append(int(h_str))
            except ValueError:
                pass

        champions = {}
        for h in sorted(horizons):
            meta = get_champion_metadata(h)
            if meta:
                champions[h] = meta
        return champions
    except Exception as e:
        return {"error": str(e)}
