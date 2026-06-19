import io
import json
import os
from datetime import datetime, timezone

import boto3

GARAGE_ENDPOINT = os.getenv("GARAGE_ENDPOINT", "http://localhost:3900")
GARAGE_ACCESS_KEY = os.getenv("GARAGE_ACCESS_KEY", "")
GARAGE_SECRET_KEY = os.getenv("GARAGE_SECRET_KEY", "")

MODELS_BUCKET = "models"
CHAMPION_KEY = "champion/model.pkl"
CHAMPION_META_KEY = "champion/metadata.json"
HISTORY_KEY = "champion/rollback_history.json"


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


def _read_json(s3, bucket, key):
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(resp["Body"].read().decode())
    except Exception:
        return None


def _write_json(s3, bucket, key, data):
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(data, indent=2).encode())


def list_versions() -> list[dict]:
    s3 = _get_s3()
    versions = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=MODELS_BUCKET, Prefix="", Delimiter="/"):
        for prefix in page.get("CommonPrefixes", []):
            name = prefix["Prefix"].rstrip("/")
            if name in ("latest", "champion", "training-state"):
                continue
            sub_paginator = s3.get_paginator("list_objects_v2")
            for sub_page in sub_paginator.paginate(Bucket=MODELS_BUCKET, Prefix=name + "/", Delimiter="/"):
                for sub_prefix in sub_page.get("CommonPrefixes", []):
                    ts = sub_prefix["Prefix"].rstrip("/").split("/")[-1]
                    meta = _read_json(s3, MODELS_BUCKET, f"{name}/{ts}/metadata.json")
                    versions.append({
                        "version": f"{name}/{ts}",
                        "model_name": name,
                        "timestamp": ts,
                        "mae": meta["mae"] if meta else None,
                        "r2": meta["r2"] if meta else None,
                    })
    return sorted(versions, key=lambda v: v["timestamp"], reverse=True)


def rollback(version: str):
    s3 = _get_s3()

    source_model = f"{version}/model.pkl"
    source_meta = f"{version}/metadata.json"

    try:
        s3.head_object(Bucket=MODELS_BUCKET, Key=source_model)
    except Exception:
        print(f"ERROR: version '{version}' not found")
        available = [v["version"] for v in list_versions()]
        if available:
            print(f"Available versions: {', '.join(available[:10])}")
        return False

    s3.copy_object(
        Bucket=MODELS_BUCKET,
        CopySource={"Bucket": MODELS_BUCKET, "Key": source_model},
        Key=CHAMPION_KEY,
    )
    s3.copy_object(
        Bucket=MODELS_BUCKET,
        CopySource={"Bucket": MODELS_BUCKET, "Key": source_meta},
        Key=CHAMPION_META_KEY,
    )

    history = _read_json(s3, MODELS_BUCKET, HISTORY_KEY) or []
    history.append({
        "to_version": version,
        "rolled_back_at": datetime.now(timezone.utc).isoformat(),
    })
    _write_json(s3, MODELS_BUCKET, HISTORY_KEY, history)

    meta = _read_json(s3, MODELS_BUCKET, CHAMPION_META_KEY)
    print(f"Rollback successful: champion -> {version}")
    if meta:
        print(f"  MAE={meta['mae']:.4f}  R²={meta['r2']:.4f}")
    return True


def get_current_champion() -> dict:
    return _read_json(_get_s3(), MODELS_BUCKET, CHAMPION_META_KEY) or {}


def get_rollback_history() -> list:
    return _read_json(_get_s3(), MODELS_BUCKET, HISTORY_KEY) or []


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python rollback.py list")
        print("  python rollback.py rollback <version>")
        print("  python rollback.py champion")
        print("  python rollback.py history")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "list":
        versions = list_versions()
        if not versions:
            print("No model versions found")
        else:
            print(f"{'VERSION':<50} {'MAE':>8}  {'R²':>6}")
            print("-" * 70)
            for v in versions:
                mae_str = f"{v['mae']:.4f}" if v["mae"] else "N/A"
                r2_str = f"{v['r2']:.4f}" if v["r2"] else "N/A"
                print(f"{v['version']:<50} {mae_str:>8}  {r2_str:>6}")

    elif cmd == "rollback":
        if len(sys.argv) < 3:
            print("Usage: python rollback.py rollback <version>")
            sys.exit(1)
        ok = rollback(sys.argv[2])
        sys.exit(0 if ok else 1)

    elif cmd == "champion":
        champ = get_current_champion()
        if champ:
            print(json.dumps(champ, indent=2))
        else:
            print("No champion model found")

    elif cmd == "history":
        hist = get_rollback_history()
        if hist:
            for entry in hist:
                print(f"  {entry['to_version']}  ({entry['rolled_back_at']})")
        else:
            print("No rollback history")
