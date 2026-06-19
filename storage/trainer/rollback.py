import io
import json
import os
from datetime import datetime, timezone

import boto3

GARAGE_ENDPOINT = os.getenv("GARAGE_ENDPOINT", "http://localhost:3900")
GARAGE_ACCESS_KEY = os.getenv("GARAGE_ACCESS_KEY", "")
GARAGE_SECRET_KEY = os.getenv("GARAGE_SECRET_KEY", "")
<<<<<<< HEAD

MODELS_BUCKET = "models"
CHAMPION_KEY = "champion/model.pkl"
CHAMPION_META_KEY = "champion/metadata.json"
HISTORY_KEY = "champion/rollback_history.json"
=======
PREDICTION_HORIZONS = [
    int(h) for h in os.getenv("PREDICTION_HORIZONS", "12,24,48,72,168,720").split(",")
]

MODELS_BUCKET = "models"
>>>>>>> 09f4d05 (feat: multi-horizon forecasting with yfinance data source (25 features, 6 horizons))


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


<<<<<<< HEAD
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
=======
def list_versions(horizon: int = None) -> list[dict]:
    s3 = _get_s3()
    versions = []
    prefixes_to_scan = [f"h={h}/" for h in (PREDICTION_HORIZONS if horizon is None else [horizon])]

    for h_prefix in prefixes_to_scan:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=MODELS_BUCKET, Prefix=h_prefix, Delimiter="/"):
            for prefix in page.get("CommonPrefixes", []):
                name = prefix["Prefix"].rstrip("/").split("/")[-1]
                if name in ("latest", "champion", "training-state"):
                    continue
                sub_paginator = s3.get_paginator("list_objects_v2")
                for sub_page in sub_paginator.paginate(Bucket=MODELS_BUCKET, Prefix=prefix["Prefix"], Delimiter="/"):
                    for sub_prefix in sub_page.get("CommonPrefixes", []):
                        ts = sub_prefix["Prefix"].rstrip("/").split("/")[-1]
                        full_key = f"{h_prefix.rstrip('/')}/{name}/{ts}"
                        meta = _read_json(s3, MODELS_BUCKET, f"{full_key}/metadata.json")
                        versions.append({
                            "version": full_key,
                            "horizon": h_prefix.strip("h=").strip("/"),
                            "model_name": name,
                            "timestamp": ts,
                            "mae": meta["mae"] if meta else None,
                            "r2": meta["r2"] if meta else None,
                        })
>>>>>>> 09f4d05 (feat: multi-horizon forecasting with yfinance data source (25 features, 6 horizons))
    return sorted(versions, key=lambda v: v["timestamp"], reverse=True)


def rollback(version: str):
    s3 = _get_s3()
<<<<<<< HEAD

    source_model = f"{version}/model.pkl"
    source_meta = f"{version}/metadata.json"
=======
    parts = version.split("/")
    if len(parts) == 3:
        h, model, ts = parts
    else:
        print(f"ERROR: invalid version format '{version}' (expected h=N/model/ts)")
        return False

    source_model = f"{version}/model.pkl"
    source_meta = f"{version}/metadata.json"
    champion_key = f"{h}/champion/model.pkl"
    champion_meta_key = f"{h}/champion/metadata.json"
>>>>>>> 09f4d05 (feat: multi-horizon forecasting with yfinance data source (25 features, 6 horizons))

    try:
        s3.head_object(Bucket=MODELS_BUCKET, Key=source_model)
    except Exception:
        print(f"ERROR: version '{version}' not found")
<<<<<<< HEAD
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
=======
        return False

    s3.copy_object(Bucket=MODELS_BUCKET, CopySource={"Bucket": MODELS_BUCKET, "Key": source_model}, Key=champion_key)
    s3.copy_object(Bucket=MODELS_BUCKET, CopySource={"Bucket": MODELS_BUCKET, "Key": source_meta}, Key=champion_meta_key)

    history_key = f"{h}/champion/rollback_history.json"
    history = _read_json(s3, MODELS_BUCKET, history_key) or []
    history.append({"to_version": version, "rolled_back_at": datetime.now(timezone.utc).isoformat()})
    _write_json(s3, MODELS_BUCKET, history_key, history)

    meta = _read_json(s3, MODELS_BUCKET, champion_meta_key)
    print(f"Rollback successful: h={h} champion -> {model}/{ts}")
>>>>>>> 09f4d05 (feat: multi-horizon forecasting with yfinance data source (25 features, 6 horizons))
    if meta:
        print(f"  MAE={meta['mae']:.4f}  R²={meta['r2']:.4f}")
    return True


<<<<<<< HEAD
def get_current_champion() -> dict:
    return _read_json(_get_s3(), MODELS_BUCKET, CHAMPION_META_KEY) or {}


def get_rollback_history() -> list:
    return _read_json(_get_s3(), MODELS_BUCKET, HISTORY_KEY) or []
=======
def show_champions():
    s3 = _get_s3()
    for h in PREDICTION_HORIZONS:
        meta = _read_json(s3, MODELS_BUCKET, f"h={h}/champion/metadata.json")
        if meta:
            print(f"h={h:>4d}: {meta['model_name']:<20s} MAE={meta['mae']:.4f}")
        else:
            print(f"h={h:>4d}: no champion")
>>>>>>> 09f4d05 (feat: multi-horizon forecasting with yfinance data source (25 features, 6 horizons))


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage:")
<<<<<<< HEAD
        print("  python rollback.py list")
        print("  python rollback.py rollback <version>")
        print("  python rollback.py champion")
        print("  python rollback.py history")
=======
        print("  python rollback.py list [horizon]")
        print("  python rollback.py champions")
        print("  python rollback.py rollback <version>")
        print("  python rollback.py history <horizon>")
>>>>>>> 09f4d05 (feat: multi-horizon forecasting with yfinance data source (25 features, 6 horizons))
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "list":
<<<<<<< HEAD
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
=======
        h = int(sys.argv[2]) if len(sys.argv) > 2 else None
        versions = list_versions(h)
        if not versions:
            print("No model versions found")
        else:
            print(f"{'VERSION':<55} {'H':>4}  {'MAE':>8}  {'R²':>6}")
            print("-" * 80)
            for v in versions:
                m = f"{v['mae']:.4f}" if v["mae"] else "N/A"
                r = f"{v['r2']:.4f}" if v["r2"] else "N/A"
                print(f"{v['version']:<55} {v['horizon']:>4}  {m:>8}  {r:>6}")

    elif cmd == "champions":
        show_champions()
>>>>>>> 09f4d05 (feat: multi-horizon forecasting with yfinance data source (25 features, 6 horizons))

    elif cmd == "rollback":
        if len(sys.argv) < 3:
            print("Usage: python rollback.py rollback <version>")
            sys.exit(1)
        ok = rollback(sys.argv[2])
        sys.exit(0 if ok else 1)

<<<<<<< HEAD
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
=======
    elif cmd == "history":
        if len(sys.argv) < 3:
            print("Usage: python rollback.py history <horizon>")
            sys.exit(1)
        s3 = _get_s3()
        h = sys.argv[2]
        hist = _read_json(s3, MODELS_BUCKET, f"h={h}/champion/rollback_history.json") or []
        if hist:
            for entry in hist:
                print(f"  {entry.get('to_version', '?')}  ({entry.get('rolled_back_at', '?')})")
        else:
            print(f"No rollback history for h={h}")
>>>>>>> 09f4d05 (feat: multi-horizon forecasting with yfinance data source (25 features, 6 horizons))
