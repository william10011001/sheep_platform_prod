# -*- coding: utf-8 -*-
"""Pull recorded Parquet from R2/S3 down to this machine (5090) for backtest.

Mirror of sync_parquet.py but in the download direction. Tracks what was already
pulled in a local manifest so re-runs are cheap. R2 egress is free.

    pip install boto3
    set AWS_ACCESS_KEY_ID=... & set AWS_SECRET_ACCESS_KEY=...
    python pull_from_r2.py --root data\\from_aws --bucket bbo-data --prefix node=aws-tokyo \
        --endpoint-url https://<ACCOUNT_ID>.r2.cloudflarestorage.com --interval 900
"""
import argparse
import json
import os
import sys
import time

MANIFEST = ".pulled.json"


def load_manifest(root):
    try:
        return set(json.load(open(os.path.join(root, MANIFEST), encoding="utf-8")))
    except Exception:
        return set()


def save_manifest(root, done):
    try:
        os.makedirs(root, exist_ok=True)
        json.dump(sorted(done), open(os.path.join(root, MANIFEST), "w", encoding="utf-8"))
    except Exception as e:
        print("manifest save failed:", e)


def run_once(s3, bucket, root, prefix, dry):
    done = load_manifest(root)
    paginator = s3.get_paginator("list_objects_v2")
    got = bytes_got = skip = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            if key in done:
                skip += 1
                continue
            local = os.path.join(root, key.replace("/", os.sep))
            if dry:
                print(f"  would pull {key} -> {local} ({obj['Size']:,}B)")
            else:
                os.makedirs(os.path.dirname(local), exist_ok=True)
                s3.download_file(bucket, key, local)
                done.add(key)
            got += 1
            bytes_got += obj["Size"]
            if got % 200 == 0 and not dry:
                save_manifest(root, done)
    if not dry:
        save_manifest(root, done)
    print(f"[{time.strftime('%H:%M:%S')}] pulled {got} files ({bytes_got/1e6:.1f} MB), "
          f"skipped {skip} already-local" + (" [DRY-RUN]" if dry else ""))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=os.path.join("data", "from_aws"))
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", default="")
    ap.add_argument("--endpoint-url", default=None, help="R2: https://<acct>.r2.cloudflarestorage.com")
    ap.add_argument("--region", default=None)
    ap.add_argument("--interval", type=float, default=0, help="loop every N s (0 = once)")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    s3 = None
    if not a.dry_run:
        try:
            import boto3
        except ImportError:
            sys.exit("boto3 not installed.  pip install boto3")
        s3 = boto3.client("s3", endpoint_url=a.endpoint_url, region_name=a.region)

    while True:
        run_once(s3, a.bucket, a.root, a.prefix, a.dry_run)
        if not a.interval:
            break
        time.sleep(a.interval)


if __name__ == "__main__":
    main()
