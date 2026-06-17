# -*- coding: utf-8 -*-
"""Ship recorded Parquet partitions to S3 / Cloudflare R2 (S3-compatible).

Each node uploads under its own prefix so three machines never collide:
    s3://<bucket>/<prefix>/exchange=.../date=.../hour=.../part-*.parquet

Only fully-written files (older than --min-age seconds) are uploaded; already-
uploaded files are tracked in a local manifest so re-runs are cheap. Run once
from cron, or as a loop with --interval.

Needs:  pip install boto3
Creds:  env AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY (R2: use an R2 API token's
        access key id + secret), or ~/.aws/credentials.

Examples:
  # Cloudflare R2
  python sync_parquet.py --root data/bbo --bucket bbo-archive --prefix node=5090 \
      --endpoint-url https://<ACCOUNT_ID>.r2.cloudflarestorage.com --interval 300

  # Amazon S3 (Tokyo)
  python sync_parquet.py --root data/bbo --bucket bbo-archive --prefix node=aws \
      --region ap-northeast-1 --delete-after --interval 300
"""
import argparse
import json
import os
import sys
import time

MANIFEST = ".synced.json"


def load_manifest(root):
    p = os.path.join(root, MANIFEST)
    try:
        return set(json.load(open(p, encoding="utf-8")))
    except Exception:
        return set()


def save_manifest(root, done):
    try:
        json.dump(sorted(done), open(os.path.join(root, MANIFEST), "w", encoding="utf-8"))
    except Exception as e:
        print("manifest save failed:", e)


def iter_parquet(root, min_age):
    now = time.time()
    for dirpath, _, files in os.walk(root):
        for f in files:
            if not f.endswith(".parquet"):
                continue
            full = os.path.join(dirpath, f)
            try:
                st = os.stat(full)
            except OSError:
                continue
            if now - st.st_mtime < min_age:
                continue  # may still be in flight
            rel = os.path.relpath(full, root).replace(os.sep, "/")
            yield full, rel, st.st_size


def run_once(s3, bucket, root, prefix, min_age, delete_after, dry):
    done = load_manifest(root)
    up = bytes_up = skip = 0
    for full, rel, size in iter_parquet(root, min_age):
        key = f"{prefix.rstrip('/')}/{rel}" if prefix else rel
        if rel in done:
            skip += 1
            continue
        if dry:
            print(f"  would upload {rel} -> s3://{bucket}/{key} ({size:,}B)")
        else:
            s3.upload_file(full, bucket, key)
            done.add(rel)
            if delete_after:
                try:
                    os.remove(full)
                except OSError:
                    pass
        up += 1
        bytes_up += size
        if up % 200 == 0 and not dry:
            save_manifest(root, done)
    if not dry:
        save_manifest(root, done)
    print(f"[{time.strftime('%H:%M:%S')}] uploaded {up} files ({bytes_up/1e6:.1f} MB), "
          f"skipped {skip} already-synced" + (" [DRY-RUN]" if dry else ""))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="data/bbo")
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", default="", help="key prefix, e.g. node=5090")
    ap.add_argument("--endpoint-url", default=None, help="set for R2: https://<acct>.r2.cloudflarestorage.com")
    ap.add_argument("--region", default=None)
    ap.add_argument("--min-age", type=float, default=20.0, help="skip files younger than N s (still being written)")
    ap.add_argument("--delete-after", action="store_true", help="delete local file after successful upload")
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
        run_once(s3, a.bucket, a.root, a.prefix, a.min_age, a.delete_after, a.dry_run)
        if not a.interval:
            break
        time.sleep(a.interval)


if __name__ == "__main__":
    main()
