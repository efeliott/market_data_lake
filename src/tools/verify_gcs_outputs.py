"""
Utility: quick GCS listing to check partitions and row-count files (size only).
Usage:
    python scripts/verify_gcs_outputs.py --bucket market-lake-dev-eliott --prefix silver/prices
"""

import argparse
from google.cloud import storage


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", required=True, help="e.g. silver/prices or gold/returns_daily")
    args = parser.parse_args()

    client = storage.Client()
    blobs = list(client.list_blobs(args.bucket, prefix=args.prefix))
    for b in blobs:
        print(f"{b.name} ({b.size} bytes)")

    print(f"Total objects: {len(blobs)}")


if __name__ == "__main__":
    main()
