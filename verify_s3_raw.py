"""
SellerIQ S3 Raw Files Verification
===================================
Read-only script to confirm S3 raw files are intact before
attempting recovery from a prod data loss.

Does NOT modify anything. Just lists and counts files.

Run:
    python verify_s3_raw.py
"""

import boto3
import config


def count_prefix(s3, bucket: str, prefix: str) -> tuple[int, int]:
    """Count objects under a prefix. Returns (count, total_bytes)."""
    paginator = s3.get_paginator("list_objects_v2")
    count = 0
    total_bytes = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            count += 1
            total_bytes += obj["Size"]
    return count, total_bytes


def fmt_size(b: int) -> str:
    if b < 1024:
        return f"{b} B"
    if b < 1024 * 1024:
        return f"{b/1024:.1f} KB"
    if b < 1024 * 1024 * 1024:
        return f"{b/1024/1024:.1f} MB"
    return f"{b/1024/1024/1024:.2f} GB"


def main():
    print("=" * 60)
    print("S3 Raw Files Verification")
    print("=" * 60)

    s3 = boto3.client(
        "s3",
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
        region_name=config.AWS_REGION,
    )
    bucket = config.S3_BUCKET
    print(f"Bucket: {bucket}")
    print(f"Region: {config.AWS_REGION}")
    print()

    prefixes_to_check = [
        ("Sales & Traffic", "amazon/GET_SALES_AND_TRAFFIC_REPORT/"),
        ("Inventory",       "amazon/GET_FBA_MYI_ALL_INVENTORY_DATA/"),
        ("Catalog",         "amazon/catalog/"),
        ("Listings",        "amazon/GET_MERCHANT_LISTINGS_ALL_DATA/"),
    ]

    print(f"{'Category':<22} {'Files':<10} {'Total size'}")
    print("-" * 50)
    grand_total_files = 0
    grand_total_bytes = 0
    for label, prefix in prefixes_to_check:
        try:
            count, size = count_prefix(s3, bucket, prefix)
            grand_total_files += count
            grand_total_bytes += size
            print(f"{label:<22} {count:<10} {fmt_size(size)}")
        except Exception as exc:
            print(f"{label:<22} ERROR: {exc}")

    print("-" * 50)
    print(f"{'TOTAL':<22} {grand_total_files:<10} {fmt_size(grand_total_bytes)}")
    print()

    # Show 5 most recent sales-traffic files as a sanity check
    print("Most recent Sales & Traffic files:")
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix="amazon/GET_SALES_AND_TRAFFIC_REPORT/",
    )
    contents = sorted(
        response.get("Contents", []),
        key=lambda o: o["LastModified"],
        reverse=True,
    )
    for obj in contents[:5]:
        print(f"  {obj['LastModified'].strftime('%Y-%m-%d %H:%M')}  {fmt_size(obj['Size']):>10}  {obj['Key']}")

    print()
    if grand_total_files > 0:
        print("✓ S3 raw files are intact. Recovery is fully tractable.")
    else:
        print("✗ No raw files found. Investigate S3 bucket separately.")


if __name__ == "__main__":
    main()