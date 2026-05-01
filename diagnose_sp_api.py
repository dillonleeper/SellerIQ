"""
SellerIQ SP-API Diagnostic
==========================
A read-only diagnostic script to figure out the actual rate-limit state
of your SP-API account.

What it does
------------
1. Calls a *very* cheap operation (getMarketplaceParticipations) which has
   a generous rate limit. If THIS throttles, the issue is account-wide,
   not just the createReport endpoint.
2. Calls getReports (a list call, also cheap) to check the listing endpoint.
3. Captures and prints the rate-limit headers from each response so we can
   see your actual current rate limit, not the documented default.
4. Looks at recent ingestion_job_log rows to count how many createReport
   calls have happened today and what the failure pattern looks like.

What it does NOT do
-------------------
- It does NOT call createReport. We are NOT making the throttle situation
  worse. No reports are requested.
- It does NOT modify any data.
- It does NOT load anything to S3 or Postgres.

Run it
------
    python diagnose_sp_api.py

Then paste the entire output back. That tells us exactly where the problem is.
"""

from __future__ import annotations

import sys
import traceback
from datetime import datetime, timedelta, UTC

import psycopg2
from sp_api.api import Sellers, Reports
from sp_api.base import Marketplaces

import config


def banner(title: str) -> None:
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def get_credentials() -> dict:
    return {
        "refresh_token": config.AMAZON_REFRESH_TOKEN,
        "lwa_app_id": config.AMAZON_CLIENT_ID,
        "lwa_client_secret": config.AMAZON_CLIENT_SECRET,
    }


def get_postgres_connection():
    return psycopg2.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        dbname=config.POSTGRES_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
    )


# --------------------------------------------------------------------------
# Helpers to extract headers from both successful responses AND exceptions.
# python-amazon-sp-api stores headers in slightly different places depending
# on whether the call succeeded or threw. We try both.
# --------------------------------------------------------------------------
def extract_headers(obj) -> dict:
    """Best-effort extraction of headers from a response or exception."""
    headers = getattr(obj, "headers", None)
    if headers is None:
        return {}
    # headers can be a dict, a list of tuples, or a CaseInsensitiveDict.
    try:
        return dict(headers)
    except Exception:
        try:
            return {k: v for k, v in headers}
        except Exception:
            return {}


def print_rate_limit_headers(headers: dict, label: str) -> None:
    """Print just the rate-limit-relevant headers."""
    print(f"\n  [{label}] Response headers of interest:")
    keys_of_interest = [
        "x-amzn-RateLimit-Limit",
        "x-amzn-ratelimit-limit",        # case-insensitive variant
        "x-amzn-RequestId",
        "x-amzn-requestid",
        "Retry-After",
        "retry-after",
    ]
    found_any = False
    for key in keys_of_interest:
        if key in headers:
            print(f"    {key}: {headers[key]}")
            found_any = True
    if not found_any:
        print("    (none of the rate-limit headers were present)")
        print(f"    All headers we got back: {list(headers.keys())}")


# --------------------------------------------------------------------------
# Tests
# --------------------------------------------------------------------------
def test_sellers_api() -> None:
    """
    The Sellers API getMarketplaceParticipations call is a 'health check'
    style operation — generous rate limits. If this throttles, your whole
    account is in cooldown, not just the Reports endpoint.
    """
    banner("TEST 1 — Sellers API (account-level health check)")
    try:
        sellers = Sellers(credentials=get_credentials(), marketplace=Marketplaces.US)
        response = sellers.get_marketplace_participations()
        headers = extract_headers(response)
        print(f"  STATUS: SUCCESS")
        print(f"  Number of marketplace participations: "
              f"{len(response.payload) if isinstance(response.payload, list) else 'unknown'}")
        print_rate_limit_headers(headers, "Sellers.getMarketplaceParticipations")
    except Exception as exc:
        headers = extract_headers(exc)
        print(f"  STATUS: FAILED")
        print(f"  Exception type: {type(exc).__name__}")
        print(f"  Exception message: {exc}")
        print_rate_limit_headers(headers, "Sellers (failure)")


def test_get_reports_list() -> None:
    """
    getReports lists existing reports. It's a different endpoint from
    createReport, with its own rate bucket. If this works fine, the issue
    is specific to createReport, not the whole Reports API.
    """
    banner("TEST 2 — Reports API getReports (listing endpoint)")
    try:
        reports = Reports(credentials=get_credentials(), marketplace=Marketplaces.US)
        # Look at reports created in the last 24 hours
        created_since = (datetime.now(UTC) - timedelta(days=1)).isoformat()
        response = reports.get_reports(
            reportTypes=["GET_SALES_AND_TRAFFIC_REPORT"],
            createdSince=created_since,
        )
        headers = extract_headers(response)
        report_list = response.payload.get("reports", []) if hasattr(response, "payload") else []
        print(f"  STATUS: SUCCESS")
        print(f"  GET_SALES_AND_TRAFFIC_REPORT reports created in last 24h: {len(report_list)}")
        if report_list:
            print(f"\n  Recent reports:")
            for r in report_list[:10]:
                print(
                    f"    {r.get('reportId', '?'):<20} "
                    f"status={r.get('processingStatus', '?'):<12} "
                    f"created={r.get('createdTime', '?')}"
                )
        print_rate_limit_headers(headers, "Reports.getReports")
    except Exception as exc:
        headers = extract_headers(exc)
        print(f"  STATUS: FAILED")
        print(f"  Exception type: {type(exc).__name__}")
        print(f"  Exception message: {exc}")
        print_rate_limit_headers(headers, "Reports (failure)")


def report_local_pipeline_activity() -> None:
    """
    Check our own ingestion_job_log to see how many calls we've made today
    and what the failure pattern looks like. This tells us if we're really
    making "a few" calls, or if something is calling more than expected.
    """
    banner("TEST 3 — Local ingestion_job_log activity (last 24h)")

    try:
        conn = get_postgres_connection()
    except Exception as exc:
        print(f"  Could not connect to Postgres: {exc}")
        return

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('ingestion_job_log')")
            if cur.fetchone()[0] is None:
                print("  ingestion_job_log table does not exist yet — skipping.")
                return

            # Aggregate counts by report_type, marketplace, status for the last 24h.
            cur.execute("""
                SELECT
                    report_type,
                    marketplace,
                    request_status,
                    COUNT(*) AS n,
                    MIN(created_at) AS first_seen,
                    MAX(created_at) AS last_seen
                FROM ingestion_job_log
                WHERE created_at >= NOW() - INTERVAL '24 hours'
                GROUP BY report_type, marketplace, request_status
                ORDER BY report_type, marketplace, request_status
            """)
            rows = cur.fetchall()

        if not rows:
            print("  No ingestion_job_log activity in the last 24 hours.")
            return

        print(f"  {'report_type':<40} {'mkt':<4} {'status':<12} {'n':<5} {'first_seen':<22} {'last_seen'}")
        print(f"  {'-' * 110}")
        for r in rows:
            report_type, marketplace, status, n, first_seen, last_seen = r
            print(f"  {report_type:<40} {marketplace:<4} {status:<12} {n:<5} "
                  f"{str(first_seen):<22} {str(last_seen)}")

        # Also count failed jobs per marketplace in the last 24h
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    marketplace,
                    COUNT(*) AS failed_count,
                    MAX(created_at) AS most_recent_failure,
                    MAX(error_message) AS sample_error
                FROM ingestion_job_log
                WHERE request_status = 'failed'
                  AND created_at >= NOW() - INTERVAL '24 hours'
                GROUP BY marketplace
                ORDER BY marketplace
            """)
            failure_rows = cur.fetchall()

        if failure_rows:
            print(f"\n  FAILURE SUMMARY (last 24h):")
            for r in failure_rows:
                marketplace, n, when, sample = r
                print(f"    {marketplace}: {n} failures, most recent at {when}")
                if sample:
                    print(f"      sample error: {sample[:200]}")

    finally:
        conn.close()


def main() -> int:
    print("=" * 70)
    print("SellerIQ SP-API Diagnostic")
    print(f"Run at: {datetime.now(UTC).isoformat()} UTC")
    print("=" * 70)
    print("\nThis script does NOT call createReport. Safe to run.")

    try:
        test_sellers_api()
    except Exception as exc:
        print(f"  Test 1 raised an unexpected error: {exc}")
        traceback.print_exc()

    try:
        test_get_reports_list()
    except Exception as exc:
        print(f"  Test 2 raised an unexpected error: {exc}")
        traceback.print_exc()

    try:
        report_local_pipeline_activity()
    except Exception as exc:
        print(f"  Test 3 raised an unexpected error: {exc}")
        traceback.print_exc()

    banner("DONE — paste the entire output above into the chat")
    return 0


if __name__ == "__main__":
    sys.exit(main())
