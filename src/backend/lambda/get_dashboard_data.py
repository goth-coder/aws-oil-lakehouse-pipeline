"""
Lambda: GET /dashboard/data

Returns aggregated dashboard data derived from Athena queries against
default.oil_price_processed.

Cache strategy:
  - Cache hit:  s3://<BUCKET>/cache/dashboard_data.json exists → return body
  - Cache miss: execute 3 Athena queries → assemble → write cache → return

Cache invalidation: Lambda get_status.py deletes the cache object when the
Glue Job reports SUCCEEDED.

Returns:
  {
    "kpis": {
      "currentPrice": number,
      "deltaPercent": number,
      "ma7d": number | null,
      "high52w": number,
      "low52w": number
    },
    "series": [{"date": "YYYY-MM-DD", "price_usd": number, "moving_avg_7d": number | null}, ...],
    "monthlyAvg": [{"month": "YYYY-MM", "avg": number}, ...]
  }
"""

import csv
import json
import os
import time
from io import StringIO

import boto3

S3_BUCKET = os.environ["S3_BUCKET"]
ATHENA_OUTPUT_LOCATION = os.environ.get(
    "ATHENA_OUTPUT_LOCATION",
    f"s3://{S3_BUCKET}/athena-results/",
)
CACHE_KEY = "cache/dashboard_data.json"
DATABASE = "default"
TABLE = "oil_price_processed"

s3 = boto3.client("s3")
athena = boto3.client("athena")

# ── Athena queries ─────────────────────────────────────────────────────────────

QUERY_KPIS = f"""
WITH windowed AS (
  SELECT
    "date",
    price_usd,
    moving_avg_7d,
    LAG(price_usd, 1) OVER (ORDER BY "date" ASC)   AS prev_price,
    MAX(price_usd)    OVER (ORDER BY "date" ASC ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) AS high_52w,
    MIN(price_usd)    OVER (ORDER BY "date" ASC ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) AS low_52w
  FROM {DATABASE}.{TABLE}
)
SELECT price_usd AS current_price, moving_avg_7d AS ma7d,
       prev_price, high_52w, low_52w
FROM windowed
ORDER BY "date" DESC
LIMIT 1
"""

QUERY_SERIES = f"""
SELECT "date", MAX(price_usd) AS price_usd, MAX(moving_avg_7d) AS moving_avg_7d
FROM {DATABASE}.{TABLE}
GROUP BY "date"
ORDER BY "date" ASC
"""

QUERY_MONTHLY = f"""
SELECT
  CONCAT(CAST(year AS VARCHAR), '-', LPAD(CAST(month AS VARCHAR), 2, '0')) AS month,
  AVG(price_usd) AS avg
FROM {DATABASE}.{TABLE}
WHERE "date" >= DATE_FORMAT(DATE_ADD('month', -12, CURRENT_DATE), '%Y-%m-%d')
GROUP BY year, month
ORDER BY year ASC, month ASC
"""


def _run_athena_query(sql: str) -> tuple[list[dict], str]:
    """Start an Athena query, poll until completion, return (rows, execution_id)."""
    start = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_LOCATION},
    )
    execution_id = start["QueryExecutionId"]

    for _ in range(60):  # max 60 × 1s = 60s per query
        result = athena.get_query_execution(QueryExecutionId=execution_id)
        state = result["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            reason = result["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown"
            )
            raise RuntimeError(
                f"Athena query {execution_id} {state}: {reason}"
            )
        time.sleep(1)
    else:
        raise TimeoutError(
            f"Athena query {execution_id} did not complete within 60 seconds."
        )

    # Fetch results from the S3 output CSV
    output_key = f"athena-results/{execution_id}.csv"
    obj = s3.get_object(Bucket=S3_BUCKET, Key=output_key)
    body = obj["Body"].read().decode("utf-8")
    reader = csv.DictReader(StringIO(body))
    return list(reader), execution_id


def _build_payload(
    kpi_rows: list[dict],
    series_rows: list[dict],
    monthly_rows: list[dict],
) -> dict:
    """Assemble the final JSON payload from Athena CSV results."""
    kpi = kpi_rows[0] if kpi_rows else {}

    current_price = float(kpi.get("current_price") or 0)
    prev_price = float(kpi.get("prev_price") or 0)
    delta_percent = (
        round((current_price / prev_price - 1) * 100, 2)
        if prev_price
        else 0.0
    )
    ma7d_raw = kpi.get("ma7d")
    ma7d = float(ma7d_raw) if ma7d_raw not in (None, "", "null") else None

    kpis = {
        "currentPrice": round(current_price, 2),
        "deltaPercent": delta_percent,
        "ma7d": round(ma7d, 2) if ma7d is not None else None,
        "high52w": round(float(kpi.get("high_52w") or 0), 2),
        "low52w": round(float(kpi.get("low_52w") or 0), 2),
    }

    series = []
    for row in series_rows:
        ma_raw = row.get("moving_avg_7d")
        series.append(
            {
                "date": row["date"],
                "price_usd": round(float(row["price_usd"]), 2),
                "moving_avg_7d": (
                    round(float(ma_raw), 2)
                    if ma_raw not in (None, "", "null")
                    else None
                ),
            }
        )

    monthly_avg = [
        {"month": row["month"], "avg": round(float(row["avg"]), 2)}
        for row in monthly_rows
    ]

    return {"kpis": kpis, "series": series, "monthlyAvg": monthly_avg}


def lambda_handler(event, context):  # noqa: ANN001
    # ── Cache hit ──────────────────────────────────────────────────────────────
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=CACHE_KEY)
        cached_body = obj["Body"].read().decode("utf-8")
        return {
            "statusCode": 200,
            "headers": {"Access-Control-Allow-Origin": "*"},
            "body": cached_body,
        }
    except s3.exceptions.NoSuchKey:
        pass  # cache miss — proceed to Athena queries
    except Exception:  # noqa: BLE001
        pass  # treat any cache read error as a miss

    # ── Cache miss: run Athena queries ────────────────────────────────────────
    try:
        kpi_rows, kpi_query_id = _run_athena_query(QUERY_KPIS)
        series_rows, _ = _run_athena_query(QUERY_SERIES)
        monthly_rows, _ = _run_athena_query(QUERY_MONTHLY)

        payload = _build_payload(kpi_rows, series_rows, monthly_rows)
        payload["queryId"] = kpi_query_id
        body = json.dumps(payload)

        # Write to cache
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=CACHE_KEY,
            Body=body.encode("utf-8"),
            ContentType="application/json",
        )

        return {
            "statusCode": 200,
            "headers": {"Access-Control-Allow-Origin": "*"},
            "body": body,
        }

    except Exception as exc:  # noqa: BLE001
        return {
            "statusCode": 500,
            "headers": {"Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"error": str(exc)}),
        }
