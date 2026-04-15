"""
Lambda: POST /pipeline/run

Scrapes historical Brent oil prices from IPEA, writes Parquet to S3 raw
(partitioned by date, idempotent), then starts the Glue ETL Job.

Dependencies in Lambda Layer: requests, pandas, pyarrow  (via AWSSDKPandas-Python311)
No s3fs needed — uses boto3 (pre-installed in Lambda runtime) for S3 writes.

Returns: { "jobRunId": "<id>" }  HTTP 200
         { "error": "<message>" } HTTP 500
"""

import io
import json
import os
from io import StringIO

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

IPEA_URL = (
    "http://www.ipeadata.gov.br/ExibeSerie.aspx"
    "?module=m&serid=1650971490&oper=view"
)

S3_BUCKET = os.environ["S3_BUCKET"]
GLUE_JOB_NAME = os.environ["GLUE_JOB_NAME"]

s3 = boto3.client("s3")
glue = boto3.client("glue")


def _scrape_ipea() -> pd.DataFrame:
    """Fetch IPEA page and parse the grd_DXMainTable into a DataFrame."""
    response = requests.get(IPEA_URL, timeout=30)
    response.raise_for_status()

    tables = pd.read_html(StringIO(response.text), attrs={"id": "grd_DXMainTable"})
    if not tables:
        raise ValueError(
            "Table 'grd_DXMainTable' not found in IPEA HTML response."
        )

    return tables[0].copy()


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns, parse dates, cast price, add partition columns."""
    df.columns = ["date", "price_usd"]
    df = df.dropna(subset=["date", "price_usd"])

    # Convert "DD/MM/YYYY" → "YYYY-MM-DD"
    df["date"] = pd.to_datetime(
        df["date"], format="%d/%m/%Y", errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    df = df.dropna(subset=["date"])

    # Normalize comma decimal separator if present
    df["price_usd"] = (
        df["price_usd"]
        .astype(str)
        .str.replace(",", ".", regex=False)
        .astype(float)
    )

    df = df[df["price_usd"] > 0].copy()

    # IPEA reports Brent in US¢/barrel — convert to US$/barrel
    df["price_usd"] = (df["price_usd"] / 100).round(2)

    dt = pd.to_datetime(df["date"], format="%Y-%m-%d")
    df["year"] = dt.dt.year.astype(str)
    df["month"] = dt.dt.month.apply(lambda m: f"{m:02d}")
    df["day"] = dt.dt.day.apply(lambda d: f"{d:02d}")

    return df.reset_index(drop=True)


def _write_parquet(df: pd.DataFrame) -> None:
    """
    Write one Parquet file per year to S3 using boto3 directly.

    Path: s3://<BUCKET>/raw/year=<Y>/data.parquet
    Partitioning by year (~40 files) instead of by day (~10 000 files)
    keeps the Lambda well within API Gateway's 29-second timeout.
    Idempotent: s3.put_object overwrites the existing object at the same key.
    """
    for year, group in df.groupby("year"):
        s3_key = f"raw/year={year}/data.parquet"

        partition_df = group[["date", "price_usd", "year", "month", "day"]]
        table = pa.Table.from_pandas(partition_df, preserve_index=False)

        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=buffer.getvalue(),
        )


def lambda_handler(event, context):  # noqa: ANN001
    try:
        df_raw = _scrape_ipea()
        df = _normalize(df_raw)
        _write_parquet(df)

        response = glue.start_job_run(JobName=GLUE_JOB_NAME)
        job_run_id = response["JobRunId"]

        return {
            "statusCode": 200,
            "headers": {"Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"jobRunId": job_run_id}),
        }

    except Exception as exc:  # noqa: BLE001
        return {
            "statusCode": 500,
            "headers": {"Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"error": str(exc)}),
        }
