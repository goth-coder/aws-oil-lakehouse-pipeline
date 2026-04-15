"""
Lambda: GET /pipeline/status/{jobRunId}

Queries Glue for the current status of a running job.
If the job SUCCEEDED, invalidates the S3 dashboard cache so the next
GET /dashboard/data re-executes Athena and returns fresh data.

Returns:
  { "status": "RUNNING" | "SUCCEEDED" | "FAILED" }
  { "status": "FAILED", "error": "<message>" }   (on job failure)
"""

import json
import os

import boto3

S3_BUCKET = os.environ["S3_BUCKET"]
GLUE_JOB_NAME = os.environ["GLUE_JOB_NAME"]
CACHE_KEY = "cache/dashboard_data.json"

glue = boto3.client("glue")
s3 = boto3.client("s3")

# Glue JobRunState → normalised status
_STATE_MAP = {
    "STARTING": "RUNNING",
    "RUNNING": "RUNNING",
    "STOPPING": "RUNNING",
    "STOPPED": "FAILED",
    "SUCCEEDED": "SUCCEEDED",
    "FAILED": "FAILED",
    "TIMEOUT": "FAILED",
    "ERROR": "FAILED",
    "WAITING": "RUNNING",
}


def lambda_handler(event, context):  # noqa: ANN001
    try:
        job_run_id = event["pathParameters"]["jobRunId"]

        result = glue.get_job_run(
            JobName=GLUE_JOB_NAME,
            RunId=job_run_id,
        )
        run = result["JobRun"]
        glue_state = run.get("JobRunState", "UNKNOWN")
        status = _STATE_MAP.get(glue_state, "RUNNING")

        if status == "SUCCEEDED":
            # Invalidate dashboard cache so next GET /dashboard/data is fresh
            try:
                s3.delete_object(Bucket=S3_BUCKET, Key=CACHE_KEY)
            except Exception:  # noqa: BLE001
                pass  # cache bust failure is non-fatal

        body: dict = {"status": status}
        if status == "FAILED":
            error_message = run.get("ErrorMessage") or glue_state
            body["error"] = error_message

        return {
            "statusCode": 200,
            "headers": {"Access-Control-Allow-Origin": "*"},
            "body": json.dumps(body),
        }

    except Exception as exc:  # noqa: BLE001
        return {
            "statusCode": 500,
            "headers": {"Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"error": str(exc)}),
        }
