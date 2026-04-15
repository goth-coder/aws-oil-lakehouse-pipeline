import sys
import logging

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql.functions import avg, col, row_number, when, year
from awsglue.dynamicframe import DynamicFrame

# ── Logger setup ──────────────────────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Args ──────────────────────────────────────────────────────────────────────
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "S3_BUCKET", "RAW_PREFIX", "PROCESSED_PREFIX"],
)

logger.info(f"🚀 Starting job: {args['JOB_NAME']}")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

S3_BUCKET = args["S3_BUCKET"]
RAW_PREFIX = args["RAW_PREFIX"].rstrip("/")
PROCESSED_PREFIX = args["PROCESSED_PREFIX"].rstrip("/")

raw_path = f"s3://{S3_BUCKET}/{RAW_PREFIX}/"
processed_path = f"s3://{S3_BUCKET}/{PROCESSED_PREFIX}/"

logger.info(f"📥 Reading raw data from: {raw_path}")

# ── Read raw data ──────────────────────────────────────────────────────────────
raw_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [raw_path], "recurse": True},
    format="parquet",
)

df = raw_dyf.toDF()

logger.info(f"📊 Raw data count: {df.count()}")
logger.info(f"📊 Raw schema: {df.schema}")

# ── Ordering ──────────────────────────────────────────────────────────────────
logger.info("🔄 Ordering data by date")

df = df.orderBy("date")

# ── Compute moving average ────────────────────────────────────────────────────
logger.info("🧠 Computing 7-day moving average")

w_avg = Window.orderBy("date").rowsBetween(-6, 0)
w_rn = Window.orderBy("date")

df = (
    df.withColumn("_rn", row_number().over(w_rn))
    .withColumn("_raw_avg", avg(col("price_usd")).over(w_avg))
    .withColumn(
        "moving_avg_7d",
        when(col("_rn") >= 7, col("_raw_avg")).otherwise(None),
    )
    .drop("_rn", "_raw_avg")
)

# ── Add year partition ────────────────────────────────────────────────────────
logger.info("📅 Adding year partition column")

df = df.withColumn("year", year(col("date")))

# Reduzir small files
df = df.repartition(1, "year")

logger.info(f"📊 Processed data count: {df.count()}")

logger.info("🔍 Sample data:")
df.show(5, truncate=False)

# ── Convert to DynamicFrame ───────────────────────────────────────────────────
processed_dyf = DynamicFrame.fromDF(df, glueContext, "processed_dyf")

# ── Write ─────────────────────────────────────────────────────────────────────
logger.info(f"💾 Writing processed data to: {processed_path}")

sink = glueContext.getSink(
    connection_type="s3",
    path=processed_path,
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["year"],
)

sink.setFormat("glueparquet")
sink.setCatalogInfo(
    catalogDatabase="default",
    catalogTableName="oil_price_processed",
)

sink.writeFrame(processed_dyf)

logger.info("✅ Write completed successfully")

# ── Commit ────────────────────────────────────────────────────────────────────
job.commit()

logger.info("🏁 Job finished successfully")