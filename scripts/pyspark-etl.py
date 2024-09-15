import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, TimestampType
import datetime

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define the schema for the raw JSON data
schema = "WoolColor STRING, ItemName STRING, ItemCount INT, GameTick INT"

# Read the raw JSON data from the S3 bucket
df = spark.read.schema(schema).json("s3://YOUR_BUCKET_NAME/2024/09/14/19/")

# Function to convert GameTick to hh:mm:ss format
def tick_to_timestamp(tick):
    seconds = tick / 20 
    timestamp = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=seconds)  # Epoch time
    return timestamp

# Use UDF to convert GameTick to a Timestamp
tick_to_timestamp_udf = udf(tick_to_timestamp, TimestampType()) 
df_with_timestamp = df.withColumn("Timestamp", tick_to_timestamp_udf(col("GameTick")))

# Drop GameTick column
df_transformed = df_with_timestamp.drop("GameTick")

# Write the transformed data to S3 in Parquet
df_transformed.write.mode("overwrite").parquet("s3://YOUR_BUCKET_NAME/processed-data/")

job.commit()