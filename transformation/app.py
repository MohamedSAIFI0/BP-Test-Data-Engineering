from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import os
from dotenv import load_dotenv

load_dotenv()
spark = SparkSession.builder.appName("my-app").master("local[*]").getOrCreate()

path = os.getenv("DATA_PATH")
output_path = os.getenv("OUTPUT_PATH")
orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("price", FloatType(),True),
    StructField("order_date", DataType(), True)
])

if not path:
    raise ValueError("DATA_PATH is not defined in .env")

if not output_path:
    raise ValueError("OUTPUT_PATH is not defined in .env")

def extract_data():
    df = spark.read.option("header","true").option("dateFormat","yyyy-MM-dd").schema(orders_schema).csv(r"path")
    return df

def clean_data(df):
    df = df.dropna(
        subset=["order_id", "price", "order_date"]
    )
    df = df.dropDuplicates(
        ["order_id"]
    )

    df = df.withColumn(
        "customer_name",
        F.lower(F.trim(F.col("customer_name")))
    )

    df = df.withColumn(
        "address",
        F.lower(F.trim(F.col("address")))
    )

    return df

def load_data(df):
    df.write.option("header","true").mode("overwrite").csv(r"output_path")
    