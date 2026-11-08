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

def extract_data():
    df = spark.read.option("header","true").option("dateFormat","yyyy-MM-dd").schema(orders_schema).csv(r"path")
    return df

def transform_data(df):
    df = df.dropna()
    df = df.dropDuplicates()
    return df

def load_data(df):
    df.write.option("header","true").mode("overwrite").csv(r"output_path")
    