from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName("my-app").master("local[*]").getOrCreate()

def extract_data():
    df = spark.read.option("header","true").option("inferSchema","true").csv(r"data\orders.csv")
    return df

def transform_data(df):
    df = df.dropna()
    df = df.dropDuplicates()
    return df

def load_data(df):
    df.write.option("header","true").mode("overwrite").csv(r"data\clean_data.csv")
    