from src import *
from pyspark.sql import SQLContext

_spark = get_spark()
sqlContext = SQLContext(_spark.sparkContext)


def read_phoenix_table(table_name):
    df = sqlContext.read \
        .format("org.apache.phoenix.spark") \
        .option("table", table_name) \
        .option("zkUrl", PHOENIX_ZK_URL) \
        .load()
    return df


def write_phoenix_table(df, table_name):
    df.write \
        .format("org.apache.phoenix.spark") \
        .mode("overwrite") \
        .option("table", table_name) \
        .option("zkUrl", PHOENIX_ZK_URL) \
        .save()
