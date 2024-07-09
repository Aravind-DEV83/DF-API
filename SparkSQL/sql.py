from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

def initialize_spark():

    return SparkSession.builder. \
                        appName('Different Data Sources'). \
                        getOrCreate()

if __name__ == "__main__":
    spark = initialize_spark()

    rawParquetDf = spark.read. \
            format("parquet"). \
            load("dataSource/flight*.parquet")
    rawParquetDf.show()