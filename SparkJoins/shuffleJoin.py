from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, regexp_extract, substring_index, trim, coalesce
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

def initialize_spark():

    return SparkSession.builder. \
                        appName('Join Examples'). \
                        master("local[3]"). \
                        getOrCreate()


if __name__ == "__main__":
    spark = initialize_spark()

    flight_df1 = spark.read.json("data/d1/")
    flight_df2 = spark.read.json("data/d2/")

    spark.conf.set("spark.sql.shuffle.partitions", 3)

    join_expr = flight_df1.id == flight_df2.id

    joined_df = flight_df1.join(flight_df2, join_expr, "inner")

    joined_df.foreach(lambda x: None)


