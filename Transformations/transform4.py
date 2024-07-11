from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, regexp_extract, substring_index, trim
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

def initialize_spark():

    return SparkSession.builder. \
                        appName('SparkSQLTables'). \
                        enableHiveSupport(). \
                        getOrCreate()


if __name__ == "__main__":
    spark = initialize_spark()

    data_list = [
        ("Ravi", 28, 1, 2002),
        ("Abdul", 23, 5, 81),
        ("John", 12, 12, 6),
        ("Rosy", 7, 8, 63),
        ("Abdul", 23, 5, 81)
    ]

    raw_df = spark.createDataFrame(data_list).toDF("name", "day", "month", "year")

    raw_df.show() 