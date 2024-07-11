from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

def initialize_spark():

    return SparkSession.builder. \
                        appName('SparkSQLTables'). \
                        enableHiveSupport(). \
                        getOrCreate()

def to_date_df(df, format, name):
    return df.withColumn(name, to_date(col(name), format))

if __name__ == "__main__":
    spark = initialize_spark()

    raw_df = spark.read. \
                    format("csv"). \
                    option("header", "true"). \
                    option("inferSchema", "true"). \
                    option("path", "data/flight*.csv"). \
                    load()
    
    raw_df.show()
    
    converted_df = to_date_df(raw_df, "M/d/y", 'FL_DATE')   

    converted_df.show()


