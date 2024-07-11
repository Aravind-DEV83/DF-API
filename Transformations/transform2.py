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

    raw_data = spark.read. \
                    format("text"). \
                    option("path", "data/apache_logs.txt"). \
                    load()
    
    regEx = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'

    parsed_df = raw_data.select(
        regexp_extract('value', regEx, 1).alias("ip"),
        regexp_extract('value', regEx, 4).alias("date"),
        regexp_extract('value', regEx, 6).alias("request"),
        regexp_extract('value', regEx, 10).alias("referrer")
    )

    parsed_df.show()
    parsed_df.printSchema()

    parsed_df. \
            where("trim(referrer) != '-' "). \
            withColumn("referrer", substring_index("referrer", "/", 3)). \
            groupBy("referrer"). \
            count(). \
            show(100, truncate=False)
