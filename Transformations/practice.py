from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, regexp_extract, substring_index, trim, lit, expr, sum, max, min
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

def initialize_spark():

    return SparkSession.builder. \
                        appName('Pyspark Practice'). \
                        enableHiveSupport(). \
                        getOrCreate()


if __name__ == "__main__":
    spark = initialize_spark()

    raw_df = spark.read. \
                    format("csv"). \
                    option("inferSchema", "true"). \
                    option("header", "true"). \
                    load("data/flight*.csv")

    # 1. Renaming col names
    renamed_df = raw_df.withColumnRenamed("FL_DATE", "FLIGHT_DATE")
    renamed_df.show(1)

    renamed2_df = raw_df.selectExpr("FL_DATE as FLIGHT_DATE", "DISTANCE as DIST")
    renamed2_df.show(1)

    renamed3_df = raw_df.select(col("FL_DATE").alias("FLIGHT_DATE"), col("DISTANCE").alias("DIST"))
    renamed3_df.show(1)

    # 2. Add new Columns
    newColumns_df = raw_df. \
                    withColumn("Country", lit("US")). \
                    withColumn("Cost", expr("DISTANCE * DEP_TIME"))
    newColumns_df.show(3)

    # 3. Filtering data
    filtered_df = raw_df.filter((col("ORIGIN") != 'BOS') & (col("DISTANCE") > 449))
    filtered_df.show(3)

    # 4, Sorting data
    sorted_df = raw_df.orderBy(col("DISTANCE").desc())
    sorted_df.show(3)

    # 5. Find & Remove duplicates
    duplicates_df = raw_df. \
                        groupBy(["FL_DATE", "OP_CARRIER"]). \
                        count(). \
                        where("count > 1")
    duplicates1_df = raw_df.exceptAll(raw_df.dropDuplicates(['FL_DATE', 'OP_CARRIER'])).show()
    duplicates2_df = raw_df.distinct()
    duplicates2_df.show(3)

    # 6. GroupBy
    grouped_df = raw_df. \
                    groupBy("FL_DATE"). \
                    agg(
                        sum("DISTANCE").alias("TOTAL_DISTANCE"), 
                        max("DISTANCE").alias("MAX_DISTANCE"),
                        min("DISTANCE").alias("MIN_DISTANCE"))
    grouped_df.show()

    # 7. Write to a file
    
    duplicates2_df.write. \
                    format("csv"). \
                    mode("overwrite"). \
                    option("path", "output/"). \
                    partitionBy("OP_CARRIER", "ORIGIN"). \
                    option("maxRecordsPerFile", 10000). \
                    save()
    
    

     

