from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp, round, countDistinct, expr, count
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType


if __name__ == "__main__":
    spark = SparkSession.builder. \
                        master("local[4]"). \
                        appName("Processing Fire Department & Emergency Medical Services"). \
                        getOrCreate()
    
    raw_df = spark.read. \
                    format("csv"). \
                    option("header", "true"). \
                    option("inferSchema", "true"). \
                    load("data/Fire*.csv")
    
    renamed_df = raw_df. \
                        withColumnRenamed("Call Number", "CallNumber"). \
                        withColumnRenamed("Unit ID", "UnitID"). \
                        withColumnRenamed("Incident Number", "IncidentNumber"). \
                        withColumnRenamed("Call Date", "CallDate"). \
                        withColumnRenamed("Watch Date", "WatchDate"). \
                        withColumnRenamed("Call Final Disposition", "CallFinalDisposition"). \
                        withColumnRenamed("Available DtTm", "AvailableDtTm"). \
                        withColumnRenamed("Zipcode of Incident", "Zipcode"). \
                        withColumnRenamed("Station Area", "StationArea"). \
                        withColumnRenamed("Final Priority", "FinalPriority"). \
                        withColumnRenamed("ALS Unit", "ALSUnit"). \
                        withColumnRenamed("Call Type Group", "CallTypeGroup"). \
                        withColumnRenamed("Unit Sequence in call dispatch", "UnitSequenceInCallDispatch"). \
                        withColumnRenamed("Fire Prevention District", "FirePreventionDistrict"). \
                        withColumnRenamed("Superivisor District", "SupervisorDistrict")
    
    parsed_df = renamed_df. \
                        withColumn("CallDate", to_date(col("CallDate"), "MM/dd/yyyy")). \
                        withColumn("WatchDate", to_date(col("WatchDate"), "MM/dd/yyyy")). \
                        withColumn("AvailableDtTm", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))
    
    #1. How many distinct type of calls were made to the fire department?

    distinctCount_df = parsed_df. \
        where("'Call Type' is not null"). \
        agg(
            countDistinct("Call Type").alias("CountDistinctCallTypes")
        )

    distinctCount_df.show(1)

    #2. What were the distinct types of calls made to the fire department?

    distinctType_df = parsed_df. \
                            where(" 'Call Type' is not null "). \
                            select(col("Call Type").alias("DistinctCallTypes")). \
                            distinct()

    distinctType_df.show()

    #3. What were the most common Call Types

    CommonCallTypes_df = parsed_df. \
                                where(" 'Call Type' is not null "). \
                                select(col("Call Type")).\
                                groupBy(col("Call Type")). \
                                count(). \
                                orderBy("count", ascending=False)
    CommonCallTypes_df.show()

    # 4. What Zipcodes accounted for most common calls?
    Zipcode_df = parsed_df. \
                        select("Zipcode"). \
                        groupBy("Zipcode"). \
                        agg(
                            count("Zipcode").alias("CallCount")
                        ). \
                        orderBy("CallCount", ascending=False)
                        

    Zipcode_df.show()

    #5. What SanFransico neighbours are in Zipcodes 94102 and 94103






    
    

