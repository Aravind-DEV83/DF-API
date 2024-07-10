from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

def initialize_spark():

    return SparkSession.builder. \
                        appName('SparkSQLTables'). \
                        enableHiveSupport(). \
                        getOrCreate()

if __name__ == "__main__":
    spark = initialize_spark()

    rawParquetDf = spark.read. \
            format("parquet"). \
            load("dataSource/flight*.parquet")
    rawParquetDf.show()

    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    print("AIRLINE_DB is set....")
    spark.sql("Select CURRENT_DATABASE()").show()

    rawParquetDf.write. \
                format("csv"). \
                mode("overwrite"). \
                bucketBy(5, "OP_CARRIER", "ORIGIN"). \
                sortBy("OP_CARRIER", "ORIGIN"). \
                saveAsTable("flight_data_tbl")
    print("Managed table flights_data_tbl created sucessfully...")
    
    spark.sql("DROP TABLE IF EXISTS airline_db.unmanaged_flights_tbl").show()
    print("Dropping the table unmanaged_flights-tbl if exists...")

    spark.sql(
        """
            CREATE TABLE IF NOT EXISTS unmanaged_flights_tbl (
                FL_DATE STRING, 
                OP_CARRIER STRING, 
                OP_CARRIER_FL_NUM INT, 
                ORIGIN STRING, 
                ORIGIN_CITY_NAME STRING, 
                DEST STRING, 
                DEST_CITY_NAME STRING, 
                CRS_DEP_TIME INT, 
                DEP_TIME INT, 
                WHEELS_ON INT, 
                TAXI_IN INT, 
                CRS_ARR_TIME INT, 
                ARR_TIME INT, 
                CANCELLED INT, 
                DISTANCE INT
            )
            USING CSV 
            OPTIONS(path='/Users/aravind_jarpala/Downloads/Pyspark/DF-API/SparkSQL/data/flight-time.csv')
        """
    )

    spark.sql("SELECT * FROM unmanaged_flights_tbl").show()

    spark.sql(""" 
              CREATE OR REPLACE GLOBAL TEMP VIEW US_ORIGIN_SFO_GLOBAL_TEMP_VIEW AS 
              SELECT FL_DATE, OP_CARRIER, ORIGIN, DEST FROM unmanaged_flights_tbl 
              WHERE ORIGIN = 'SFO' 
             """
            ).show()
    
    print("Views.....")
    spark.sql("select * from global_temp.US_ORIGIN_SFO_GLOBAL_TEMP_VIEW ").show()

    print(spark.catalog.listTables("AIRLINE_DB"))
    print(spark.catalog.listColumns("unmanaged_flights_tbl"))

    spark.sql(" DROP VIEW IF EXISTS US_ORIGIN_SFO_GLOBAL_TEMP_VIEW").show()


    

