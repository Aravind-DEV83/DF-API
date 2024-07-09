from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

def initialize_spark():

    return SparkSession.builder. \
                        appName('Different Data Sources'). \
                        getOrCreate()

if __name__ == "__main__":
    spark = initialize_spark()

    flightSchemaStruct = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])

    flightSchemaDDL = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
          ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
          WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT """

    rawCsvDf = spark.read. \
                    format("csv"). \
                    option("inferSchema", "true"). \
                    option("header", "true"). \
                    schema(flightSchemaStruct). \
                    option("mode", "FAILFAST"). \
                    option("dateFormat", "M/d/y"). \
                    load("data/flight*.csv")
    
    rawCsvDf.show(5)
    print(rawCsvDf.schema)

    rawJsonDf = spark.read. \
                format("json"). \
                schema(flightSchemaDDL). \
                option("dateFormat", "M/d/y"). \
                load("data/flight*.json") 
    
    rawJsonDf.show(5)
    print(rawJsonDf.schema)

    rawParquetDf = spark.read. \
                format("parquet"). \
                load("data/flight*.parquet")

    rawParquetDf.show(5)
    print(rawParquetDf.schema)