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

    print(rawParquetDf.rdd.getNumPartitions())
    rawParquetDf.groupBy(spark_partition_id()).count().show()

    partitionedDf = rawParquetDf.repartition(5)

    print(partitionedDf.rdd.getNumPartitions())
    partitionedDf.groupBy(spark_partition_id()).count().show()


    ''' partitionedDf.write. \
                format("avro"). \
                mode("overwrite"). \
                option("path", "avro/"). \
                save()
    '''

    rawParquetDf.write. \
                format("json"). \
                mode("overwrite"). \
                option("path", "avro/"). \
                partitionBy("OP_CARRIER", "ORIGIN"). \
                option("maxRecordsPerFile", 10000). \
                save()


