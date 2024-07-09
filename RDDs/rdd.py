import sys
from lib import logger
from collections import namedtuple
from pyspark.sql import SparkSession
from pyspark import SparkConf

SurveyRecord = namedtuple("SurveyRecord", ["Age", "Gender", "Country", "State"])

def intialize_spark():
    
    conf = SparkConf().setMaster("local[3]"). \
                    setAppName("RDD's Introduction")

    return SparkSession.builder. \
                        config(conf=conf). \
                        getOrCreate()

if __name__ == "__main__":
    spark = intialize_spark()
    sc = spark.sparkContext

    Logger = logger.Log4j(spark)

    if len(sys.argv) != 2:
        print('Error')
        logger.error("Usage: HelloSpark <filename>")
    
    linesRDD = sc.textFile(sys.argv[1])
    partitionedRDD = linesRDD.repartition(2)

    colsRDD = partitionedRDD.map(
        lambda line: line.replace('"', '').split(",")
    )

    selectRDD = colsRDD.map(
        lambda cols: SurveyRecord(int(cols[1]), cols[2], cols[3], cols[4])
    )

    filteredRDD = selectRDD.filter(lambda r : r.Age < 40)


    kvRDD = filteredRDD.map(lambda r: (r.Country, 1))

    countRDD = kvRDD.reduceByKey(lambda v1, v2: v1 + v2)

    colsList = countRDD.collect()
    for i in colsList:
        Logger.info(i)



    


