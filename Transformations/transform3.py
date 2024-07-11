from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, expr, udf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
import re

def initialize_spark():

    return SparkSession.builder. \
                        appName('SparkSQLTables'). \
                        enableHiveSupport(). \
                        getOrCreate()

def gender_transform(gender):

    f_pattern = r'^f$|f.m|w.m'
    m_pattern = r'^m$|ma|m.l'

    if re.search(f_pattern, gender.lower()):
        return "Female"
    elif re.search(m_pattern, gender.lower()):
        return "Male"
    else:
        return "Unknwon"

if __name__ == "__main__":
    spark = initialize_spark()

    raw_df = spark.read. \
                    format("csv"). \
                    option("header", "true"). \
                    option("inferSchema", "true"). \
                    option("path", "data/survey.csv"). \
                    load()
    
    # raw_df.show(5)
    
    #Column Object Expression
    # parse_gender_udf = udf(gender_transform, StringType())
    # print("Catalog Entry: ")
    # [print(i) for i in spark.catalog.listFunctions() if "gender_transform" in i.name]
    # transformed_df = raw_df.withColumn("Gender", parse_gender_udf("Gender"))

    # transformed_df.show(3)

    # #SQL Expression
    spark.udf.register("parse_gender_udf", gender_transform, StringType())
    print("Catalog Entry: ")
    [print(i) for i in spark.catalog.listFunctions() if "parse_gender_udf" in i.name]
    # print(spark.catalog.listFunctions())
    transformed_df = raw_df.withColumn("Gender", expr("parse_gender_udf(Gender)"))

    transformed_df.show(3)





