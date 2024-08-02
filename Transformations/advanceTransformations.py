from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import when, col, sum, round, to_date, to_timestamp, row_number, rank, date_format, dense_rank

def initialize_spark():
    return SparkSession.builder. \
                        appName("Advance DataFrame Transformations"). \
                        enableHiveSupport(). \
                        getOrCreate()

if __name__ == "__main__":
    spark = initialize_spark()

    raw_df = spark.read. \
                    format("csv"). \
                    option("header", "true"). \
                    option("inferSchema", "true"). \
                    load("data/invoices.csv")

    raw_df.show(1)

    parsed_df = raw_df.withColumn("InvoiceDate", to_date(to_timestamp(raw_df["InvoiceDate"], "dd-MM-yyyy H.mm")))


    daily_revenue = parsed_df. \
                        groupBy(col("InvoiceDate")). \
                        agg(
                            round(sum(col("UnitPrice")),2).alias("Total_Revenue")
                        ). \
                        orderBy(col("InvoiceDate"))
    
    daily_revenue.show()
    # Global Ranks - orderBy
    global_window_spec = Window.orderBy(col("UnitPrice").desc())
    parsed_df.withColumn("rank1", rank().over(global_window_spec)). \
                orderBy(col("UnitPrice").desc()). \
                show()

    # Ranks with each partition or group - partitionBy and orderBy
    window_spec = Window.partitionBy("InvoiceDate").orderBy(col("UnitPrice").desc())
    parsed_df.filter(date_format(parsed_df["InvoiceDate"], "yyyyMM") == 201012). \
                withColumn("rank2", rank().over(window_spec)). \
                withColumn("drnk", dense_rank().over(window_spec)). \
                withColumn("row_num", row_number().over(window_spec)). \
                orderBy(col("UnitPrice").desc()). \
                filter("rank2 <=5"). \
                show()
    
    # Filter based on dense ranks per partition
    parsed_df.filter("InvoiceDate BETWEEN '2011-01-01' AND '2011-03-31' "). \
                withColumn("drank3", dense_rank().over(window_spec)). \
                filter("drank3 <=5"). \
                show()
    

