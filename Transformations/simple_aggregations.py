from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, sum, avg, count, countDistinct, round, expr, weekofyear, to_date, cast, to_timestamp, year

def intialize_spark():
    return SparkSession.builder. \
                        appName("Simple Aggregations example"). \
                        getOrCreate()

if __name__ == "__main__":
    spark = intialize_spark()

    raw_df = spark.read. \
                    format("csv"). \
                    option("header", "true"). \
                    option("inferSchema", "true"). \
                    option("path", "data/invoices.csv"). \
                    load()
    
    raw_df.select(count('*').alias("Count All"),
                sum("Quantity").alias("Total Quantity"),
                avg("UnitPrice").alias("AvgPrice"),
                countDistinct("InvoiceNo").alias("CountDistinct")).show()
    
    raw_df.selectExpr(
        "count(1) as `Count 1`",
        "count(stockCode) as `Count Field`",
        "sum(Quantity) as `Total Quantity`",
        "avg(UnitPrice) as `AvgPrice`"
    ).show()

    raw_df.createOrReplaceTempView("sales")

    spark.sql("""

        select Country, InvoiceNo, sum(Quantity) as `Total Quantity`, round(sum(Quantity * UnitPrice), 2) as `Invoice Value`
            from sales
            group by Country, InvoiceNo

    """).show()

    summary_df = raw_df.groupBy("Country", "InvoiceNo"). \
        agg(sum("Quantity").alias("Total Quantity"),
            round(sum(expr("Quantity * UnitPrice")), 2).alias("Invoice Value")
    ).show()

    summary2_df = raw_df. \
                withColumn("InvoiceDate", to_date(col("InvoiceDate"), 'dd-MM-yyyy H.mm')). \
                where("year(InvoiceDate) == 2010"). \
                withColumn("WeekNumber", weekofyear(col("InvoiceDate"))). \
                groupBy("Country", "WeekNumber"). \
                agg(
                    countDistinct("InvoiceNo").alias("NumInvoices"),
                    sum("Quantity").alias("Total Quantity"),
                    round(sum(expr("Quantity * UnitPrice")), 2).alias("Invoice Value")
                )
    
    my_window = (Window.partitionBy("Country"). \
                orderBy('WeekNumber'). \
                rowsBetween(Window.unboundedPreceding, Window.currentRow))
    
    final_df = summary2_df. \
                withColumn("Running Total", round(sum("Invoice Value").over(my_window), 2))

    # summary2_df. sort("Country", "WeekNumber").show()
    final_df.show()