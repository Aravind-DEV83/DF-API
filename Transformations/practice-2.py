from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import when, lit
from functools import reduce

def initialize_spark():
    return SparkSession.builder. \
                        appName("Pyspark practice 2"). \
                        enableHiveSupport(). \
                        getOrCreate()

def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)

def merge(df_main, df_upd, df_main_key, df_upd_key):

    df_main_cols = df_main.columns
    df_upd_cols = df_upd.columns

    add_suffix = [ df_upd[f'{col_name}'].alias(col_name + "_tmp") for col_name in df_upd_cols]
    
    df_upd = df_upd.select(*add_suffix)

    condition = df_main["id"] == df_upd["id_tmp"]
    joined_df = df_main.join(df_upd, condition, "fullouter")

    for col_name in df_main_cols:
        joined_df = joined_df.withColumn(col_name, 
                        when(
                        (df_main[col_name] != df_upd[col_name + '_tmp']) | (df_main[col_name].isNull()), df_upd[col_name + '_tmp']).otherwise(df_main[col_name]))
    joined_df.show()

    df_final = joined_df.select(*df_main_cols)

    return df_final

if __name__ == "__main__":
    spark = initialize_spark()

    # 1. Union multiple dataframes

    df1 = spark.createDataFrame([[1,1], [2, 2]], ['a', 'b'])

    df2 = spark.createDataFrame([[3, 333], [4, 444], [666, 6]], ['b', 'a']) 
    df3 = spark.createDataFrame([[555, 5], [666, 6]], ['b', 'a']) 
    
    unioned_df = df1.unionByName(df2, allowMissingColumns=True)
    unioned_df.show()
    unioned2_df = unionAll(df1, df2, df3).show()


    # 2. Merge two data frames with updates and insert
    my_schema = ['id', 'name', 'country']

    first_df = spark.createDataFrame(data=[[1, 'Hunter Marquez', 'South Korea']
                               ,[2, 'Cameran Blevins', 'Poland']
                               ,[3, 'Tiger Mckee', 'Brazil']
                               ,[4, 'John Fuller', 'Mexico']
                               ,[5, 'Isaiah Ryan', 'China']], schema=my_schema)
    
    second_df = spark.createDataFrame(data=[[1, 'Hunter M.', 'South Korea']
                               ,[2, 'Cameran B.', 'Germany']
                               ,[4, 'John F.', 'Mexico']
                               ,[5, 'Isaiah Ryan', 'South Korea']
                               ,[6, 'Aspen Wiley', 'Peru']
                               ,[7, 'Meredith Mosley', 'Belgium']
                               ,[8, 'Madaline Duke', 'Turkey']
                               ], schema=my_schema)
    
    result = merge(first_df, second_df, first_df["id"], second_df["id"])

    result.show()




