import datetime, pdb, time
import polars as po
import polars
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import databricks.koalas as ks
import databricks
from tools import timeit

import argparse

def spark_read_csv(
        spark: pyspark.sql.session.SparkSession.Builder, 
        path: str
    ) -> pyspark.sql.dataframe.DataFrame:
    return spark.read.options(header='True',  delimiter=',').csv(path)

#@timeit
#def spark_join_test() -> pyspark.sql.dataframe.DataFrame:
@timeit
def spark_join_test() -> None:
    spark = SparkSession.builder.master("local[*]")\
                .config('spark.driver.memory','20g')\
                .appName("read system data").getOrCreate()

    spark_read_csv(spark, "/home/chihwei/playground/data/node_memory_Active_bytes*").createOrReplaceTempView("Active")
    spark_read_csv(spark, "/home/chihwei/playground/data/node_memory_Cached_bytes*").createOrReplaceTempView("Cached")
    spark_read_csv(spark, "/home/chihwei/playground/data/node_memory_MemFree_bytes*").createOrReplaceTempView("MemFree")
    spark_read_csv(spark, "/home/chihwei/playground/data/node_memory_MemTotal_bytes*").createOrReplaceTempView("MemTotal")

    output = spark.sql("""
        SELECT a.instance,a.group,a.timestamp,a.node_memory_Active_bytes,c.node_memory_Cached_bytes,
            mf.node_memory_MemFree_bytes,mt.node_memory_MemTotal_bytes
        FROM Active a
        LEFT JOIN Cached c ON
            a.instance = c.instance AND a.group = c.group AND a.timestamp = c.timestamp
        LEFT JOIN MemFree mf ON
            a.instance = mf.instance AND a.group = mf.group AND a.timestamp = mf.timestamp
        LEFT JOIN MemTotal mt ON
            a.instance = mt.instance AND a.group = mt.group AND a.timestamp = mt.timestamp
    """)

    #return output
    output.show()
    spark.stop()

#@timeit
#def koalas_join_test() -> databricks.koalas.frame.DataFrame:
@timeit
def koalas_join_test() -> None:
    """
    # set memory on koalas
    https://koalas.readthedocs.io/en/latest/user_guide/best_practices.html#leverage-pyspark-apis
    # koalas sql join
    https://www.databricks.com/blog/2020/03/31/10-minutes-from-pandas-to-koalas-on-apache-spark.html
    """
    builder = SparkSession.builder.appName("Koalas")\
                  .config('spark.driver.memory','20g')

    builder.getOrCreate()

    Active = ks.read_csv("/home/chihwei/playground/data/node_memory_Active_bytes*")
    Cached = ks.read_csv("/home/chihwei/playground/data/node_memory_Cached_bytes*")
    MemFree = ks.read_csv("/home/chihwei/playground/data/node_memory_MemFree_bytes*")
    MemTotal = ks.read_csv("/home/chihwei/playground/data/node_memory_MemTotal_bytes*")

    output = ks.sql("""
        SELECT a.instance,a.group,a.timestamp,a.node_memory_Active_bytes,c.node_memory_Cached_bytes,
            mf.node_memory_MemFree_bytes,mt.node_memory_MemTotal_bytes
        FROM {Active} a
        LEFT JOIN {Cached} c ON
            a.instance = c.instance AND a.group = c.group AND a.timestamp = c.timestamp
        LEFT JOIN {MemFree} mf ON
            a.instance = mf.instance AND a.group = mf.group AND a.timestamp = mf.timestamp
        LEFT JOIN {MemTotal} mt ON
            a.instance = mt.instance AND a.group = mt.group AND a.timestamp = mt.timestamp
    """)

    #return output
    print(output.head(20))

#@timeit
#def polars_join_test() -> polars.internals.lazyframe.frame.LazyFrame:
@timeit
def polars_join_test() -> None:
    data_1 = po.scan_csv("/home/chihwei/playground/data/node_memory_Active_bytes*")
    data_2 = po.scan_csv("/home/chihwei/playground/data/node_memory_Cached_bytes*")
    data_3 = po.scan_csv("/home/chihwei/playground/data/node_memory_MemFree_bytes*")
    data_4 = po.scan_csv("/home/chihwei/playground/data/node_memory_MemTotal_bytes*")

    tmp_1 = data_1.join(data_2, on=['instance','group','timestamp'], how='left')
    tmp_2 = data_3.join(data_4, on=['instance','group','timestamp'], how='left')

    output = tmp_1.join(tmp_2, on=['instance','group','timestamp'], how='left')

    #return output.collect()
    print(output.collect())

def get_command():
    parser = argparse.ArgumentParser(description='dataframe speed test')
    parser.add_argument('--spark', action='store_true')
    parser.add_argument('--koalas', action='store_true')
    parser.add_argument('--polars', action='store_true')
    return parser

if __name__ == "__main__"  :

    parser = get_command()
    args = parser.parse_args()
    if args.spark:
        spark_join_test()
    elif args.koalas:
        koalas_join_test()
    elif args.polars:
        polars_join_test()
    else:
        parser.print_help()
