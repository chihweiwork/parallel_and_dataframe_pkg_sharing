#from my_read_csv import read_csv_koalas, read_csv_polars, read_csv_spark
#from my_read_csv import read_csv_polars
import datetime, pdb, time, argparse
import polars as po
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import databricks.koalas as ks

from tools import timeit

def timestamp2datetime(target):
    target = datetime.datetime.fromtimestamp(int(target))
    minutes = int(target.strftime("%M"))//15*15
    y, m, d, H, M, S, _, _, _ = target.timetuple()
    output = datetime.datetime(y, m, d, H, minutes, 0).strftime("%Y-%m-%d'T'%H:%M:%S")
    return output

@timeit
def polars_test(path):

    t1 = time.time()

    podf = (
        po.scan_csv(path, null_values=["NA"])
          .with_columns([
              po.col("SecondsUTC").apply(lambda t: timestamp2datetime(t)).alias("SecondsUTC")
          ]).collect()
    )

    print(podf)

@timeit
def spark_test(path): 
    t1 = time.time()
    spark = SparkSession.builder.master("local[*]")\
               .appName("read sflow data").getOrCreate()

    spdf = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(path)
    tp2dt = F.udf(lambda x: timestamp2datetime(x), T.StringType())
    spdf = spdf.withColumn("SecondsUTC", tp2dt(F.col("SecondsUTC")))
    spdf.show()

@timeit
def koalas_test(path):
    t1 = time.time()
    kdf = ks.read_csv(path)
    kdf['SecondsUTC'] = kdf.SecondsUTC.apply(lambda x: timestamp2datetime(x))
    
    print(kdf.head(20))

def get_command():
    parser = argparse.ArgumentParser(description='dataframe speed test')
    parser.add_argument('--spark', action='store_true')
    parser.add_argument('--koalas', action='store_true')
    parser.add_argument('--polars', action='store_true')
    return parser

if __name__ == "__main__"  :

    parser = get_command()
    args = parser.parse_args()
    path = "/home/chihwei/playground/data/network_data.csv"
    if args.spark:
        spark_test(path)
    elif args.koalas:
        koalas_test(path)
    elif args.polars:
        polars_test(path)
    else:
        parser.print_help()

