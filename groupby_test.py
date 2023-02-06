import datetime, pdb, time, argparse
import polars as po
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import databricks.koalas as ks
from tools import timeit

@timeit
def spark_groupby_test(path):
    spark = SparkSession.builder.master("local[*]")\
               .config('spark.driver.memory','20g')\
               .appName("read sflow data").getOrCreate()

    spdf = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(path)

    agg_funcs = [
        F.min(F.col('packetSequenceNo')).alias('packetSequenceNo'),
        F.min(F.col('sampleSequenceNo')).alias('sampleSequenceNo'),
        F.sum(F.col('sampledPacketSize')).alias('samplePacketSum'),
        F.count(F.col('packetSequenceNo')).alias('packetCount')
    ]

    agg_spdf = spdf.groupBy(GROUP_KEYS).agg(*agg_funcs)
    agg_spdf.show()

@timeit
def koalas_groupby_test(path):
    kdf = ks.read_csv(path)

    agg_kdf = kdf.groupby(GROUP_KEYS).agg(
        packetSequenceNo=('packetSequenceNo','min'),
        sampleSequenceNo=('sampleSequenceNo','min'),
        sampledPacketSum=('sampledPacketSize','sum'),
        packetCount=('sampledPacketSize','count')
    ).reset_index()
    print(agg_kdf.head(20))

@timeit
def polars_groupby_test(path):

    q = (
        po.scan_csv(path).groupby(GROUP_KEYS).agg([
            po.col('packetSequenceNo').min().alias('packetSequenceNo'),
            po.col('sampleSequenceNo').min().alias('sampleSequenceNo'),
            po.col('sampledPacketSize').sum().alias('samplePacketSum'),
            po.count('packetSequenceNo').alias('packetCount')
        ])
    )
    podf = q.collect()
    print(podf)

def get_command():
    parser = argparse.ArgumentParser(description='dataframe speed test')
    parser.add_argument('--spark', action='store_true')
    parser.add_argument('--koalas', action='store_true')
    parser.add_argument('--polars', action='store_true')
    return parser   

if __name__ == "__main__":


    path = "/home/chihwei/playground/data/reformat.csv"

    GROUP_KEYS = [
        "agent", "SecondsUTC", "srcIP", "dstIP", "srcMAC", "dstMAC",
        "inputPort", "outputPort", "IPProtocol", "TCPSrcPort", "TCPDstPort"
    ]
    

    parser = get_command()
    args = parser.parse_args()
    if args.spark:
        spark_groupby_test(path)
    elif args.koalas:
        koalas_groupby_test(path)
    elif args.polars:
        polars_groupby_test(path)
    else:
        parser.print_help()
