# python 加速運算，資料收集分享

## Contents

- 利用 Decorator 裝飾器 達成平行化計算

- python dataframe package 介紹

---

## 利用 Decorator 裝飾器 達成平行化計算

### 裝飾器 簡單介紹

#### 裝飾器範例

```python
def print_func_name(func):
    def warp():
        print("Now use function '{}'".format(func.__name__))
        func()
    return warp


@print_func_name
def dog_bark():
    print("Bark !!!")


@print_func_name
def cat_miaow():
    print("Miaow ~~~")


if __name__ == "__main__":
    dog_bark()
    # > Now use function 'dog_bark'
    # > Bark !!!

    cat_miaow()
    # > Now use function 'cat_miaow'
    # > Miaow ~~~
```

範例中 decorator `print_func_name` 會輸出 function 的名稱，自定義的 function  `dog_bark` 會 `print` Bark !!!，執行程式後，會得到

```bash
Now use function 'dog_bark'
Bark !!!
Now use function 'cat_miaow'
Miaow ~~~
```

由結果可知，使用裝飾器後，程式會先執行裝飾器中的內容，在執行自定義函數的內容，利用這個特性，將自定義的函數進行平行化的計算

#### 預計優化的範例 (Single process)

```python
import pdb

import prometheus_api_client as prom_client
import datetime, argparse, time

from tools import timeit, datetime_range_day, mutiple_thread, mutiple_process, init_folder

import pandas as pd

def download_input_generator(metrics, stime, etime):
    for metric in metrics:
        qstring = f"{metric}"
        for d in datetime_range_day(stime, etime):
            sd, ed = d, d + datetime.timedelta(days=1)
            yield {"qstring":qstring, "metric":metric,"start_time":sd, "end_time":ed}


def get_prometheus_data(target: dict) -> None:
    data = PROM.get_metric_range_data(
        target["qstring"],
        start_time=target["start_time"],
        end_time=target["end_time"],
        chunk_size = datetime.timedelta(seconds=60)
    )

    metric_df = prom_client.MetricSnapshotDataFrame(data)[['instance', 'group', 'timestamp', 'value']]
    metric_df = metric_df.rename(columns={'value':target["metric"]})
    file_name = "/home/chihwei/playground/data/{0}-{1}-{2}.csv".format(
        target["qstring"],
        target["start_time"].strftime("%Y-%m-%d"),
        target["end_time"].strftime("%Y-%m-%d")
    )
    metric_df.to_csv(file_name, index=False)


if __name__ == "__main__":

    PROM = prom_client.PrometheusConnect(url =f"{prometheus_ip_address}", disable_ssl=True)
    stime = datetime.datetime(2023,1,1,0,0,0)
    etime = datetime.datetime(2023,1,24,0,0,0)
    metrics = [
        "node_memory_MemFree_bytes", "node_memory_MemTotal_bytes",
        "node_memory_Active_bytes", "node_memory_Cached_bytes"
    ]

    start_time = time.perf_counter()
    for target in download_input_generator(metrics, stime, etime):
        get_prometheus_data(target)
    end_time = time.perf_counter()
    print(f"{end_time-start_time:2f} seconds")
```

以下載資料的程式為例，前文的範例中，是利用 `prometheus_api_client` 下載 prometheus 中 `metrics` 從 2023-01-01 到 2023-01-04 的資料。

- `download_input_generator` 是一個 generator 負責迭代出下載資料的資訊，如 start/end time、metric name 與 query string

- `get_prometheus_data` 負責下載資料，並且把資料寫到指定的位置中

前文的例子是使用一個 for loop 去迭代出下載所需的資訊，並且一個一個的把資料下載下來，是屬於 single core single thread 的計算

#### 單核心多線程 muti-threading

```python
import pdb

import prometheus_api_client as prom_client
import datetime, argparse, time

from tools import timeit, datetime_range_day, mutiple_thread, mutiple_process, init_folder

import pandas as pd

def download_input_generator(metrics, stime, etime):
    for metric in metrics:
        qstring = f"{metric}"
        for d in datetime_range_day(stime, etime):
            sd, ed = d, d + datetime.timedelta(days=1)
            yield {"qstring":qstring, "metric":metric,"start_time":sd, "end_time":ed}
@timeit
@mutiple_thread
def get_prometheus_data(target: dict) -> None:
    data = PROM.get_metric_range_data(
        target["qstring"],
        start_time=target["start_time"],
        end_time=target["end_time"],
        chunk_size = datetime.timedelta(seconds=60)
    )

    metric_df = prom_client.MetricSnapshotDataFrame(data)[['instance', 'group', 'timestamp', 'value']]
    metric_df = metric_df.rename(columns={'value':target["metric"]})
    file_name = "/home/chihwei/playground/data/{0}-{1}-{2}.csv".format(
        target["qstring"],
        target["start_time"].strftime("%Y-%m-%d"),
        target["end_time"].strftime("%Y-%m-%d")
    )
    metric_df.to_csv(file_name, index=False)


if __name__ == "__main__":

    PROM = prom_client.PrometheusConnect(url =f"{prometheus_ip_address}", disable_ssl=True)
    stime = datetime.datetime(2023,1,1,0,0,0)
    etime = datetime.datetime(2023,1,24,0,0,0)
    metrics = [
        "node_memory_MemFree_bytes", "node_memory_MemTotal_bytes",
        "node_memory_Active_bytes", "node_memory_Cached_bytes"
    ]

    get_prometheus_data(download_input_generator(metrics, stime, etime))
    
```

前問的範例，與 Single process 的 code 做比較，可以發現，自定義的函數都沒有改變，有變的地方只有

- function: `get_prometheus_data` 多了兩個裝飾器 (decorator) 分別為
  
  - timeit: 計算函數所的持續運算時間
  
  - mutiple_thread: 將程式變成 muti-threading 的裝飾器

- `__main__` 中的 for loop 被改成:
  `get_prometheus_data(download_input_generator(metrics, stime, etime))`

觀察兩者的差別後可以發現，使用 decorator 可以在最小程度的改動程式邏輯的情況下，達成程式平行計算的效果

##### decorator - mutiple_thread

```python
def mutiple_thread(func):
    """
    定義裝飾器，使用簡單的語法糖來達成平行多線程運算的能力
    參考文件:https://medium.com/analytics-vidhya/python-decorator-to-parallelize- any-function-23e5036fb6a
    :param func:
    - type: function
    - desc: 放入自定義的函數
            :return:
                - type: function
    """
    @wraps(func)
    def wrapper(lst):
        """
        :param lst:
            - type: iteratable object
        :return:
        """
        number_of_threads_multiple = 8
        number_of_workers = int(8  * number_of_threads_multiple)
        result = []
        executer = concurrent.futures.ThreadPoolExecutor(max_workers=number_of_workers)
        futures = [executer.submit(func,  item) for item in lst]
        for future in concurrent.futures.as_completed(futures):
            result.append(future.result())
        return result
    return wrapper
```

### 多核心 muti-processing

```python
import pdb

import prometheus_api_client as prom_client
import datetime, argparse, time

from tools import timeit, datetime_range_day, mutiple_thread, mutiple_process, init_folder

import pandas as pd

def download_input_generator(metrics, stime, etime):
    for metric in metrics:
        qstring = f"{metric}"
        for d in datetime_range_day(stime, etime):
            sd, ed = d, d + datetime.timedelta(days=1)
            yield {"qstring":qstring, "metric":metric,"start_time":sd, "end_time":ed}

def get_prometheus_data(target: dict) -> None:
    data = PROM.get_metric_range_data(
        target["qstring"],
        start_time=target["start_time"],
        end_time=target["end_time"],
        chunk_size = datetime.timedelta(seconds=60)
    )

    metric_df = prom_client.MetricSnapshotDataFrame(data)[['instance', 'group', 'timestamp', 'value']]
    metric_df = metric_df.rename(columns={'value':target["metric"]})
    file_name = "/home/chihwei/playground/data/{0}-{1}-{2}.csv".format(
        target["qstring"],
        target["start_time"].strftime("%Y-%m-%d"),
        target["end_time"].strftime("%Y-%m-%d")
    )
    metric_df.to_csv(file_name, index=False)

@timeit
@mutiple_process(get_prometheus_data)
def mutiple_process_get_data(target: dict) -> None:
    return get_prometheus_data(target)

if __name__ == "__main__":

    PROM = prom_client.PrometheusConnect(url =f"{prometheus_ip_address}", disable_ssl=True)
    stime = datetime.datetime(2023,1,1,0,0,0)
    etime = datetime.datetime(2023,1,24,0,0,0)
    metrics = [
        "node_memory_MemFree_bytes", "node_memory_MemTotal_bytes",
        "node_memory_Active_bytes", "node_memory_Cached_bytes"
    ]

    mutiple_process_get_data(download_input_generator(metrics, stime, etime))
```

muti-processing 的狀況會比 muti-threading 複雜一點，為了使用 decorator 實現 muti-processing 必須使用 decorator fectory 的概念，具體的改動為：

- 增加 function `mutiple_process_get_data` 目的為 :
  
  1. 讓 `__main__` 呼叫使用
  
  2. 加上 decorator

- 將 `get_prometheus_data` 傳入 decorator 中

若不使用 decorator fectory 會出現 `_pickle.PicklingError` 的錯誤，詳細原因，請參考**參考文章 2** 

##### decorator - mutiple_process

```python
def mutiple_process(multiprocess_func):
    """
    定義裝飾器，使用簡單的語法糖來達成平行多核心運算的能力
    參考文件:https://stackoverflow.com/questions/63473520/cannot-use- processpoolexecutor-if-in-a-decorator
    :param func:
        - type: function
        - desc: 放入自定義的函數
    :return:
        - type: function
    """
    def _decorate(func):
        number_of_process = 8
        def impl(iteratable):
            executer = concurrent.futures.ProcessPoolExecutor(max_workers=number_of_process)
            futures = [executer.submit(multiprocess_func,  item) for item in iteratable]
            result = list()
            for future in concurrent.futures.as_completed(futures):
                result.append(future.result())
            return result
        return impl
    return _decorate
```

### 小結

- 使用 Decorator 在保留原先的程式邏輯，並且增加新的功能

- Decorator 具有很好的重複使用性

- 利用上述介紹的方法，可以最小程度的改動程式，並且達到平行計算的效果

#### 參考文章

1. https://medium.com/citycoddee/python進階技巧-3-神奇又美好的-decorator-嗷嗚-6559edc87bc0

2. [python - Cannot use ProcessPoolExecutor if in a decorator? - Stack Overflow](https://stackoverflow.com/questions/63473520/cannot-use-%20processpoolexecutor-if-in-a-decorator)

---

## 好用工具介紹 Polars 與 Koalas

### 北極熊來襲～ Polars

Lightning-fast DataFrame library for Rust and Python

Polars is a blazingly fast DataFrames library implemented in Rust using Apache Arrow Columnar Format as the memory model.

- Lazy (類似 spark)| eager execution 
- Multi-threaded
- SIMD (**Single Instruction Multiple Data** **單指令流多資料流**)
- Query optimization
- Powerful expression API
- Hybrid Streaming (larger than RAM datasets)
- Rust | Python | NodeJS | ...

### 以為自己在使用 Pandas 的 Koalas

Pandas API on Apache Spark

The Koalas project makes data scientists more productive when interacting with big data, by implementing the pandas DataFrame API on top of Apache Spark.

pandas is the de facto standard (single-node) DataFrame implementation in Python, while Spark is the de facto standard for big data processing. With this package, you can:

- Be immediately productive with Spark, with no learning curve, if you are already familiar with pandas.
- Have a single codebase that works both with pandas (tests, smaller datasets) and with Spark (distributed datasets).

### 比較 pyspark koalas polars 的差別與效能

| 測試項目          | Pyspark | Koalas | Polars |
| ------------- | ------- | ------ | ------ |
| apply (sec)   | 35.03   | 98.63  | 380.77 |
| groupby (sec) | 71.64   | 79.62  | 23.14  |
| join (sec)    | 20.80   | 46.65  | 22.87  |

根據上表，Polars 雖然是 single process muti-thread 但意外的在 groupby 與 join 上表現得不錯，以下來比較三個工具，在不同測試項目中，程式碼的差別

#### apply

##### pyspark

```python
def spark_test(path):
    t1 = time.time()
    spark = SparkSession.builder.master("local[*]")\
               .appName("read sflow data").getOrCreate()

    spdf = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(path)
    tp2dt = F.udf(lambda x: timestamp2datetime(x), T.StringType())
    spdf = spdf.withColumn("SecondsUTC", tp2dt(F.col("SecondsUTC")))
    spdf.show()
```

##### koalas

```python
def koalas_test(path):
    t1 = time.time()
    kdf = ks.read_csv(path)
    kdf['SecondsUTC'] = kdf.SecondsUTC.apply(lambda x: timestamp2datetime(x))
    
    print(kdf.head(20))
```

##### polars

```python
def polars_test(path):

    t1 = time.time()

    podf = (
        po.scan_csv(path, null_values=["NA"])
          .with_columns([
              po.col("SecondsUTC").apply(lambda t: timestamp2datetime(t)).alias("SecondsUTC")
          ]).collect()
    )

    print(podf)
```



#### groupby

##### pyspark

```python
spark_groupby_test(path):
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
```

##### koalas

```python
def koalas_groupby_test(path):
    kdf = ks.read_csv(path)

    agg_kdf = kdf.groupby(GROUP_KEYS).agg(
        packetSequenceNo=('packetSequenceNo','min'),
        sampleSequenceNo=('sampleSequenceNo','min'),
        sampledPacketSum=('sampledPacketSize','sum'),
        packetCount=('sampledPacketSize','count')
    ).reset_index()
    print(agg_kdf.head(20))
```

##### polars

```python
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
```

#### join

##### pyspark

```python
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
    output.show()
```

##### koalas

```python
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

    print(output.head(20)
```

##### polars

```python
def polars_join_test() -> None:
    data_1 = po.scan_csv("/home/chihwei/playground/data/node_memory_Active_bytes*")
    data_2 = po.scan_csv("/home/chihwei/playground/data/node_memory_Cached_bytes*")
    data_3 = po.scan_csv("/home/chihwei/playground/data/node_memory_MemFree_bytes*")
    data_4 = po.scan_csv("/home/chihwei/playground/data/node_memory_MemTotal_bytes*")

    tmp_1 = data_1.join(data_2, on=['instance','group','timestamp'], how='left')
    tmp_2 = data_3.join(data_4, on=['instance','group','timestamp'], how='left')

    output = tmp_1.join(tmp_2, on=['instance','group','timestamp'], how='left')

    print(output.collect())
```

#### polars 與 spark 無縫接軌

polars 官方並沒有提供從 spark dataframe 轉到 polars dataframe 的工具，但 spark 可以使用 Apache Arrow 而 polars 使用 Apache Arrow Columnar Format as the memory model，所以可以簡單透過 pyarrow 來將 spark dataframe 轉換成 polars dataframe

```python
import pyarrow as pa
import polars as po

def to_polars(data:pyspark.sql.dataframe.DataFrame) -> polars.internals.dataframe.frame.DataFrame:
    output = po.from_arrow(
        pa.Table.from_batches(
            data._collectAsArrow()
        )
    )
    return output
```

> 新版本的 arrow 是使用 `_collect_as_arrow()` ，但我們環境中使用的 arrow 並不是最新的本版，因此需要改成使用 `_collectAsArrow()`
> 
> python requirment
> 
> pyarrow==6.01
> 
> pandas==1.1.5
> 
> polars==0.12.5
> 
> 
> python -> 3.6
> 
> spark -> 2.4



比較從 `spark dataframe` 中轉換  `to_pandas` 與 `to_polars` 

##### 機器規格

- cpu : i7-8700

- memory : 64 G

##### 測試程式

```python
import polars as po
import pyarrow as pa                                                                                                                                                              from pyspark.sql import SparkSession

from tools import timeit
import pdb

@timeit
def to_pandas(data):
    print(data.toPandas())

@timeit
def to_polars(data):
    output = po.from_arrow(
        pa.Table.from_batches(
            data._collect_as_arrow()
        )
    )
    print(output)                                                                                                                                                                                                                                                                                                                                                   if __name__ == "__main__":

    path = "../data/test.csv"

    spark = SparkSession.builder.master("local[4]")\
               .config("spark.driver.memory", "24g")\
               .config("spark.driver.maxResultSize", "0")\
               .config("spark.sql.execution.arrow.pyspark.enabled", "true")\
               .appName("read sflow data").getOrCreate()

    spdf = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(path)

    to_pandas(spdf)
    to_polars(spdf)
```

##### 測試結果

| 筆數                          | to pandas | to polars |
| --------------------------- | --------- | --------- |
| 1,000,000 row x 20 columns  | 2.92 sec  | 1.51 sec  |
| 10,000,000 row x 20 columns | 14.86 sec | 12.91 sec |
| 50,000,000 row x 20 columns | 60.75 sec | 65.71 sec |

#### 小結

- koalas 可以簡單地使用 pandas API 來調用 spark

- koalas 也可以透過建立 spark session 與 spark 溝通，設定 spark 相關的環境，如：
  
  設定 cpu 資源，memory 使用量 ...

- koalas 的 join，除了可以使用 pandas 原先的 join 方式，也可以使用 spark sql 的方式，可以讓會使用 sql 的開發人員，使用

- polars 是一個相當強大的工具，如果程式需要在本機使用 dataframe 可以透過 polars 來加速程式的運算

- pyspark dataframe 轉換到 polars dataframe 速度比較快

##### 參考文件

- [Koalas: pandas API on Apache Spark — Koalas 1.8.2 documentation](https://koalas.readthedocs.io/en/latest/)

- [Introduction - Polars - User Guide](https://pola-rs.github.io/polars-book/user-guide/)

- [python - How to transform Spark dataframe to Polars dataframe? - Stack Overflow](https://stackoverflow.com/questions/73203318/how-to-transform-spark-dataframe-to-polars-dataframe)




