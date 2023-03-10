import pdb

import prometheus_api_client as prom_client
import datetime, argparse, time

from tools import timeit, datetime_range_day, multiple_thread, multiple_process, init_folder

import pandas as pd


def download_input_generator(metrics, stime, etime):
    for metric in metrics:
        qstring = f"{metric}"
        for d in datetime_range_day(stime, etime):
            sd, ed = d, d + datetime.timedelta(days=1)
            yield {"qstring":qstring, "metric":metric,"start_time":sd, "end_time":ed}

@timeit
@multiple_thread
def get_prometheus_data(target: dict) -> None:
    print(f"starting download  metric: {target['metric']}, date from {target['start_time']} to {target['end_time']}")
    data = PROM.get_metric_range_data(
        target["qstring"],
        start_time=target["start_time"],
        end_time=target["end_time"],
        chunk_size = datetime.timedelta(seconds=60)
    )

    print(f"processing metric: {target['metric']}, date from {target['start_time']} to {target['end_time']}")

    metric_df = prom_client.MetricSnapshotDataFrame(data)[['instance', 'group', 'timestamp', 'value']]
    metric_df = metric_df.rename(columns={'value':target["metric"]})
    file_name = "/home/chihwei/playground/data/{0}-{1}-{2}.csv".format(
        target["qstring"], 
        target["start_time"].strftime("%Y-%m-%d"), 
        target["end_time"].strftime("%Y-%m-%d")
    )
    metric_df.to_csv(file_name, index=False)
    print(f"finish write {file_name} !!!")

@timeit
@multiple_process(get_prometheus_data)
def multiple_process_get_data(target: dict) -> None:
    return get_prometheus_data(target)



if __name__ == "__main__":


    PROM = prom_client.PrometheusConnect(url =f"{prometheus_ip_address}", disable_ssl=True)
    stime = datetime.datetime(2023,1,1,0,0,0)
    etime = datetime.datetime(2023,1,24,0,0,0)
    metrics = [
        "node_memory_MemFree_bytes", "node_memory_MemTotal_bytes",
        "node_memory_Active_bytes", "node_memory_Cached_bytes"
    ]

    """
    # multi-threading
    get_prometheus_data(download_input_generator(metrics, stime, etime))
    """

    """
    # multi-processing
    # ??????????????????????????? download_input_generator ??? decorator ??????
    multiple_process_get_data(download_input_generator(metrics, stime, etime))
    """

    """
    # single process 
    # ??????????????????????????? download_input_generator ??? decorator ??????
    start_time = time.perf_counter()
    for target in download_input_generator(metrics, stime, etime):
        pdb.set_trace()
        get_prometheus_data(target)
    end_time = time.perf_counter()
    print(f"{end_time-start_time:2f} seconds")
    """
