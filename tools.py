import time
import datetime
import glob
import os
import concurrent.futures
from functools import wraps

def ipv6_reformat(target):

    tmp = target.split('.')
    if len(tmp) == 4: return target
    else:
        return ":".join(tmp)
    
def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f'Function {func.__name__}{args} {kwargs} Took {total_time:.4f} seconds')
        return result
    return timeit_wrapper

def datetime_range_day(stime, etime):
    for n in range(int((etime-stime).days)):
        yield stime + datetime.timedelta(days=n)
    

def multiple_thread(func):
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

def multiple_process(multiprocess_func):
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

def init_folder(path):
    files = glob.glob(path)
    for file in files:
        if os.path.isfile(file):
            os.remove(file)
