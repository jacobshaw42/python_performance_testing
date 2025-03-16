# found at https://www.geeksforgeeks.org/monitoring-memory-usage-of-a-running-python-program/

# importing libraries
import os
import psutil
import datetime
# inner psutil function
def process_memory():
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    return mem_info.rss/1024/1024/1024, mem_info.vms/1024/1024/1024

# decorator function
def profile(func):
    def wrapper(*args, **kwargs):
        mem_before, _ = process_memory()
        s = datetime.datetime.now()
        result = func(*args, **kwargs)
        e = datetime.datetime.now()
        mem_after, _ = process_memory()
        print("{}:consumed memory (GB): \n before {}\n after {}\n used {}".format(
            func.__name__,
            mem_before, mem_after, mem_after - mem_before))
        print(f"Ran in {e-s}")

        return result
    return wrapper

def memory_check():
    rss, vms = process_memory()
    print(f"current consumed res memory/virtual memory: {round(rss, 2)}/{round(vms, 2)} GB")