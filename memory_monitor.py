# found at https://www.geeksforgeeks.org/monitoring-memory-usage-of-a-running-python-program/

# importing libraries
import os
import psutil
import datetime
# inner psutil function
def process_memory():
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    return mem_info.rss/1024/1024/1024

# decorator function
def profile(func):
    def wrapper(*args, **kwargs):
        mem_before = process_memory()
        s = datetime.datetime.now()
        result = func(*args, **kwargs)
        e = datetime.datetime.now()
        mem_after = process_memory()
        print("{}:consumed memory (GB): \n before {}\n after {}\n used {}".format(
            func.__name__,
            mem_before, mem_after, mem_after - mem_before))
        print(f"Ran in {e-s}")

        return result
    return wrapper