# python_performance_testing

After spending sometime researching what methods in python are the best for apply transformations to dataframes or ingesting data into a SQL Server database, I noticed some conflicting information. 

Counter to my initial assumptions, I even found information suggesting that a iterrows over a dataframe would basically be the same thing as an apply method. 

The following are the results of some basic tests I performed several times. 

The file used for testing was a TSV from PatentsView (g_application.tsv) that is 8433988 records, 6 columns, and roughly 370 MB unzipped.

You can access the files used to obtain these results with this [github repo](https://github.com/jacobshaw42/python_performance_testing) that also contains this file.

## Testing Inserting into SQL Server

In an order to determine what process would be most effective for certain scenarios, I tested a few different methods to for inserting into a SQL Server database.

Using `testing_inserts.py` from the python_performance_testing the following results were determined monitoring Ubuntu's `top` command for memory and using python's `datetime` package for tracking time.

|          Method                | VIRT Mem (GB) | RES Mem (GB) | Time (minutes) | Number of Records | Records per second |
|--------------------------------|---------------|--------------|----------------|-------------------|--------------------|
| pandas to_sql                  | 2.7-3.2       | 2.3-2.8      | 2:07           | 8433988           | 66409              |
| bcpandas                       | 1.8-3.1       | 1.5-2.4      | 1:25           | 8433988           | 99223              |
| dask (full load)               | 1.6-2.8       | 0.8-2.1      | 1:45           | 8433988           | 80323              |
| dask (by partition, 16MB)      | 0.8           | 0.3          | 1:47           | 8433988           | 78822              |
| dask partition (16MB) bcpandas | 1.1           | 0.3          | 1:29           | 8887790           | 99014              |
| pymssql all at once            | 1.1           | 0.4          | 1:36           | 8887790           | 91794              |
| pymssql                        | 1.1           | 0.2          | 1:42           | 8887790           | 86394              |

The most obvious result is that pandas to_sql is the worst on resource usage and speed.

For bcpandas, the results clearly display a speed advantage for the cost of memory utilization.

When it comes to dask, we see a slight slow down from bcpandas, but a huge decrease in memory utilization. Especially when using partitions.

It is worth noting that using a full dask load (not by partition), the memory usage does not flux like pandas and bcpandas. The memory starts at the top range and progresses downward. However, the marginal speed advantage is probably not worth it at any file size.

The answer for most cases is quite possibly using dask by partition with bcpandas. This takes advantage of the low memory cost without taking considerably longer.

## Testing Pandas Manipulation

Using the `testing_dataframe_transformations.py` script to test for dataframe manipulations.

Upon searching for concrete answers on what methods are most efficient for transforming a pandas dataframe, I found mixed opinions on whether the pandas apply method was faster than iterrows and a python for loop.

The results show different operations for converting values, manipulating strings, and creating a hash using hashlib.

| Method Used           | Operation               | Time (seconds) |
|-----------------------|-------------------------|----------------|
| iterrows for loop     | Boolean Type Conversion | 181.852        |
| apply lambda          | Boolean Type Conversion |   0.466        |
| map lambda            | Boolean Type Conversion |   0.459        |
| pd.Series.astype      | Boolean Type Conversion |   0.078        |
| apply lambda          | String Concat           |   0.736        |
| map lambda            | String Concat           |   0.760        |
| Simple Concat         | String Concat           |   0.319        |
| apply lambda          | date conversion         |  27.498        |
| map lambda            | date conversion         |  26.944        |
| pandas to_datetime    | date conversion         |   1.585        |
| apply lambda          | hashing                 |  24.641        |
| astype str sum concat | hashing                 |   5.333        |

Due to the amount of time the iterrows loop took for a single boolean transformation, I did not bother to check the rest of the transformations.

When it comes to transforming a dataframe, the apply or map function clearly faster than an iterrows loop over the dataframe. 

The map and apply function had very little difference for almost all cases. 

The more native dataframe methods (pd.to_datetime, astype, direct manipulation on row) are clearly several times faster in all cases. While this should not be surprising, the amount by which it is faster is pretty incredible. In the case of date conversion, it is nearly 1/25th the time an apply/map would take. 

