# python_performance_testing

After spending sometime researching what methods in python are the best for apply transformations to dataframes or ingesting data into a SQL Server database, I noticed some conflicting information. 

Counter to my initial assumptions, I even found information suggesting that a iterrows over a dataframe would basically be the same thing as an apply method. 

The following are the results of some basic tests I performed several times. 

The file used for testing was a TSV from PatentsView (g_application.tsv) that is 8433988 records, 6 columns, and roughly 370 MB unzipped.

You can access the files used to obtain these results with this [github repo](https://github.com/jacobshaw42/python_performance_testing) that also contains this file.

## Testing Inserting into SQL Server

In an order to determine what process would be most effective for certain scenarios, I tested a few different methods to for inserting into a SQL Server database.

Using `testing_inserts.py` from the python_performance_testing the following results were determined monitoring Ubuntu's `top` command for memory and using python's `datetime` package for tracking time.

|          Method           | VIRT Mem (GB) | RES Mem (GB) | Time (minutes) | Number of Records |
|---------------------------|---------------|--------------|----------------|-------------------|
| pandas to_sql             | 2.7-3.2       | 2.3-2.8      | 2:07           | 8433988           |
| bcpandas                  | 1.8-3.1       | 1.5-2.4      | 1:25           | 8433988           |
| dask (full load)          | 2.2-3.4       | 1.7-2.8      | 1:45           | 8433988           |
| dask (by partition, 16MB) | 1.4-1.8       | 0.7-1.1      | 1:47           | 8433988           |

The results clearly display a speed advantage for bcpandas, while still outperforming memory usage for a dask full load.

Using dask by 16MB partitions, we can see a clear advantage for lower memory usage, but longest time. The increase from a full dask load is neglible. However, the time differnce between bcpandas and dask is considerable. If the low use of memory is a priority, it is clear to use dask by partitions. If memory is not a concern, bcpandas is definitely a faster insert process.

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

