import pandas as pd
import dask.dataframe as dd
from datetime import datetime as dt
import sqlalchemy as sa
from dask.diagnostics import ProgressBar
from bcpandas import to_sql, SqlCreds
from config import user, password, database, server, port
import pymssql
from memory_monitor import profile, memory_check
import os

file = "g_application.tsv"
constr = f"mssql+pyodbc://{user}:{password}@{server}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
constr = "mssql+pyodbc:///?odbc_connect=Driver={ODBC+Driver+17+for+SQL+Server};Server="+server+";Database="+database+";UID="+user+";PWD="+password+";TrustServerCertificate=yes"
# engine = sa.create_engine(
#     constr, 
#     #fast_executemany=True, 
#     #connect_args={"TrustServerCertificate":"yes"}
# )
# creds = SqlCreds(
#     server=server,
#     database=database,
#     username=user,
#     password=password,
#     # driver_version="ODBC Driver 17 for SQL Server"
# )
# creds = SqlCreds.from_engine(engine)

@profile
def pandas_test(file, engine):
    print('-'*20, 'pandas inserts')
    s = dt.now()
    print("reading file")
    pdf = pd.read_csv(file, sep="\t", dtype=str)
    pdf["InsertingProcess"] = "pandas_to_sql_native"
    pdf.to_sql('pandas_inserts', con=engine,chunksize=10000, schema='dbo', if_exists='replace')
    print("pandas to sql time to execute: ", dt.now() - s)
    del pdf

@profile
def bcpandas_test(file, creds):
    print('-'*20, 'bcpandas inserts')
    s = dt.now()
    print("reading file")
    pdf = pd.read_csv(file, sep="\t",dtype=str)
    pdf["InsertingProcess"] = "bcpandas_all"
    print("read file")
    to_sql(pdf, 'bcpandas_inserts',creds=creds, index=False, if_exists='replace',batch_size=10000)
    print(dt.now() - s)
    del pdf

@profile
def dask_test_all_at_once(file, constr):
    print('-'*20, 'dask full dataframe inserts')
    pbar = ProgressBar()
    pbar.register()
    s = dt.now()
    ddf = dd.read_csv(file, sep="\t",dtype=str, blocksize="16MB")
    ddf.to_sql("dask_inserts", uri=constr, if_exists="replace", index=False, engine_kwargs={"fast_executemany":True},compute=True, chunksize=10000)
    print(dt.now() - s)
    del ddf

@profile
def dask_test_one_partition(file, constr, table_name):
    print('-'*20, 'dask by partitions inserts')
    pbar = ProgressBar()
    pbar.register()
    s = dt.now()
    ddf = dd.read_csv(file, sep="\t",blocksize="16MB",dtype=str)
    for i in range(ddf.npartitions):
        partition=ddf.get_partition(i)
        if i == 0:
            partition.to_sql(table_name, uri=constr, if_exists="replace", index=False, engine_kwargs={"fast_executemany":True},compute=True, chunksize=10000)
        elif i > 0:
            partition.to_sql(table_name, uri=constr, if_exists="append", index=False, engine_kwargs={"fast_executemany":True},compute=True, chunksize=10000)
    print(dt.now() - s)
    del ddf

@profile
def dask_test_one_partition_bcpandas(file, table_name):
    print('-'*20, 'dask by partitions inserts')
    pbar = ProgressBar()
    pbar.register()
    s = dt.now()
    creds = SqlCreds(
        server=server,
        database=database,
        username=user,
        password=password,
        #driver_version="ODBC Driver 17 for SQL Server"
    )
    ddf = dd.read_csv(file, sep="\t",blocksize="16MB",dtype=str)
    for i in range(ddf.npartitions):
        partition=ddf.get_partition(i)
        if i == 0:
            to_sql(partition.compute(), table_name=table_name,creds=creds, index=False, if_exists='append',batch_size=10000)
            #partition.to_sql(table_name, uri=constr, if_exists="replace", index=False, engine_kwargs={"fast_executemany":True},compute=True, chunksize=10000)
        elif i > 0:
            to_sql(partition.compute(), table_name=table_name,creds=creds, index=False, if_exists='append',batch_size=10000)
            #partition.to_sql(table_name, uri=constr, if_exists="append", index=False, engine_kwargs={"fast_executemany":True},compute=True, chunksize=10000)
    print(dt.now() - s)
    del ddf

@profile
def pymssql_test_all(file):
    conn = pymssql.connect(server, user, password, database)
    print('-'*20, 'pymssql bulk copy')
    s = dt.now()
    pdf = pd.read_csv(file, sep="\t", dtype=str)
    pdf["InsertingProcess"] = "bulk_copy"
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE dbo.pandas_inserts")
    records = pdf.to_records(index=False).tolist()
    cur.execute("TRUNCATE TABLE dbo.pandas_inserts")
    conn.commit()
    conn.bulk_copy(
        "dbo.pandas_inserts",
        records,
        column_ids=[2,3,4,5,6,7,8],
        batch_size=10000,
    )
    conn.commit()
    e = dt.now()
    print("bulk copy time to execute: ",e-s)
    del pdf

@profile
def pymssql_test_one_partition(file):
    conn = pymssql.connect(server, user, password, database)
    print('-'*20, 'pymssql bulk copy')
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE dbo.pandas_inserts")
    conn.commit()
    s = dt.now()
    ddf = dd.read_csv(file, sep="\t",blocksize="16MB",dtype=str)
    for n in range(ddf.npartitions):
        pdf = ddf.get_partition(n).compute()
        pdf["InsertingProcess"] = "bulk_copy"
        records = pdf.to_records(index=False).tolist()
        conn.bulk_copy(
            "dbo.pandas_inserts",
            records,
            column_ids=[2,3,4,5,6,7,8],
            batch_size=10000,
        )
        conn.commit()
        del pdf
    e = dt.now()
    print("bulk copy time to execute: ",e-s)
    
@profile
def pymssql_test_pandas_chunk(file):
    conn = pymssql.connect(server, user, password, database)
    print('-'*20, 'pymssql bulk copy')
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE dbo.pandas_inserts")
    conn.commit()
    s = dt.now()
    for i, chunk in enumerate(pd.read_csv(file, sep="\t",chunksize=100000,dtype=str)):
        chunk["InsertingProcess"] = "bulk_copy"
        records = chunk.to_records(index=False).tolist()
        conn.bulk_copy(
            "dbo.pandas_inserts",
            records,
            column_ids=[2,3,4,5,6,7,8],
            batch_size=10000,
        )
        conn.commit()
        memory_check()
    e = dt.now()
    print("bulk copy time to execute: ",e-s)

@profile
def pymssql_test_one_stream(file):
    import csv
    conn = pymssql.connect(server, user, password, database)
    print('-'*20, 'pymssql bulk copy')
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE dbo.pandas_inserts")
    conn.commit()
    s = dt.now()
    csvfile = csv.reader(open(file, "r"), delimiter="\t", quotechar='"', escapechar="\\")
    total=0
    lines = []
    for ind, line in enumerate(csvfile,start=1):
        if ind == 1:
            continue
        if ind % 10000 == 0:
            conn.bulk_copy(
                "dbo.pandas_inserts",
                lines,
                column_ids=[2,3,4,5,6,7,8],
                batch_size=10000,
            )
            conn.commit()
            total += len(lines)
            print(f"loaded {total} at ind {ind}")
            lines = []
            continue

        line.append("bulk_copy")
        lines.append(line)
    conn.bulk_copy(
        "dbo.pandas_inserts",
        lines,
        column_ids=[2,3,4,5,6,7,8],
        batch_size=10000,
    )
    conn.commit()
    e = dt.now()
    print("bulk copy time to execute: ",e-s)

print(os.getpid())
#pandas_test(file, engine)
#bcpandas_test(file, creds)
#dask_test_all_at_once(file, constr)
#dask_test_one_partition(file, constr,"dask_inserts")
#dask_test_one_partition_bcpandas(file, 'dask_bcpandas_inserts')
#pymssql_test_one_partition(file)
pymssql_test_pandas_chunk(file)
#pymssql_test_one_stream(file)