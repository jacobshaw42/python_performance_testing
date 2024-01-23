import dask.dataframe as dd
from datetime import datetime as dt
import sqlalchemy as sa
from dask.diagnostics import ProgressBar
from config import user, password, database, server, port
import hashlib
from bcpandas import to_sql, SqlCreds
import pandas as pd
import polars as pl

files = ["test_wide_csv0.csv","test_wide_csv3.csv","test_wide_csv6.csv","test_wide_csv9.csv","test_wide_csv12.csv","test_wide_csv15.csv","test_wide_csv18.csv"]
files = [
    "test_wide_csv100.csv",
    #"test_wide_csv192.csv"
    ]
constr = f"mssql+pyodbc://{user}:{password}@{server}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
engine = sa.create_engine(constr, fast_executemany=True)
creds = SqlCreds.from_engine(engine)

hashing = lambda x: hashlib.md5("".join([str(y) for y in x]).encode()).hexdigest()

def dask_test_one_partition(file, constr, table_name):
    print('-'*20, 'dask by partitions inserts')
    pbar = ProgressBar()
    pbar.register()
    s = dt.now()
    ddf = dd.read_csv(file,blocksize="16MB",dtype=str)
    
    # ddf['hash'] = ddf.apply(hashing, axis=1, meta=("hash", "str"))
    
    # ddf['asofdate'] = "2023-08-25"
    # ddf['inseringPr'] = "test"
    # ddf['testtest'] = "testest"
    
    for i in range(ddf.npartitions):
        partition=ddf.get_partition(i)
        if i == 0:
            partition.to_sql(table_name, uri=constr, if_exists="append", index=False, engine_kwargs={"fast_executemany":True},compute=True, chunksize=10000)
        elif i > 0:
            partition.to_sql(table_name, uri=constr, if_exists="append", index=False, engine_kwargs={"fast_executemany":True},compute=True, chunksize=10000)
    print(dt.now() - s)
    del ddf
            
            
def bcpandas_test(file, creds, tablename):
    print('-'*20, 'bcpandas inserts')
    s = dt.now()
    print("reading file")
    pdf = pd.read_csv(file,dtype=str)
    
    #pdf['hash'] = pdf.apply(hashing, axis=1)
    
    #pdf['asofdate'] = "2023-08-25"
    #pdf['inseringPr'] = "test"
    #pdf['testtest'] = "testest"   
    
    print("read file")
    to_sql(pdf, tablename,creds=creds, index=False, if_exists='append',batch_size=10000)
    print(dt.now() - s)
    del pdf

def polars_test(f, creds, table_name, sep=","):
    s = dt.now()
    pldf = pl.read_csv(f, separator=sep)#.collect(streaming=True)
    pldf = pldf.to_pandas()
    print("read in: ",dt.now() -s)
    s = dt.now()
    to_sql(pldf, table_name, creds,index=False,if_exists="append", batch_size=10000)
    print("wrote in: ", dt.now() -s)

# for f in files:
#     print(f)
#     for i in range(5):
#         print(f"insert {i}")
#         dask_test_one_partition(f, constr, f.split('.')[0])
#         bcpandas_test(f, creds, f.split('.')[0])

polars_test(files[0], creds, "polars_inserts")
