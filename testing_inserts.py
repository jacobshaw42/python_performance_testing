import pandas as pd
import dask.dataframe as dd
from datetime import datetime as dt
import sqlalchemy as sa
from dask.diagnostics import ProgressBar
from bcpandas import to_sql, SqlCreds
from config import user, password, database, server, port


file = "g_application.tsv"
constr = f"mssql+pyodbc://{user}:{password}@{server}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
engine = sa.create_engine(constr, fast_executemany=True)
creds = SqlCreds.from_engine(engine)

def pandas_test(file, engine):
    print('-'*20, 'pandas inserts')
    s = dt.now()
    print("reading file")
    pdf = pd.read_csv(file, sep="\t", dtype=str)
    pdf.to_sql('pandas_inserts', con=engine,chunksize=10000, schema='dbo', if_exists='replace')
    print(dt.now() - s)
    del pdf

def bcpandas_test(file, creds):
    print('-'*20, 'bcpandas inserts')
    s = dt.now()
    print("reading file")
    pdf = pd.read_csv(file, sep="\t",dtype=str)
    print("read file")
    to_sql(pdf, 'bcpandas_inserts',creds=creds, index=False, if_exists='replace',batch_size=10000)
    print(dt.now() - s)
    del pdf

def dask_test_all_at_once(file, constr):
    print('-'*20, 'dask full dataframe inserts')
    pbar = ProgressBar()
    pbar.register()
    s = dt.now()
    ddf = dd.read_csv(file, sep="\t",dtype=str)
    ddf.to_sql("dask_inserts", uri=constr, if_exists="replace", index=False, engine_kwargs={"fast_executemany":True},compute=True)
    print(dt.now() - s)
    del ddf
    
def dask_test_one_partition(file, constr, table_name):
    print('-'*20, 'dask by partitions inserts')
    pbar = ProgressBar()
    pbar.register()
    s = dt.now()
    ddf = dd.read_csv(file, sep="\t",blocksize="16MB",dtype=str)
    for i in range(ddf.npartitions):
        partition=ddf.get_partition(i)
        if i == 0:
            partition.to_sql(table_name, uri=constr, if_exists="replace", index=False, engine_kwargs={"fast_executemany":True},compute=True)
        elif i > 0:
            partition.to_sql(table_name, uri=constr, if_exists="append", index=False, engine_kwargs={"fast_executemany":True},compute=True)
    print(dt.now() - s)
    del ddf

pandas_test(file, engine)
#bcpandas_test(file, creds)
#dask_test_all_at_once(file, constr)
#dask_test_one_partition(file, constr,"dask_partitions_inserts")

