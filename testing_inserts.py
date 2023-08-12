import pandas as pd
import dask.dataframe as dd
from datetime import datetime as dt
import sqlalchemy as sa
from dask.diagnostics import ProgressBar
from bcpandas import to_sql, SqlCreds


#conn = sqlite3.connect("testing.db")
file = "g_application.tsv"
constr = "mssql+pyodbc://SA:password1!@localhost:1433/testdb?driver=ODBC+Driver+17+for+SQL+Server"
engine = sa.create_engine(constr)
creds = SqlCreds.from_engine(engine)


def pandas_test(file, creds):
    s = dt.now()
    print("reading file")
    pdf = pd.read_csv(file, sep="\t",dtype=str)
    print("read file")
    to_sql(pdf, 'bcpandas_inserts',creds=creds, index=False, if_exists='replace',batch_size=10000)
    print(dt.now() - s)
    del pdf

def dask_test_all_at_once(file, constr):
    pbar = ProgressBar()
    pbar.register()
    s = dt.now()
    ddf = dd.read_csv(file, sep="\t",dtype=str)
    ddf.to_sql("dask_inserts", uri=constr, if_exists="replace", index=False, engine_kwargs={"fast_executemany":True},compute=True)
    print(dt.now() - s)
    # first take, 10 minutes for 50M records in 2.7GB file
    del ddf
    
def dask_test_one_partition(file, constr, table_name):
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
    # Low memory usage, negliable in time compared to all at once


pandas_test(file, creds)
dask_test_all_at_once(file, constr)
dask_test_one_partition(file, constr,"g_app")

