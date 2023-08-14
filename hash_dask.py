import dask.dataframe as dd
import sqlalchemy as sa
from dask.diagnostics import ProgressBar
from datetime import datetime as dt
import hashlib
import pandas as pd

file = "g_application.tsv"
new_file = "g_application2.tsv"
constr = "mssql+pyodbc://SA:password1!@localhost:1433/testdb?driver=ODBC+Driver+17+for+SQL+Server"
engine = sa.create_engine(constr)

hashing = lambda x: hashlib.md5("".join([str(y) for y in x]).encode()).hexdigest()

def dask_load_hashes(file, constr,table_name):
    pbar = ProgressBar()
    pbar.register()
    s = dt.now()
    ddf = dd.read_csv(file, sep="\t", blocksize="16MB",dtype=str)
    
    ddf['hashes'] = ddf.apply(hashing, axis=1, meta = pd.Series(dtype='str'))
    ddfh = ddf[['patent_id','hashes']]
    for i in range(ddfh.npartitions):
        partition = ddfh.get_partition(i)
        if i == 0:
            partition.to_sql(table_name, index = False, uri = constr, if_exists="replace", engine_kwargs={"fast_executemany":True},compute=True)
        elif i > 0:
            partition.to_sql(table_name, index = False, uri = constr, if_exists="append", engine_kwargs={"fast_executemany":True}, compute=True)
    print(dt.now() - s)

def check_dask_hashes(file, engine,table_name):
    # setup progress bar
    pbar = ProgressBar()
    pbar.register()
    s = dt.now()    
    
    # read current values and get hash list
    print("reading current data from sql")
    df = pd.read_sql(sa.text(f"select * from dbo.{table_name}"), engine.connect())
    print('time to read:', dt.now() - s)
    s = dt.now()
    

    # read new file and create hash column
    ddf = dd.read_csv(file, sep="\t", blocksize="16MB",dtype=str)
    ddf['hashes'] = ddf.apply(hashing, axis=1, meta=pd.Series(dtype='str'))
    ddf['rule_47_flag'] = ddf['rule_47_flag'].apply(lambda x: bool(x) if x in ('0','1') else None, meta = ("rule_47_flag","bool"))
    ddf['filing_date'] = ddf['filing_date']

    # filter to changed hashes
    f1 = ddf['patent_id'].isin(df['patent_id'].to_list())
    f2 = ddf['hashes'].isin(df['hashes'].to_list())
    ddf_new = ddf[~f1]
    dff_changed = ddf[(f1 & ~f2)]
    print(ddf_new.head())
    print(dff_changed.head())
    print('time to compute: ',dt.now() - s)
#dask_load_hashes(file, constr, "app_hashes")
check_dask_hashes(new_file, engine, "app_hashes")