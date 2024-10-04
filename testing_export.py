import pandas as pd
import os
import sqlalchemy as sa
from config import user, password, database, server, port
from memory_monitor import profile
from sqlalchemy import Table, Column, MetaData
from io import StringIO
import s3fs

file = "g_application.tsv"
constr = f"mssql+pyodbc://{user}:{password}@{server}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
constr = "mssql+pyodbc:///?odbc_connect=Driver={ODBC+Driver+17+for+SQL+Server};Server="+server+";Database="+database+";UID="+user+";PWD="+password+";TrustServerCertificate=yes"
engine = sa.create_engine(
    constr,
)

columns = [
    Column("application_id"),
    Column("patent_id"),
    Column("patent_application_type"),
    Column("filing_date"),
    Column("series_code"),
    Column("rule_47_flag"),
    Column("index"),
]
outfile = "test.csv"

def stream_to_s3(sa_query, con, bucket, key):
    s3 = s3fs.S3FileSystem(anon=False)
    s3_file_path = f"s3://{bucket}/{key}"
    with s3.open(s3_file_path, mode="w") as f:
        i=0
        for chunk in pd.read_sql(sa_query, con=con, chunksize=10000):
            if i == 0:
                header = chunk.columns.to_list()
            else:
                header=None
            i+=1
            csv_buffer = StringIO()
            chunk.to_csv(csv_buffer, index=False, header=header)
            f.write(csv_buffer.getvalue())

@profile
def pandas_read_sql_to_csv(file, engine):
    if os.path.isfile(file):
        os.remove(file)
    table_name = "pandas_inserts"
    pi = Table(
        table_name,
        MetaData(), 
        *columns,
        schema="dbo"
    )
    bucket = "test-write-python-generator"
    key="test.csv"
    #pi = pi.select().with_only_columns(*columns).limit(100000)
    pi = f"SELECT TOP 100000 * FROM {table_name}"
    stream_to_s3(pi, engine, bucket, key)

pandas_read_sql_to_csv(outfile, constr)