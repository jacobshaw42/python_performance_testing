import pandas as pd
from datetime import datetime as dt
import pyodbc

from config import user, password, database, server, port
import pymssql

file = "g_application.tsv"
conn = pymssql.connect(server, user, password, database)

cur = conn.cursor()

# cur.execute("SELECT * from bcp_spaces")

# for row in cur:
#     print(row)
connectionString = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={user};PWD={password}'
odbc_conn = pyodbc.connect(connectionString)
odbc_cur = odbc_conn.cursor()
odbc_cur.fast_executemany = True

df = pd.read_csv(
    file,
    sep='\t',
    low_memory=False,
)
df.iloc[:,-1] = df.iloc[:,-1].astype(str)
df = df.iloc[:10000,:]
df["InsertingProcess"] = "executemany"
cols = str(df.columns.tolist()).replace("[","(").replace("]",")").replace("'","")
records = df.to_records(index=False).tolist()

inserts = f'INSERT INTO dbo.pandas_inserts {cols} VALUES (%s, %s, %s, %s, %s, %s, %s)'.replace("%s", "?")
#print(records)
print(inserts)
s = dt.now()
odbc_cur.executemany(  ### Absolutely horrible compared to other methods. Not worth finishing
    inserts,
    records,
    #batch_size=10000,
)
conn.commit()
e = dt.now()
print("execute many time to execute: ",e-s)
return
cur.execute("TRUNCATE TABLE dbo.pandas_inserts")
conn.commit()
print("truncated table")
df["InsertingProcess"] = "bulk_copy"
records = df.to_records(index=False).tolist()
#print(records)
s = dt.now()
conn.bulk_copy(
    "dbo.pandas_inserts",
    records,
    column_ids=[2,3,4,5,6,7,8],
    batch_size=10000,
)
conn.commit()
e = dt.now()
print("bulk copy time to execute: ",e-s)