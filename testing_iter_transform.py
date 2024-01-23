import dask.dataframe as dd
from datetime import datetime as dt
import hashlib
import pandas as pd

file = "g_application.tsv"
df = pd.read_csv(file, sep="\t")
#ddf = dd.read_csv(file, sep="\t",dtype=str,blocksize="1mb").get_partition(0)
hash_join = lambda x: hashlib.md5("".join([str(y) for y in x if y is not None]).encode()).hexdigest()
df = df.iloc[:100000,:]
current = {}
#def filter(x):

s = dt.now()
df['hashed'] = df.apply(hash_join, axis=1)
#print(ddf.compute())
e = dt.now()
print(df)
print("time to run with apply: ", e - s)


s = dt.now()
df['hashed2'] = None
for i, row in df.iterrows():
    df['hashed2'][i] = hash_join(row[:-1])
    # row['hashed2'] = hash_join(row)

#print(ddf.compute())
print(df)
print('iterrows hashing: ',dt.now() - s)
