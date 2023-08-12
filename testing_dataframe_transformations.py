import pandas as pd
from datetime import datetime as dt
import hashlib

hash_join = lambda x: hashlib.md5("".join([str(y) for y in x if y is not None]).encode()).hexdigest()

file = "g_application.tsv"
df = pd.read_csv(file, sep="\t", dtype=str)
def transform_apply(df: pd.DataFrame):
    ts = dt.now()
    ##### boolean type conversion
    ### astype is fastest
    # apply
    s = dt.now()
    df['rule_47_flag'].apply(lambda x: bool(x) if x in ('0','1') else None)
    print('apply boolean type conversion: ', dt.now() - s)
    # map
    s = dt.now()
    df['rule_47_flag'].map(lambda x: bool(x) if x in ('0','1') else None)
    print('map boolean type conversion: ',dt.now()-s)
    # astype
    s = dt.now()
    df['rule_47_flag'].astype(bool)
    print('native boolean type conversion: ',dt.now()-s)
    df['rule_47_flag'] = df['rule_47_flag'].astype(bool)
    
    ##### string manipulation
    ### native cat is about half the time of the other two
    # apply
    s = dt.now()
    df['filing_date'].apply(lambda x: "".join([x, " 01:12:05"]))
    print('apply string manip ', dt.now() - s)
    # map
    s = dt.now()
    df['filing_date'].map(lambda x: "".join([x, " 01:12:05"]))
    print('map string manip ', dt.now() - s)
    # native cat
    s = dt.now()
    df['filing_date'] + " 01:12:05"
    print('native cat string manip ', dt.now() - s)
    df['filing_date'] = df['filing_date'] + " 01:12:05"
    
    ##### date conversion
    ### pandas to datetime is by far fastest even with an additional filtering assignment for None
    # apply, commented out for taking 25 seconds
    s = dt.now()
    df['filing_date'].apply(lambda x: dt.strptime(x, '%Y-%m-%d %H:%M:%S').date())
    print('apply date conversion: ', dt.now() -s)
    # map, commented out for taking 25 seconds
    s = dt.now()
    df['filing_date'].map(lambda x: dt.strptime(x, '%Y-%m-%d %H:%M:%S').date())
    print('map date conversion: ', dt.now() -s)
    # pandas to datetime
    s = dt.now()
    df['filing_date'] = pd.to_datetime(df['filing_date'],format='%Y-%m-%d %H:%M:%S',errors='coerce').dt.date
    df.loc[df['filing_date'].isna(),'filing_date'] = None
    print('pandas to datetime date conversion: ', dt.now() -s)

    
    ##### hashing
    ### hash astype values sum
    # apply join cat string
    s = dt.now()
    df.apply(hash_join, axis=1)
    print('apply join hashing: ', dt.now() - s)
    # apply hash astype values sum, 5 seconds
    s = dt.now()
    df['hashes'] = hashlib.md5(df.astype(str).values.sum(axis=1)).hexdigest()
    print('hash astype values sum: ', dt.now() - s)
    print('total time for entire testing: ', dt.now() - ts)
    
# this for loop is over twice as long for a single and simple column manipulation as the entire function above it
# near 3 minutes
def transform_loop(df: pd.DataFrame):
    s = dt.now()
    for i, row in df.iterrows():
        x = row['rule_47_flag']
        row['rule_47_flag'] = bool(x) if x in ('0','1') else None
    print('boolean type conversion iterrows for loop: ',dt.now() - s)

print('-'*20, 'test pandas')
transform_apply(df)

transform_loop(df)