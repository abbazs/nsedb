from datetime import datetime

import pandas as pd

from indexdb import indexdb

idb = indexdb()
dfd = pd.read_sql(
    sql='SELECT * FROM fno WHERE "SYMBOL"=NIFTY & "INSTRUMENT"=FUTIDX & "TIMESTAMP"=dt',
    con=idb.engine,
    columns=["TIMESTAMP"],
)
dfd = pd.read_sql(
    sql='SELECT * FROM fno WHERE "SYMBOL"=NIFTY & "INSTRUMENT"=FUTIDX & "TIMESTAMP"=dt',
    con=idb.engine,
    columns=["TIMESTAMP"],
)
dfd = pd.read_sql(sql="SELECT * FROM idx", con=idb.engine, columns=["TIMESTAMP"])

dt = datetime(2005, 1, 17)
dfd = pd.read_sql(
    sql=f"SELECT \"TIMESTAMP\" FROM fno WHERE \"SYMBOL\"='NIFTY' AND \"INSTRUMENT\"='FUTIDX' AND \"TIMESTAMP\"='{dt}'",
    con=indexdb.engine
)

didx = pd.read_sql(sql='SELECT DISTINCT "TIMESTAMP" FROM idx', con=idb.engine) 
didx = didx[didx["TIMESTAMP"]>"2005-1-1"]
dfno = pd.read_sql(sql='SELECT DISTINCT "TIMESTAMP" FROM fno', con=idb.engine)
dfg = pd.concat([didx, dfno])
dfg = dfg.assign(val=1)
dfgr = dfg.groupby("TIMESTAMP").sum()
dfgr = dfgr.reset_index()
dfgx = dfgr.query("val==1")
lst = dfgx["TIMESTAMP"].to_list()
idb.updateFNOBhavData_for_given_dates(lst)
idb.updateFNOBhavData_for_given_date("2005-1-28")              