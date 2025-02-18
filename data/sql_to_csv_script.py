from pathlib import Path
import pandas as pd
import sqlite3
import os

import warnings
warnings.filterwarnings('ignore')

# Specify time window
start_date = '1963-01-01'
final_date = '2023-12-31'

# Database Connection
db = sqlite3.connect(database=f"{start_date}__{final_date}.sqlite")

# Compustat + CRSP Datasets
compustat_data = pd.read_sql_query(sql="SELECT * FROM compustat", con=db, parse_dates={"datadate"})
crsp_data = pd.read_sql_query(sql="SELECT * FROM crsp", con=db, parse_dates={"date"})

# Merge them
characteristics = (compustat_data
                           .assign(date=lambda x: x["datadate"].dt.to_period("M").dt.to_timestamp())
                           .merge(crsp_data, how="left", on=["gvkey", "date"], )
                           .assign(
                                    bm=lambda x: x["be"] / x["mktcap"],
                                    sorting_date=lambda x: x["date"] + pd.DateOffset(months=6)
                                  ))

characteristics = characteristics.get(["gvkey", "bm", "sorting_date", "op", "inv"])

df = (crsp_data.merge(characteristics, how="left", left_on=["gvkey", "date"], right_on=["gvkey", "sorting_date"])
                              .sort_values(["date", "permno"])
                              .groupby("permno")
                              .apply(lambda x: x.assign(bm=x["bm"].fillna(method="ffill"),
                                                        op=x["op"].fillna(method="ffill"),
                                                        inv=x["inv"].fillna(method="ffill"))))
df = df.reset_index(drop=True)

df = df.get(["permno", "date", "exchange", "industry", "mktcap", "bm", "op", "inv", "volatility", "ret_excess"]).dropna()
df.to_csv(f'{start_date}__{final_date}.csv', index=False)

ff3_factors = pd.read_sql_query(sql="SELECT * FROM fama_french_3_M", con=db, parse_dates={"date"})
ff3_factors.to_csv('ff3_factors.csv', index=False)

ff5_factors = pd.read_sql_query(sql="SELECT * FROM fama_french_5_M", con=db, parse_dates={"date"})
ff5_factors.to_csv('ff5_factors.csv', index=False)