# %%
import os

import pandas as pd

# %%
os.chdir("/home/sajo/fuel_prices_qld/data/data")
file_name = "month/2023-01-jan-2023-fuel-prices-changes-only.csv"
file_name_week = "week/22_52.csv"


df = pd.read_csv(file_name)
# %%
df
# %%
df = pd.read_csv(
    file_name_week,
    parse_dates=["TransactionDateutc"],
    date_parser=lambda x: pd.to_datetime(x, format="%d/%m/%Y %H:%M"),
)
df

# %%
time_string = "2021-10-26T22:33:21.62"
df = pd.read_csv(
    file_name_week,
    parse_dates=["TransactionDateUtc"],
    date_parser=lambda x: pd.to_datetime(x, format="%Y-%m-%d %H:%M"),
)
df
# %%
