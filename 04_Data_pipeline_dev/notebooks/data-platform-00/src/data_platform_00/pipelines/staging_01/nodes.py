import dask.dataframe as dd
import pandas as pd

def show(ddf : dd.DataFrame):
    df = ddf.compute()
    print(df.info())
    print(df)