import dask.dataframe as dd
import pandas as pd

def show(ddf : dd.DataFrame):
    df = ddf.compute()
    print(df.info())
    print(df)

def staging01(ddf1 : dd.DataFrame ,ddf2 : dd.DataFrame)->dd.DataFrame:
    
    ddf3 = ddf1[["customer_id","company"]]
    ddf3["contact_name"] = ddf1["first_name"]+" "+ddf1["last_name"]
    
    ddf4 = ddf2[["customer_id","company_name","contact_name"]].rename(columns={"company_name": "company"})
    integate_ddf = dd.concat([ddf3,ddf4]).astype(str)
    return integate_ddf
