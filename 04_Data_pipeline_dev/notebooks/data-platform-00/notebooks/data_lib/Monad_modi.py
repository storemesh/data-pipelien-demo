import yaml
import pandas as pd

with open('/home/jovyan/notebooks/data-platform-00/notebooks/data_lib/master.yaml', 'r') as file:
    data = yaml.safe_load(file)

master_dict = data['master_country']

class Monad:
    def __init__(self, value):
        self.value = value
        self.status = 'dirty'  # Starting with 'dirty'
        self.message = []
        self.dtype = object
    def __or__(self, func):
        if self.status == 'dirty': # Only process if 'dirty'
            try:
                x = func(self.value)
                self.value = x
                self.status = 'passed'
                self.message.append(f'Passed:{func.__name__}()')
                return self 
            except Exception as e:
                self.message.append(f'Failed:{func.__name__}():{str(e)}')
        return self

    def __repr__(self):
        return f'{self.status}({self.value}){self.message}'

    def __str__(self):
        return self.__repr__()

def country_code(x):
    valid_countries = ['TH', 'US', 'UK']
    if x not in valid_countries:
        raise Exception("Invalid code")
    else:
        return x
        
def country_name(x):
    valid_countries ={'Thailand':'TH','United State':'USA', 'United Kingdom':'UK'}
    return valid_countries[x]

def country_name_thai(x):
    valid_countries ={'ไทย':'TH','สหรัฐอเมริกา':'USA', 'อังกฤษ':'UK'}
    return valid_countries[x]

def province_name(x):
    valid_province = province_set
    if x not in valid_province:
        raise Exception("Invalid code")
    else:
        return x

def clean_datetime(x):
    return pd.to_datetime(x,errors="raise")

def country_name_01(x):
    valid_country = master_dict
    return valid_country[x.lower()]

def country_name_02(x):
    master_dict = {"vietnam":"VN",
                    "korea" : "KR",
                   "taiwan": "TW",
                   "united kingdom": "GB",
                   "hong kong" : "HK",
                   "russia" : "RU",
                   "laos" : "LA",
                   "netherlands" : "NL",
                   "turkey" : "TR",
                   "iran" : "IR",
                   "republic of south africa" : "ZA"
                   
                  }
    
    valid_country = master_dict
    return valid_country[x.lower()]

def check_countryName(df : pd.DataFrame,col : str)-> pd.DataFrame:
    column_name = col
    df[column_name+'_result'] = df[column_name].apply(lambda x: Monad(x)| country_name_01 | country_name_02 )
    return df

def explode_result(df : pd.DataFrame,col : str)-> pd.DataFrame:
    column_name = col
    df_result=pd.DataFrame({
    'input':df[column_name],
    'value':df[column_name+'_result'].apply(lambda x: x.value),
    'status':df[column_name+'_result'].apply(lambda x: x.status),
    'message':df[column_name+'_result'].apply(lambda x: x.message),
    
    })
    df_result['column']=column_name
    df2_result = df_result.explode('message').reset_index(names=['row'])
    return df2_result

def get_dirty(df : pd.DataFrame)-> pd.DataFrame:
    final_df = df[df["status"]=="dirty"]
    return final_df