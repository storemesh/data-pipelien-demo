import pandas as pd

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

def clensing(df : pd.DataFrame,col : str,function_list : list)-> pd.DataFrame:
    column_name = col
    df[column_name+'_result'] = df[column_name].apply(lambda x: _clensing(x,function_list))
    return df

def _clensing(x,function_list : list):
    monad = Monad(x)
    for f in function_list:
        monad |= f
    return monad

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
    df2_result = df2_result[["row","column","input","value","status","message"]]
    return df2_result

def get_dirty(df : pd.DataFrame)-> pd.DataFrame:
    final_df = df[df["status"]=="dirty"]
    return final_df