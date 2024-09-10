import duckdb
import yaml
from jinja2 import Template
import os

catalog_template = """
landing_lakefs_{{ table_name }}:
  type: dask.ParquetDataset
  filepath: s3a://data-platform-01/main/landing_zone/{{ table_name }}.parquet
  credentials: lakefs
  
"""

def read_catalog(file_path):
    with open(file_path, 'r') as file:
        catalog = yaml.safe_load(file)
    return catalog

def create_catalog(path:str):
    if not os.path.exists(path):
        with open(path, 'w') as file:
            file.write('')
              
def landing_func(pg_credential: dict,lake_credential: dict,path:str,select:list):
    
    name_catalog = read_catalog(path)
    
    user = pg_credential["user"]
    password = pg_credential["pass"]
    host = pg_credential["host"]
    port = pg_credential["port"]
    db = pg_credential["db"]
    
    lakefsEndPoint = lake_credential["lakefsEndPoint"]
    lakefsAccessKey = lake_credential["lakefsAccessKey"]
    lakefsSecretKey = lake_credential["lakefsSecretKey"]
    
    PG_CON_STR=f"postgresql://{user}:{password}@{host}:{port}/{db}" 
    con = duckdb.connect(':memory:')
    # or run and create file for save state "db.duckdb"
    con.sql("""INSTALL postgres; LOAD postgres;""") 
    con.sql(f"""
INSTALL httpfs;
LOAD httpfs;
SET s3_endpoint="{lakefsEndPoint}";
SET s3_access_key_id="{lakefsAccessKey}";
SET s3_secret_access_key="{lakefsSecretKey}";
SET s3_url_style="path";
SET s3_use_ssl="false";
""")
    str_exc = f"ATTACH 'dbname={db} user={user} password={password} host={host} port={port}' AS db_pg (TYPE POSTGRES);"
    con.sql(str_exc)
    
    data = con.execute("SELECT * FROM duckdb_tables;").df()
    data_list = data['table_name'].tolist()

    for i in data_list:
        if i in select:
            name_check = f"landing_lakefs_{i}"
            if name_catalog == None or name_check not in name_catalog.keys() :

                con.sql(f"CREATE OR REPLACE VIEW {i} AS SELECT * FROM db_pg.{i};")      

                template = Template(catalog_template)
                catalog_entry = template.render(table_name=i)

                with open(path, 'a') as file:
                    file.write(catalog_entry)

                # Copy data to the s3 path
                con.execute(f"COPY {i} TO 's3a://data-platform-01/main/landing_zone/{i}.parquet' (FORMAT PARQUET)")
            else :
                con.sql(f"CREATE OR REPLACE VIEW {i} AS SELECT * FROM db_pg.{i};")
                con.execute(f"COPY {i} TO 's3a://data-platform-01/main/landing_zone/{i}.parquet' (FORMAT PARQUET)")