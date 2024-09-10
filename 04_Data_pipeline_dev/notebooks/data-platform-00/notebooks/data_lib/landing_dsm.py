import psycopg2
import duckdb
import yaml
from jinja2 import Template
import os
import dask.dataframe as dd

lakefsEndPoint='192.168.28.40:8000'
lakefsAccessKey='access_key'
lakefsSecretKey='secret_key'

file_path = "/home/jovyan/notebooks/data-platform-00/notebooks/catalog.yml"

catalog_template = """
landing_{{ app_name }}_{{ source }}_{{ dbname }}_{{ table_name }}:
  type: dask.ParquetDataset
  filepath: s3a://data-platform-notebook/main/landing_zone/{{ app_name }}/{{ source }}/{{ dbname }}/{{ table_name }}.parquet
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

def list_table(db_params:dict)-> list :

    connection = psycopg2.connect(dbname=db_params["dbname"],user=db_params["user"],password=db_params["password"],host=db_params["host"],port=db_params["port"])
    cursor = connection.cursor()
    
    # Query to list tables
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public';
    """)
    list_table = []
    # Fetch all table names
    tables = cursor.fetchall()
    for table in tables:
        list_table.append(table[0])
    cursor.close()
    connection.close()
    return list_table

def landing_func(pg_credential: dict,select_tables:list):
    
    create_catalog(file_path)
    name_catalog = read_catalog(file_path)
    
    app = pg_credential["appliction"]
    sour = pg_credential["source"]
    user = pg_credential["user"]
    password = pg_credential["password"]
    host = pg_credential["host"]
    port = pg_credential["port"]
    db = pg_credential["dbname"]
    
    
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
        if i in select_tables:
            name_check = f"landing_lakefs_{i}"
            if name_catalog == None or name_check not in name_catalog.keys() :

                con.sql(f"CREATE OR REPLACE VIEW {i} AS SELECT * FROM db_pg.{i};")      

                template = Template(catalog_template)
                catalog_entry = template.render(app_name=app,source=sour,dbname=db,table_name=i)

                with open(file_path, 'a') as file:
                    file.write(catalog_entry)

                # Copy data to the s3 path
                con.execute(f"COPY {i} TO 's3a://data-platform-notebook/main/landing_zone/{app}/{sour}/{db}/{i}.parquet' (FORMAT PARQUET)")
            else:
                con.sql(f"CREATE OR REPLACE VIEW {i} AS SELECT * FROM db_pg.{i};")  
                con.execute(f"COPY {i} TO 's3a://data-platform-notebook/main/landing_zone/{app}/{sour}/{db}/{i}.parquet' (FORMAT PARQUET)")
    print("done")
    
def get_parquet(path:str)-> dd.DataFrame:
    prefix = "s3a://data-platform-notebook/main"
    path_file = f"{prefix}{path}"
    ddf = dd.read_parquet(path_file, 
      storage_options={
        'anon':False,
        'key': lakefsAccessKey,
        'secret': lakefsSecretKey,
        'client_kwargs':{
            'endpoint_url': 'http://'+lakefsEndPoint
        }
    })
    return ddf