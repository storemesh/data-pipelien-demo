import duckdb
import yaml
from jinja2 import Template
import os

catalog_template = """
l.{{ application }}.{{ source }}.{{ table_name }}:
  type: dask.ParquetDataset
  filepath: s3a://data-platform-01/main/landing_zone/{{ application }}/{{ source }}/{{ table_name }}/
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
              
def landing_func(pg_credential: dict,lake_credential: dict,path:str):
    
    name_catalog = read_catalog(path)

    con = duckdb.connect(':memory:')
    
    con.sql("""INSTALL postgres; LOAD postgres;""") 
    con.sql(f"""
            INSTALL httpfs; LOAD httpfs;
            SET s3_endpoint="{lake_credential["lakefsEndPoint"]}";
            SET s3_access_key_id="{lake_credential["lakefsAccessKey"]}";
            SET s3_secret_access_key="{lake_credential["lakefsSecretKey"]}";
            SET s3_url_style="path"; SET s3_use_ssl="false";
        """)
    con.sql(f"ATTACH '{pg_credential["host"]}' AS db_pg (TYPE POSTGRES);")
    
    data = con.execute("SELECT * FROM duckdb_tables;").df()
    data_list = data['table_name'].tolist()

    for i in data_list:
            name_check = f"l.{pg_credential["application"]}.{pg_credential["source"]}.{i}"
            sql_create = f"CREATE OR REPLACE VIEW {i} AS SELECT * FROM db_pg.{i};"
            sql = f"COPY {i} TO 's3a://data-platform-01/main/landing_zone/{pg_credential["application"]}/{pg_credential["source"]}/{i}.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE 100)"
            if name_catalog == None or name_check not in name_catalog.keys() :     

                template = Template(catalog_template)
                catalog_entry = template.render(application=pg_credential["application"],source=pg_credential["source"],table_name=i)

                with open(path, 'a') as file:
                    file.write(catalog_entry)
                    
                con.sql(sql_create) 
                con.execute(sql)
            else :
                con.sql(sql_create)
                con.execute(sql)