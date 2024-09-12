# 04_Data_pipeline_dev

this project is a sandbox for data engineering practices and starter pack for development data pipeline
with virsual environment in container

## Getting started

first set up program it needs for use in this project
- [ ] [Docker Desktop] or use docker cli for run the container of project
- [ ] [IDE] download IDE that use easy for use but in this for ject use vs studio code
- [ ] git clone project to your work local directory

## Run Docker-compose for start your project

- [ ] cd to /04_Data_pipeline_dev your project directory 
(if you clone git already it might be at "../data-pipeline-demo")
- [ ] run this command "docker compose up -d" or "docker-compose up -d" 
(for difference docker compose version)
- [ ] check docker desktop for make sure that you container is runing you will see 3 container

```
postgresql-dev is running on port 5432
lakefs-dev is running on port 8000
notebook-dev is running on port 8888
```

- [ ] for sure you can access you container please connect with browser "localhost:8888" or "localhost:8000" 
(jupyter notebook,and lakefs)

## postgresql

a container database that contain database name chinook and northwind (opendata)

- [ ] [check] if the lakefs container is running and works you can access with this code

```
docker exec -it 04_data_pipeline_dev-postgresql_dev-1 psql -U admin -d chinook
```
if work you terminal will cd to the postgres container and can list database name and table name with

```
\l
\dt
```

if you want to disconnect from postgres container you run this command 

```
\q
```

## Lakefs

a container s3 file storage system and git like structure. you can create repository, push data, create branch or commit file change.
can use data version control with lekefs

- [ ] [check] if the lakefs container is running access with http://localhost:8000
- [ ] [prepare lakefs] in the lekefs for work with data pipeline you must create repository first. the repository that you must create 
    is here

```
data-platform-01
data-platform-notebook
```

- [ ] [recheck] if you run the code in jupyter notebook you can check that result data can see in each repository

## jupyter notebook

- [ ] [check] if the jupyter notebook container is running access with http://localhost:8888
- [ ] connect with password "password"

### Test data pipline with jupyter notebook 

1. cd to /data-platform-00/notebook at the left nevigate panel
2. you can run data_plaform_00.ipynb and data_plaform_01.ipynb step by step in each notebook 

### Test data pipeline with kedro

1. cd to /data-platform-00 (project path)
2. you can run and check with this code 
```
kedro run pipeline=landing      #the landing data is on lakefs
kedro run pipeline=staging      #show sample data from lakefs with dask dataframe
```
3. check data pipeline with jupyter notebook you can use at /data-platform-00/notebook
you will see the notebook name "kedro-viz.ipynb" if notbook error that mean you not have kedro kernel
you can follow these step

    3.1 at top left of jupyter click plus button \
    3.2 at other create new "Terminal" \
    3.3 run this command 

```
cd ..
kedro jupyter notebook
```

    3.4 return to your kedro-viz.ipynb and restart kernel and change to kedro kernel 
    (data-platform-00)

4. if you want a visualize data pipeline please this code at terminal in path directory "/data-platform-00"

```
kedro viz --host 0.0.0.0
```

now you can access kedro viz ui at http://localhost:4141

Have a good practices!