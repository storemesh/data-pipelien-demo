FROM jupyter/all-spark-notebook:notebook-6.5.3
USER root
# Install python deps including lakeFS client
COPY requirements.txt .
RUN pip3 install -r requirements.txt
RUN rm requirements.txt

# Set up iPython pretty tables and Spark SQL magic
RUN mkdir -p /home/jovyan/.ipython/profile_default/startup
COPY ipython/startup/00-prettytables.py /home/jovyan/.ipython/profile_default/startup

# Set environment vars so that pyspark is available outside of Jupyter
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH

# Disable the "Would you like to receive official Jupyter news?" popup
RUN jupyter labextension disable "@jupyterlab/apputils-extension:announcements"

