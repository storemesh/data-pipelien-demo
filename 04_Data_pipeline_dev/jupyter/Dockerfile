FROM python:3.12-slim
RUN apt-get update --fix-missing

RUN apt install -y git gcc g++

WORKDIR /jupyter
ADD ./requirements.txt requirements.txt
RUN pip install -r requirements.txt --use-deprecated=legacy-resolver
ADD . /jupyter/
WORKDIR /jupyter