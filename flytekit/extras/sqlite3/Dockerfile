FROM python:3.8-slim-buster

RUN pip install -U https://github.com/flyteorg/flytekit/archive/586eed58b281ef0ca089467cbcc5d84901e133b9.zip#egg=flytekit

COPY executor.py /opt
RUN chmod 777 /opt/executor.py

