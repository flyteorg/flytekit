FROM python:3.8-slim-buster

RUN pip install -U https://github.com/flyteorg/flytekit/archive/8753e8bdba518ec37fd0ad98c082e3632fa7b915.zip#egg=flytekit

COPY executor.py /opt
RUN chmod 777 /opt/executor.py

