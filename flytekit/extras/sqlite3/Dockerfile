FROM python:3.8-slim-buster

RUN pip install -U https://github.com/flyteorg/flytekit/archive/d8d616fbc548873ba2511da0da0c341721508fa7.zip#egg=flytekit

COPY executor.py /opt
RUN chmod 777 /opt/executor.py

