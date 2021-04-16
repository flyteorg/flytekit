FROM python:3.8-slim-buster

RUN pip install -U https://github.com/flyteorg/flytekit/archive/4c715451e1060b56fabaa04c2b6ffc7b38910c41.zip#egg=flytekit

COPY executor.py /opt
RUN chmod 777 /opt/executor.py

