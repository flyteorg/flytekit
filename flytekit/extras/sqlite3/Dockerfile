FROM python:3.8-slim-buster

RUN pip install -U https://github.com/flyteorg/flytekit/archive/0646c76a9789cc5277d67193a8459675c198f269.zip#egg=flytekit

COPY executor.py /opt
RUN chmod 777 /opt/executor.py

