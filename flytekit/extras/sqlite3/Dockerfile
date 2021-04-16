FROM python:3.8-slim-buster

RUN pip install -U https://github.com/flyteorg/flytekit/archive/b4d09dbd3af40af347103b294f950af692e7cf2e.zip#egg=flytekit

COPY executor.py /opt
RUN chmod 777 /opt/executor.py

