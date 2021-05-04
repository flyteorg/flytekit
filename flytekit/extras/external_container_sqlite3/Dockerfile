FROM python:3.8-slim-buster

ENV FLYTE_INTERNAL_IMAGE=flytecli-sqlite3:v0.1.0

RUN pip install awscli

RUN pip install -U https://github.com/flyteorg/flytekit/archive/d6bd796b73b5525493fec345712cc65eb6c170e5.zip#egg=flytekit
