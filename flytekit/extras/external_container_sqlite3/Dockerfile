FROM python:3.8-slim-buster

ENV FLYTE_INTERNAL_IMAGE=flytecli-sqlite3:v0.1.0

RUN pip install awscli

RUN pip install -U https://github.com/flyteorg/flytekit/archive/ceb80db9a09928f64afded5fe9ff46ce7cce03fd.zip#egg=flytekit
