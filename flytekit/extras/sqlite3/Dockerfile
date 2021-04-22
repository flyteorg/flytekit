FROM python:3.8-slim-buster

ENV FLYTE_INTERNAL_IMAGE=flytecli-sqlite3:123

RUN pip install awscli

RUN pip install -U https://github.com/flyteorg/flytekit/archive/188e2c29305e7ed733f39edb0a7c5d2719ba2556.zip#egg=flytekit
