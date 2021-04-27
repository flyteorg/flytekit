FROM python:3.8-slim-buster

ENV FLYTE_INTERNAL_IMAGE=flytecli-sqlite3:123

RUN pip install awscli

RUN pip install -U https://github.com/flyteorg/flytekit/archive/8f90d639d0095d17e3c6d6c92493f1e6f73b02e1.zip#egg=flytekit
