FROM python:3.8-slim-buster

ENV FLYTE_INTERNAL_IMAGE=flytecli-sqlite3:v0.1.0

RUN pip install awscli

RUN pip install -U https://github.com/flyteorg/flytekit/archive/437530eec1420db6cf640a95810aaf50a2d0fb71.zip#egg=flytekit
