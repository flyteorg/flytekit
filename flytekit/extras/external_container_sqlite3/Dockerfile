FROM python:3.8-slim-buster

ENV FLYTE_INTERNAL_IMAGE=flytecli-sqlite3:v0.1.0

RUN pip install awscli

RUN pip install -U https://github.com/flyteorg/flytekit/archive/f7b26d6ac35b168c0bda668290dbfc97b9dee4fa.zip#egg=flytekit
