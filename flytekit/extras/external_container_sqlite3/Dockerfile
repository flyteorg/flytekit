FROM python:3.8-slim-buster

ENV FLYTE_INTERNAL_IMAGE=flytecli-sqlite3:v0.1.0

RUN pip install awscli

RUN pip install -U https://github.com/flyteorg/flytekit/archive/bb2efe681ad8b1f7c304df1a3e33a1c62dc98c7c.zip#egg=flytekit
