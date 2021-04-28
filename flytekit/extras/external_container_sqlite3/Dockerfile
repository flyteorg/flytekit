FROM python:3.8-slim-buster

ENV FLYTE_INTERNAL_IMAGE=flytecli-sqlite3:v0.1.0

RUN pip install awscli

RUN pip install -U https://github.com/flyteorg/flytekit/archive/83ea9ddc1f7d6a6268a4fd88528c27ec340b777d.zip#egg=flytekit
