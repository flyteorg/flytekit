FROM python:3.8-slim-buster

ENV FLYTE_INTERNAL_IMAGE=flytecli-sqlite3:123

RUN pip install awscli

RUN pip install -U https://github.com/flyteorg/flytekit/archive/f73c5ab869424ac0cd89f23ff1cf23e1c229ae15.zip#egg=flytekit
