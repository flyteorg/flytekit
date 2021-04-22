FROM python:3.8-slim-buster

ENV FLYTE_INTERNAL_IMAGE=flytecli-sqlite3:123

RUN pip install awscli

RUN pip install -U https://github.com/flyteorg/flytekit/archive/fb701cb03fbcc4cdd1daa48db1192a45fa2bce3a.zip#egg=flytekit
