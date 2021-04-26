FROM python:3.8-slim-buster

ENV FLYTE_INTERNAL_IMAGE=flytecli-sqlite3:123

RUN pip install awscli

RUN pip install -U https://github.com/flyteorg/flytekit/archive/e941fca66b8df526e0ea339a19ec62c84a84f911.zip#egg=flytekit
