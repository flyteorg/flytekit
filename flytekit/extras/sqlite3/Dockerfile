FROM python:3.8-slim-buster

ENV FLYTE_INTERNAL_IMAGE=flytecli-sqlite3:123

RUN pip install awscli

RUN pip install -U https://github.com/flyteorg/flytekit/archive/d3d3a97cb1e5bf67638e7f45270fb354a51606af.zip#egg=flytekit
