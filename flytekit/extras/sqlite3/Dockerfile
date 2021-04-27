FROM python:3.8-slim-buster

ENV FLYTE_INTERNAL_IMAGE=flytecli-sqlite3:123

RUN pip install awscli

RUN pip install -U https://github.com/flyteorg/flytekit/archive/6bf72ad7ce4bd9b2f04279b9b553a846315ac0a3.zip#egg=flytekit
