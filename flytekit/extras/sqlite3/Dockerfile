FROM python:3.8-slim-buster

ENV FLYTE_INTERNAL_IMAGE=flytecli-sqlite3:123

RUN pip install awscli

RUN pip install -U https://github.com/flyteorg/flytekit/archive/4089babbdc7797d8e12203bda4c83c172f1c5758.zip#egg=flytekit
