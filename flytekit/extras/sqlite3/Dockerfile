FROM python:3.8-slim-buster

ENV FLYTE_INTERNAL_IMAGE=flytecli-sqlite3:123

RUN pip install awscli

RUN pip install -U https://github.com/flyteorg/flytekit/archive/052b1adc185c48dd260450c6b22a20fea88fbbac.zip#egg=flytekit
