FROM python:3.8-slim-buster

MAINTAINER Flyte Team <users@flyte.org>
LABEL org.opencontainers.image.source https://github.com/flyteorg/flytekit

RUN pip install awscli
RUN pip install gsutil

ARG VERSION
ARG DOCKER_IMAGE

RUN pip install -U flytekit==0.19.1

WORKDIR /app

RUN pip install sqlalchemy

ENV FLYTE_INTERNAL_IMAGE "$DOCKER_IMAGE"