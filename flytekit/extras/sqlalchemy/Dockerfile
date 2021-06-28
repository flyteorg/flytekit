FROM python:3.8-slim-buster

MAINTAINER Flyte Team <users@flyte.org>
LABEL org.opencontainers.image.source https://github.com/flyteorg/flytekit

RUN pip install awscli
RUN pip install gsutil

ARG VERSION
ARG DOCKER_IMAGE

RUN pip install -U flytekit==$VERSION

WORKDIR /app

# Install Python dependencies
COPY requirements.txt /app
RUN pip install -r /app/requirements.txt

ENV FLYTE_INTERNAL_IMAGE "$DOCKER_IMAGE"