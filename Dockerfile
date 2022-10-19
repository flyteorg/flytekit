ARG PYTHON_VERSION
FROM python:${PYTHON_VERSION}-slim-buster

MAINTAINER Flyte Team <users@flyte.org>
LABEL org.opencontainers.image.source https://github.com/flyteorg/flytekit

WORKDIR /root
ENV PYTHONPATH /root

RUN pip install awscli
RUN pip install gsutil

ARG VERSION
ARG DOCKER_IMAGE

# Pod tasks should be exposed in the default image
RUN pip install -U flytekit==$VERSION flytekitplugins-pod==$VERSION flytekitplugins-deck-standard==$VERSION scikit-learn

ENV FLYTE_INTERNAL_IMAGE "$DOCKER_IMAGE"
