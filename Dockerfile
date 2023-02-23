ARG PYTHON_VERSION
FROM python:${PYTHON_VERSION}-slim-buster

MAINTAINER Flyte Team <users@flyte.org>
LABEL org.opencontainers.image.source https://github.com/flyteorg/flytekit

WORKDIR /root
ENV PYTHONPATH /root

ARG VERSION
ARG DOCKER_IMAGE

RUN apt-get update && apt-get install build-essential -y

RUN useradd -u 1000 flytekit
RUN chown flytekit: /root
USER flytekit

# Pod tasks should be exposed in the default image
RUN pip install -U flytekit==$VERSION \
	flytekitplugins-pod==$VERSION \
	flytekitplugins-deck-standard==$VERSION \
	flytekitplugins-data-fsspec[aws]==$VERSION \
	flytekitplugins-data-fsspec[gcp]==$VERSION \
	scikit-learn

ENV FLYTE_INTERNAL_IMAGE "$DOCKER_IMAGE"
