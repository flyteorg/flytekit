ARG PYTHON_VERSION
FROM python:${PYTHON_VERSION}-slim-buster

MAINTAINER Flyte Team <users@flyte.org>
LABEL org.opencontainers.image.source=https://github.com/flyteorg/flytekit

WORKDIR /root
ENV PYTHONPATH /root

ARG VERSION
ARG DOCKER_IMAGE

RUN apt-get update \
    && apt-get install -y build-essential \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Pod tasks should be exposed in the default image
RUN pip install --no-cache-dir -U flytekit==$VERSION
RUN useradd -u 1000 flytekit
# Some packages will create config file under /home by default, so we need to make sure it's writable
RUN chown flytekit: /root /home
USER flytekit

ENV FLYTE_INTERNAL_IMAGE "$DOCKER_IMAGE"
