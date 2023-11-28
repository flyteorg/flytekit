ARG PYTHON_VERSION
FROM python:${PYTHON_VERSION}-slim-buster

MAINTAINER Flyte Team <users@flyte.org>
LABEL org.opencontainers.image.source=https://github.com/flyteorg/flytekit

WORKDIR /root
ENV PYTHONPATH /root

ARG VERSION
ARG DOCKER_IMAGE

# Combining RUN commands with the && : pattern at the end
# 1. Update the necessary packages for flytekit
# 2. Delete apt cache. Reference: https://gist.github.com/marvell/7c812736565928e602c4
# 3. Change the permission of /tmp, so that others can run command on it
RUN apt-get update && apt-get install build-essential -y \
    && pip install --no-cache-dir -U flytekit==$VERSION \
        flytekitplugins-pod==$VERSION \
        flytekitplugins-deck-standard==$VERSION \
        scikit-learn \
    && apt-get clean autoclean \
    && apt-get autoremove --yes \
    && rm -rf /var/lib/{apt,dpkg,cache,log}/ \
    && useradd -u 1000 flytekit \
    && chown flytekit: /root \
    && chown flytekit: /home \
    && chown -R flytekit:flytekit /tmp \
    && chmod 755 /tmp \
    && :

USER flytekit

ENV FLYTE_INTERNAL_IMAGE "$DOCKER_IMAGE"
