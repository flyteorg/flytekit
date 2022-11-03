ARG PYTHON_VERSION
FROM python:${PYTHON_VERSION}-slim-buster AS builder

RUN apt-get update && apt-get install build-essential -y

RUN mkdir /root/wheels/
RUN pip wheel --no-deps -w /root/wheels/ phik

FROM python:${PYTHON_VERSION}-slim-buster

MAINTAINER Flyte Team <users@flyte.org>
LABEL org.opencontainers.image.source https://github.com/flyteorg/flytekit

WORKDIR /root
ENV PYTHONPATH /root

ARG VERSION
ARG DOCKER_IMAGE

RUN mkdir /root/wheels/
COPY --from=builder /root/wheels/ /root/wheels/

# Pod tasks should be exposed in the default image
RUN pip install -U /root/wheels/*.whl flytekit==$VERSION \
	flytekitplugins-pod==$VERSION \
	flytekitplugins-deck-standard==$VERSION \
	flytekitplugins-data-fsspec==$VERSION \
	scikit-learn

RUN rm -rf /root/wheels/

ENV FLYTE_INTERNAL_IMAGE "$DOCKER_IMAGE"
