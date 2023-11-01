ARG PYTHON_VERSION
FROM python:${PYTHON_VERSION}-slim-buster

MAINTAINER Flyte Team <users@flyte.org>
LABEL org.opencontainers.image.source=https://github.com/flyteorg/flytekit

WORKDIR /root
ENV PYTHONPATH /root

ARG VERSION
ARG DOCKER_IMAGE

RUN apt-get update && apt-get install build-essential -y

# Pod tasks should be exposed in the default image
RUN pip install -U flytekit==$VERSION \
	flytekitplugins-pod==$VERSION \
	flytekitplugins-deck-standard==$VERSION \
	scikit-learn

RUN apt-get update && \
    apt-get install -y curl gnupg && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && \
    apt-get update -y && \
    apt-get install google-cloud-sdk -y

RUN mkdir -p /usr/share/man/man1 /usr/share/man/man2 && \
    apt-get update &&\
    apt-get install -y --no-install-recommends openjdk-11-jre && \
    apt-get install ca-certificates-java -y && \
    apt-get clean && \
    update-ca-certificates -f;
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/


RUN useradd -u 1000 flytekit
RUN chown flytekit: /root
# Some packages will create config file under /home by default, so we need to make sure it's writable
RUN chown flytekit: /home
USER flytekit

ENV FLYTE_INTERNAL_IMAGE "$DOCKER_IMAGE"
