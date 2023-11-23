ARG PYTHON_VERSION
FROM python:${PYTHON_VERSION}-slim-buster
MAINTAINER Flyte Team <users@flyte.org>
LABEL org.opencontainers.image.source https://github.com/flyteorg/flytekit
WORKDIR /root
ENV PYTHONPATH /root

# Install necessary packages
RUN apt-get update && apt-get install build-essential libmagic1 -y
RUN apt-get install git -y

# Install code-server and its extensions
RUN apt-get -y install curl
RUN apt-get -y install wget
RUN mkdir -p /tmp/
RUN mkdir -p /tmp/code-server
RUN curl -kfL -o /tmp/code-server/code-server-4.18.0-linux-amd64.tar.gz https://github.com/coder/code-server/releases/download/v4.18.0/code-server-4.18.0-linux-amd64.tar.gz
RUN tar -xzf /tmp/code-server/code-server-4.18.0-linux-amd64.tar.gz -C /tmp/code-server/

ENV PATH="/tmp/code-server/code-server-4.18.0-linux-amd64/bin:${PATH}"

RUN wget https://open-vsx.org/api/ms-python/python/2023.20.0/file/ms-python.python-2023.20.0.vsix -P /tmp/code-server
RUN wget https://open-vsx.org/api/ms-toolsai/jupyter/2023.9.100/file/ms-toolsai.jupyter-2023.9.100.vsix -P /tmp/code-server

RUN code-server --install-extension /tmp/code-server/ms-python.python-2023.20.0.vsix
RUN code-server --install-extension /tmp/code-server/ms-toolsai.jupyter-2023.9.100.vsix

# Install Flyin packages
COPY . ../flytekit-flyin
RUN pip install -e /flytekit-flyin

ENV FLYTE_SDK_LOGGING_LEVEL=20

RUN useradd -u 1000 flytekit
RUN chown flytekit: /root
RUN chown flytekit: /home
USER flytekit
