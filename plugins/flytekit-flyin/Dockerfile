FROM python:3.9-slim-buster
USER root
WORKDIR /root
ENV PYTHONPATH /root

# Install necessary packages
RUN apt-get update && apt-get install build-essential -y
RUN apt-get install git -y

# Install additional dependencies
RUN apt-get -y install curl
RUN mkdir -p /tmp/
RUN mkdir -p /tmp/code-server
RUN curl -kfL -o /tmp/code-server/code-server-4.18.0-linux-amd64.tar.gz https://github.com/coder/code-server/releases/download/v4.18.0/code-server-4.18.0-linux-amd64.tar.gz
RUN tar -xzf /tmp/code-server/code-server-4.18.0-linux-amd64.tar.gz -C /tmp/code-server/

ENV PATH="/tmp/code-server/code-server-4.18.0-linux-amd64/bin:${PATH}"

RUN wget https://open-vsx.org/api/ms-python/python/2023.20.0/file/ms-python.python-2023.20.0.vsix -P /tmp/code-server

RUN wget https://open-vsx.org/api/ms-toolsai/jupyter/2023.9.100/file/ms-toolsai.jupyter-2023.9.100.vsix -P /tmp/code-server

RUN code-server --install-extension /tmp/code-server/ms-python.python-2023.20.0.vsix
RUN code-server --install-extension /tmp/code-server/ms-toolsai.jupyter-2023.9.100.vsix


# Install Python packages
RUN pip install -U git+https://github.com/Future-Outlier/flytekit.git@77dd7dd767feae3636fbe541b40a43a1bd11e216#subdirectory=plugins/flytekit-flyin
# RUN pip install flytekitplugins-flyin
