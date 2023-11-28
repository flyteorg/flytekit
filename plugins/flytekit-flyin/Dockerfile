ARG PYTHON_VERSION
FROM python:${PYTHON_VERSION}-slim-buster
MAINTAINER Flyte Team <users@flyte.org>
LABEL org.opencontainers.image.source https://github.com/flyteorg/flytekit
WORKDIR /root
ENV PYTHONPATH /root

# Install necessary packages
# 1. Update the necessary packages for flytekit
# 2. Install code-server
# 3. Download code-server extensions via wget
RUN apt-get update \
    && apt-get install build-essential git wget -y \
    && mkdir -p /tmp/ \
    && mkdir -p /tmp/code-server \
    && wget --no-check-certificate -O /tmp/code-server/code-server-4.18.0-linux-amd64.tar.gz https://github.com/coder/code-server/releases/download/v4.18.0/code-server-4.18.0-linux-amd64.tar.gz \
    && tar -xzf /tmp/code-server/code-server-4.18.0-linux-amd64.tar.gz -C /tmp/code-server/ \
    && wget https://open-vsx.org/api/ms-python/python/2023.20.0/file/ms-python.python-2023.20.0.vsix -P /tmp/code-server \
    && wget https://open-vsx.org/api/ms-toolsai/jupyter/2023.9.100/file/ms-toolsai.jupyter-2023.9.100.vsix -P /tmp/code-server

# Set the environment variable for code-server
ENV PATH="/tmp/code-server/code-server-4.18.0-linux-amd64/bin:${PATH}"

# 1. Use code-server to install the extensions
# 2. Install flytekit and flytekit-flyin with no cache
# 3. Delete apt cache. Reference: https://gist.github.com/marvell/7c812736565928e602c4
# 4. Some packages will create config file under /home by default, so we need to make sure it's writable
# 5. Change the permission of /tmp, so that others can run command on it
RUN code-server --install-extension /tmp/code-server/ms-python.python-2023.20.0.vsix \
    && code-server --install-extension /tmp/code-server/ms-toolsai.jupyter-2023.9.100.vsix \
    && pip install --no-cache-dir -U flytekit-flyin==$VERSION flytekit==$VERSION \
    && apt-get clean autoclean \
    && apt-get autoremove --yes \
    && rm -rf /var/lib/{apt,dpkg,cache,log}/
    && useradd -u 1000 flytekit \
    && chown flytekit: /root \
    && chown flytekit: /home \
    && chown -R flytekit:flytekit /tmp \
    && chmod 755 /tmp

USER flytekit

ENV FLYTE_INTERNAL_IMAGE "$DOCKER_IMAGE"
