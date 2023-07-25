FROM python:3.9-slim-buster
MAINTAINER Flyte Team <users@flyte.org>
LABEL org.opencontainers.image.source=https://github.com/flyteorg/flytekit

# ARG VERSION
RUN apt-get update && apt-get install -y git

RUN pip install git+https://github.com/flyteorg/flytekit.git@72d7ec75acdcc9c9ff92dd94a01bf643d4f2b313#subdirectory=plugins/flytekit-sensor
RUN pip install git+https://github.com/flyteorg/flytekit.git@72d7ec75acdcc9c9ff92dd94a01bf643d4f2b313
RUN pip install msgpack

CMD pyflyte serve --port 8000
