FROM python:3.9-slim-buster
MAINTAINER Flyte Team <users@flyte.org>
LABEL org.opencontainers.image.source=https://github.com/flyteorg/flytekit

# ARG VERSION
RUN apt-get update && apt-get install -y git

RUN pip install git+https://github.com/flyteorg/flytekit.git@1dad13341a052fc035df737074eb26b6bb1a41d1#subdirectory=plugins/flytekit-sensor
RUN pip install git+https://github.com/flyteorg/flytekit.git@1dad13341a052fc035df737074eb26b6bb1a41d1
RUN pip install msgpack

CMD pyflyte serve --port 8000
