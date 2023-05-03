# https://github.com/apache/spark/blob/master/resource-managers/kubernetes/docker/src/main/dockerfiles/spark/bindings/python/Dockerfile
FROM apache/spark-py:3.3.1
LABEL org.opencontainers.image.source=https://github.com/flyteorg/flytesnacks

USER 0
RUN ln -s /usr/bin/python3 /usr/bin/python

ARG VERSION=v1.6.0a1
RUN pip install flytekitplugins-spark==$VERSION
RUN pip install flytekit==$VERSION

RUN chown -R ${spark_uid}:${spark_uid} /root
WORKDIR /root
USER ${spark_uid}

