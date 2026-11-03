# Reference worker image for AWS EMR Serverless tasks running in Pythonic
# Spark mode. Script Spark and Hive jobs do not need flytekit on the worker
# and can use the upstream EMR Serverless Spark image directly.
#
# Build:
#   docker build --build-arg VERSION=<flytekit-version> \
#       -t <registry>/emr-serverless-flytekit:<tag> .
ARG EMR_BASE_IMAGE=public.ecr.aws/emr-serverless/spark/emr-7.0.0:latest
FROM ${EMR_BASE_IMAGE}
LABEL org.opencontainers.image.source=https://github.com/flyteorg/flytekit

ARG VERSION

USER root

RUN pip3 install --no-cache-dir --ignore-installed \
        "flytekit==${VERSION}" \
        "flytekitplugins-awsemrserverless==${VERSION}"

USER hadoop:hadoop
