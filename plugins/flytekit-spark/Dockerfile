# https://github.com/apache/spark/blob/master/resource-managers/kubernetes/docker/src/main/dockerfiles/spark/bindings/python/Dockerfile
FROM apache/spark-py:3.3.1
LABEL org.opencontainers.image.source=https://github.com/flyteorg/flytekit

USER 0
	@@ -11,10 +11,7 @@ ARG VERSION
RUN pip install --no-cache-dir flytekitplugins-spark==$VERSION flytekit==$VERSION

RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.2/hadoop-aws-3.2.2.jar -P /opt/spark/jars && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -P /opt/spark/jars

RUN wget https://github.com/GoogleCloudDataproc/hadoop-connectors/releases/download/v2.2.17/util-hadoop-hadoop3-2.2.17.jar -P /opt/spark/jars

RUN chown -R ${spark_uid}:${spark_uid} /root
WORKDIR /root
USER ${spark_uid}
