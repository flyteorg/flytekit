#!/bin/bash

# Fetches and install Spark and its dependencies. To be invoked by the Dockerfile

# echo commands to the terminal output
set -ex

# Install JDK
apt-get update -y && \
    apt-get install -y software-properties-common && \
    add-apt-repository ppa:openjdk-r/ppa && \
    apt-get update -y && \
    apt-get install -y --force-yes ca-certificates-java && \
    apt-get install -y --force-yes openjdk-8-jdk && \
    apt-get install -y wget && \
    update-java-alternatives -s java-1.8.0-openjdk-amd64 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

mkdir -p /opt/spark
mkdir -p /opt/spark/work-dir
touch /opt/spark/RELEASE

# Fetch Spark Distribution with PySpark K8 support
wget https://archive.apache.org/dist/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz -O spark-dist.tgz
echo '80a4c564ceff0d9aff82b7df610b1d34e777b45042e21e2d41f3e497bb1fa5d8  spark-dist.tgz' | sha256sum --check
mkdir -p spark-dist
tar -xvf spark-dist.tgz -C spark-dist --strip-components 1

#Copy over required files
cp -rf spark-dist/jars /opt/spark/jars
cp -rf spark-dist/examples /opt/spark/examples
cp -rf spark-dist/python /opt/spark/python
cp -rf spark-dist/bin /opt/spark/bin
cp -rf spark-dist/sbin /opt/spark/sbin
cp -rf spark-dist/data /opt/spark/data

rm -rf spark-dist.tgz
rm -rf spark-dist

# Fetch Hadoop Distribution with AWS Support
wget http://apache.mirrors.tds.net/hadoop/common/hadoop-2.7.7/hadoop-2.7.7.tar.gz -O hadoop-dist.tgz
echo 'd129d08a2c9dafec32855a376cbd2ab90c6a42790898cabbac6be4d29f9c2026  hadoop-dist.tgz' | sha256sum --check
mkdir -p hadoop-dist
tar -xvf hadoop-dist.tgz -C hadoop-dist --strip-components 1

cp -rf hadoop-dist/share/hadoop/tools/lib/hadoop-aws-2.7.7.jar /opt/spark/jars
cp -rf hadoop-dist/share/hadoop/tools/lib/aws-java-sdk-1.7.4.jar /opt/spark/jars

rm -rf hadoop-dist.tgz
rm -rf hadoop-dist

# Patch latest k8sclient for https://issues.apache.org/jira/browse/SPARK-28921. Ref: https://github.com/apache/spark/pull/25640/
rm /opt/spark/jars/kubernetes-client-4.1.2.jar
rm /opt/spark/jars/kubernetes-model-4.1.2.jar
rm /opt/spark/jars/kubernetes-model-common-4.1.2.jar

wget https://repo1.maven.org/maven2/io/fabric8/kubernetes-client/4.4.2/kubernetes-client-4.4.2.jar -P /opt/spark/jars
wget https://repo1.maven.org/maven2/io/fabric8/kubernetes-model/4.4.2/kubernetes-model-4.4.2.jar -P /opt/spark/jars
wget https://repo1.maven.org/maven2/io/fabric8/kubernetes-model-common/4.4.2/kubernetes-model-common-4.4.2.jar -P /opt/spark/jars
