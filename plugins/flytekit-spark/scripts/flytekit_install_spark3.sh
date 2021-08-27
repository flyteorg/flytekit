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

# Fetch Spark Distribution
wget https://archive.apache.org/dist/spark/spark-3.0.1/spark-3.0.1-bin-hadoop3.2.tgz -O spark-dist.tgz
echo 'e2d05efa1c657dd5180628a83ea36c97c00f972b4aee935b7affa2e1058b0279  spark-dist.tgz' | sha256sum --check
mkdir -p spark-dist
tar -xvf spark-dist.tgz -C spark-dist --strip-components 1

#Copy over required files
cp -rf spark-dist/jars /opt/spark/jars
cp -rf spark-dist/examples /opt/spark/examples
cp -rf spark-dist/python /opt/spark/python
cp -rf spark-dist/bin /opt/spark/bin
cp -rf spark-dist/sbin /opt/spark/sbin
cp -rf spark-dist/data /opt/spark/data
# Entrypoint for Driver/Executor pods
cp spark-dist/kubernetes/dockerfiles/spark/entrypoint.sh /opt/entrypoint.sh
chmod +x /opt/entrypoint.sh

rm -rf spark-dist.tgz
rm -rf spark-dist

# Hadoop dist (via Apache) has older AWS SDK version. Fetch required AWS jars from maven directly (not-ideal) to support IAM role
# https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts-minimum-sdk.html
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar -P /opt/spark/jars
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.740/aws-java-sdk-bundle-1.11.740.jar -P /opt/spark/jars
