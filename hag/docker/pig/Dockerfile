FROM ubuntu:22.04

# Variables de versión
ARG HADOOP_VERSION=3.3.6
ARG PIG_VERSION=0.17.0

# Instalar dependencias básicas
RUN apt-get update && apt-get install -y \
    openjdk-8-jdk \
    curl \
    tar \
    bash \
    procps \
    python3 \
    && rm -rf /var/lib/apt/lists/*

# Instalar Hadoop
RUN curl -L -o /tmp/hadoop.tar.gz https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
 && tar -xf /tmp/hadoop.tar.gz -C /opt \
 && mv /opt/hadoop-${HADOOP_VERSION} /opt/hadoop \
 && rm /tmp/hadoop.tar.gz

# Instalar Pig
RUN curl -L -o /tmp/pig.tar.gz https://downloads.apache.org/pig/pig-${PIG_VERSION}/pig-${PIG_VERSION}.tar.gz \
 && tar -xf /tmp/pig.tar.gz -C /opt \
 && mv /opt/pig-${PIG_VERSION} /opt/pig \
 && rm /tmp/pig.tar.gz

# Variables de entorno
ENV HADOOP_HOME=/opt/hadoop
ENV PIG_HOME=/opt/pig
ENV PATH=$PATH:$HADOOP_HOME/bin:$PIG_HOME/bin
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

WORKDIR /opt/pig-scripts
CMD ["bash"]
