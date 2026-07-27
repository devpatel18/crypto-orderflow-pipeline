FROM flink:1.20.1-scala_2.12

# PyFlink: the base image ships no Python. Ubuntu jammy's python3 is 3.10,
# which PyFlink 1.20 supports (3.8-3.11). pemja has no arm64 wheel and
# compiles against JNI headers, so a JDK + gcc are needed at build time
# (the base image only has a JRE); runtime still uses /opt/java/openjdk.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 python3-pip python3-dev curl openjdk-11-jdk-headless build-essential && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

RUN JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac)))) \
    pip3 install --no-cache-dir apache-flink==1.20.1

# Connectors: Kafka source, Iceberg runtime + AWS bundle (S3FileIO for MinIO),
# Postgres JDBC (Iceberg JDBC catalog), and shaded Hadoop (Iceberg's Flink
# integration needs org.apache.hadoop.conf.Configuration on the classpath).
ARG MAVEN=https://repo1.maven.org/maven2
RUN cd /opt/flink/lib && \
    curl -fsSLO $MAVEN/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar && \
    curl -fsSLO $MAVEN/org/apache/iceberg/iceberg-flink-runtime-1.20/1.8.1/iceberg-flink-runtime-1.20-1.8.1.jar && \
    curl -fsSLO $MAVEN/org/apache/iceberg/iceberg-aws-bundle/1.8.1/iceberg-aws-bundle-1.8.1.jar && \
    curl -fsSLO $MAVEN/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar && \
    curl -fsSLO $MAVEN/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar
