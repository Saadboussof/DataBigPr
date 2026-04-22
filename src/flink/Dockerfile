# Use the official Flink Java 11 image as the base
FROM flink:1.18-java11

# Install Python 3.10 and pip
RUN apt-get update -y && \
    apt-get install -y software-properties-common wget curl && \
    apt-get install -y python3.10 python3-pip python3.10-dev && \
    ln -s /usr/bin/python3.10 /usr/bin/python && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install PyFlink and the Cassandra Driver for Python (so the Job runs flawlessly)
RUN pip3 install --no-cache-dir apache-flink==1.18.0 cassandra-driver

# Create directories for external jars
RUN mkdir -p /opt/flink/usrlib

# Download the required Kafka and Cassandra connector JARs into the Flink environment
# Flink requires these JARs to communicate with Kafka brokers and Cassandra clusters natively
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.18/flink-sql-connector-kafka-3.0.1-1.18.jar && \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-connector-cassandra_2.12/1.16.3/flink-connector-cassandra_2.12-1.16.3.jar

# Set the working directory
WORKDIR /opt/flink/bin
