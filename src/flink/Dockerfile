# Use the official Flink Java 11 image as the base
FROM flink:1.18-java11

# Install Python 3.10 and pip
RUN apt-get update -y && \
    apt-get install -y software-properties-common wget curl && \
    apt-get install -y python3.10 python3-pip python3.10-dev && \
    ln -s /usr/bin/python3.10 /usr/bin/python && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install PyFlink and Python clients used by the Flink jobs
RUN pip3 install --no-cache-dir apache-flink==1.18.0 cassandra-driver kafka-python-ng shapely

# Create directories for external jars
RUN mkdir -p /opt/flink/usrlib

# Download the required Kafka and Cassandra connector JARs into the Flink environment
# Flink requires these JARs to communicate with Kafka brokers and Cassandra clusters natively
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.18/flink-sql-connector-kafka-3.0.1-1.18.jar && \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-connector-cassandra_2.12/1.16.3/flink-connector-cassandra_2.12-1.16.3.jar

# Copy the arrondissements GeoJSON into the container
COPY arrondissements.geojson /opt/flink/usrlib/arrondissements.geojson

# Set the working directory
# WORKDIR removed – default entrypoint works from /opt/flink

# Dans src/flink/Dockerfile
RUN wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-cassandra_2.12/1.16.1/flink-connector-cassandra_2.12-1.16.1.jar -P /opt/flink/lib/
RUN wget https://repo1.maven.org/maven2/io/dropwizard/metrics/metrics-core/3.2.6/metrics-core-3.2.6.jar -P /opt/flink/lib/
