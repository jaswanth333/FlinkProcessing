FROM flink:2.2.0

USER root

# Optional: make Flink version configurable
ARG FLINK_VERSION=2.2.0

# Install system deps + Python 3 + pip (Debian/Ubuntu style)
RUN set -ex; \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 python3-venv python3-pip wget ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Create virtualenv for PyFlink
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

# Upgrade pip and install Python deps
# apache-flink (Java) is already in the base image; you mainly need pyflink + clients
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir apache-flink psycopg2-binary 



# Download connector libraries (SQL connectors matching Flink 2.2.x)
# Adjust versions if you need newer patches, but keep them compatible with FLINK_VERSION
#wget -q -P /opt/flink/lib/ "https://repo.maven.apache.org/maven2/org/apache/flink/flink-json/${FLINK_VERSION}/flink-json-${FLINK_VERSION}.jar"; \
RUN wget -q -P /opt/flink/lib/ "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar"; \
    wget -q -P /opt/flink/lib/ "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7/4.0.0-2.0/flink-sql-connector-elasticsearch7-4.0.0-2.0.jar" \
    wget -q -P /opt/flink/lib/ "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar"

# Optional: increase metaspace for Python-heavy jobs
RUN echo "taskmanager.memory.jvm-metaspace.size: 512m" >> /opt/flink/conf/flink-conf.yaml

WORKDIR /opt/flink

USER flink
