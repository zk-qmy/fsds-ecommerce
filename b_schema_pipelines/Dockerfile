FROM python:3.13-slim

# PySpark requires Java; curl is used to fetch S3A JARs from Maven Central
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless curl && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

RUN pip install --no-cache-dir uv

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen

ENV PATH=/app/.venv/bin:$PATH
ENV PYTHONPATH=/app

# Download S3A JARs to /opt/spark-s3a — outside /app/ so the volume mount
# "- /app/.venv" does not shadow them at runtime.
# Versions must match the Hadoop bundled in PySpark 4.1.0 (Hadoop 3.3.4).
# RUN mkdir -p /opt/spark-s3a && \
#     curl -fSL -o /opt/spark-s3a/hadoop-aws-3.3.4.jar \
#       https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
#     curl -fSL -o /opt/spark-s3a/aws-java-sdk-bundle-1.12.262.jar \
#       https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Source is volume-mounted in dev — COPY is only for standalone builds
COPY . .
