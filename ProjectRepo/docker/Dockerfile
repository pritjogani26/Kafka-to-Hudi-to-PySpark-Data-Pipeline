#  ProjectRepo/docker/Dockerfile
FROM bitnami/spark:3.4.1

# Use root for setup
USER root

# Install Python dependencies
RUN pip install pyspark==3.4.1 kafka-python

# Create directories and copy configs + jars
RUN mkdir -p /app/scripts /app/configs /opt/extra_jars

# Preload configs and jars
COPY configs/ /app/configs/
COPY jars/*.jar /opt/extra_jars/

# Set correct permissions for bitnami user (UID 1001)
RUN chown -R 1001:1001 /app /opt/extra_jars

# Switch to Spark user for security
USER 1001
WORKDIR /app
