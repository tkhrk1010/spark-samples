FROM python:3.9-slim

# Install Java
RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME based on the installed Java version
RUN java_version=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d'.' -f1-2) && \
echo "Installed Java version: $java_version" && \
java_home=$(dirname "$(dirname "$(readlink -f "$(which java)")")") && \
echo "JAVA_HOME: $java_home" && \
echo "export JAVA_HOME=$java_home" >> ~/.bashrc

# Install PySpark
# https://spark.apache.org/downloads.html
RUN pip install pyspark==3.5.1

# Set SPARK_HOME environment variables
ENV SPARK_HOME=/usr/local/lib/python3.9/site-packages/pyspark

# Set PATH environment variable
ENV PATH=$PATH:$SPARK_HOME/bin

# Set the working directory
WORKDIR /app
