FROM openjdk:11-slim

ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

RUN apt-get update && \
    apt-get install -y python3 python3-pip curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | \
    tar -xz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH=/app

CMD ["python3", "-m", "src.main"]
