FROM python:3.11-slim

# Java for Spark
RUN apt-get update && apt-get install -y default-jre default-jdk curl && rm -rf /var/lib/apt/lists/*

# Spark
ENV SPARK_VERSION=3.4.3
RUN curl -L https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz -o /tmp/spark.tgz &&     tar -xzf /tmp/spark.tgz -C /opt &&     ln -s /opt/spark-$SPARK_VERSION-bin-hadoop3 /opt/spark &&     rm /tmp/spark.tgz
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "-m", "src.migration", "--dry-run", "true", "--mysql-dbtable", "source_db.customer_orders", "--snowflake-table", "ANALYTICS.PUBLIC.CUSTOMER_ORDERS"]
