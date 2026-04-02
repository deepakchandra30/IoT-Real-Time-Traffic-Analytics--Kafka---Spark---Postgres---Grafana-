FROM apache/spark-py:v3.4.0

USER root

# Python deps for Spark job (inside container)
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Spark Kafka + JDBC jars
# (packages are resolved at submit time via --packages flag)
COPY job.py /opt/spark/job.py

USER 185

CMD ["/opt/spark/bin/spark-submit", \
  "--master", "local[*]", \
  "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.7.3", \
  "/opt/spark/job.py" ]