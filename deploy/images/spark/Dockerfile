FROM apache/spark:4.2.0-python3

USER root
ARG MYSQL_JDBC=8.4.0
ADD https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/${MYSQL_JDBC}/mysql-connector-j-${MYSQL_JDBC}.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/mysql-connector-j-*.jar

WORKDIR /app
COPY requirements-analytics.txt .
RUN pip install --no-cache-dir -r requirements-analytics.txt

RUN mkdir -p /opt/spark/conf /opt/metastore/conf && chown -R spark:spark /opt/spark/conf /opt/metastore /app
COPY deploy/images/spark/conf/spark-defaults.conf /opt/spark/conf/
COPY deploy/images/spark/conf/hive-site.xml deploy/images/spark/conf/core-site.xml /opt/metastore/conf/
COPY deploy/images/spark/bin/entry.sh /opt/mdp-entry.sh
RUN chmod +x /opt/mdp-entry.sh

ENV PYTHONPATH=/app/src:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip \
    PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 \
    HADOOP_USER_NAME=hadoop \
    PATH=/opt/spark/bin:/opt/java/openjdk/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

COPY proto proto
COPY sql sql
COPY scripts scripts
COPY src src
COPY pyproject.toml .
RUN chown -R spark:spark /app
USER spark
ENTRYPOINT ["/opt/mdp-entry.sh"]
CMD ["sleep", "infinity"]
