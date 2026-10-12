#!/bin/bash
set -e

# Hive 2.3 on JDK 21 needs the same module opens Spark passes to its own JVMs.
OPENS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED"

case "$1" in
  master)
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
      --host spark-master --port 7077 --webui-port 8080
    ;;
  worker)
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
      --cores "${SPARK_WORKER_CORES:-2}" --memory "${SPARK_WORKER_MEMORY:-1g}" \
      --webui-port 8081 spark://spark-master:7077
    ;;
  metastore)
    # the Hive 2.3 metastore that ships inside Spark's own jars, so client and server match
    exec java -Xmx256m -XX:+IgnoreUnrecognizedVMOptions $OPENS \
      -Dlog4j2.contextSelector=org.apache.logging.log4j.core.selector.ClassLoaderContextSelector \
      -cp "/opt/metastore/conf:/opt/spark/jars/*" \
      org.apache.hadoop.hive.metastore.HiveMetaStore -p 9083
    ;;
  *)
    exec "$@"
    ;;
esac
