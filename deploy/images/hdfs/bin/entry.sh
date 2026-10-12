#!/bin/bash
set -e
case "$1" in
  namenode)
    if [ ! -d /data/nn/current ]; then
      hdfs namenode -format -nonInteractive -force
    fi
    exec hdfs namenode
    ;;
  datanode)
    exec hdfs datanode
    ;;
  *)
    exec "$@"
    ;;
esac
