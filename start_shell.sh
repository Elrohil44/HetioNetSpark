#!/bin/sh

${SPARK_HOME}/bin/spark-shell --conf spark.neo4j.bolt.password=password \
 --packages neo4j-contrib:neo4j-spark-connector:2.4.0-M6
