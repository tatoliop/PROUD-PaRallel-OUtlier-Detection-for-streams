#!/bin/bash

#Outlier detection job versions
export FLINK_JOB_VERSION=$(grep -oP 'flinkVersion = "\K[^"]+' ../build.sbt | perl -pe 's/\.0$//')
export SCALA_JOB_VERSION=$(grep -oP 'ThisBuild / scalaVersion := "\K[^"]+' ../build.sbt | perl -pe 's/\.\d+$//')
export OUTLIER_JOB_VERSION=$(grep -oP 'ThisBuild / version := "\K[^"]+' ../build.sbt)
export OUTLIER_JOB_NAME=PROUD-assembly-${OUTLIER_JOB_VERSION}.jar
#Docker images
export ZOOKEEPER_IMAGE="zookeeper:3.5"
export KAFKA_IMAGE="wurstmeister/kafka:2.12-2.2.1"
export INFLUX_IMAGE="influxdb:1.7.9"
export GRAFANA_IMAGE="grafana/grafana:6.5.2"
export FLINK_IMAGE="flink_outliers"
export FLINK_VERSION="${FLINK_JOB_VERSION}-scala_${SCALA_JOB_VERSION}"
#Variables
export myid=$(id -u)
export ARTIFACT_PATH="/opt/artifacts"
export INFLUX_HOST="influxdb"
export INFLUX_PORT="8086"
export INFLUX_SERVER="http://$INFLUX_HOST:$INFLUX_PORT"
export INFLUX_USER="admin"
export INFLUX_PASSWORD="flink"
export INFLUX_DB="outliers"
export INFLUX_DB_METRICS="metrics"
export GRAFANA_PASSWORD="flink"
export KAFKA_TOPIC="outliers_input"
export KAFKA_BROKERS="kafka:9094"
export FLINK_PROPERTIES="FLINK_PROPERTIES=metrics.reporter.influxdb.class: org.apache.flink.metrics.influxdb.InfluxdbReporter\n
metrics.reporter.influxdb.host: ${INFLUX_HOST}\n
metrics.reporter.influxdb.port: ${INFLUX_PORT}\n
metrics.reporter.influxdb.db: ${INFLUX_DB_METRICS}\n
metrics.reporter.influxdb.username: ${INFLUX_USER}\n 
metrics.reporter.influxdb.password: ${INFLUX_PASSWORD}"
