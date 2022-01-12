#!/bin/bash

#Outlier detection job versions
export FLINK_JOB_VERSION=$(grep -oP 'flinkVersion = "\K[^"]+' build.sbt | perl -pe 's/\.0$//')
export SCALA_JOB_VERSION=$(grep -oP 'ThisBuild / scalaVersion := "\K[^"]+' build.sbt | perl -pe 's/\.\d+$//')
export OUTLIER_JOB_VERSION=$(grep -oP 'ThisBuild / version := "\K[^"]+' build.sbt)
#Docker images
export ZOOKEEPER_IMAGE="zookeeper:3.5"
export KAFKA_IMAGE="wurstmeister/kafka:2.12-2.2.1"
export INFLUX_IMAGE="influxdb:1.7.9"
export GRAFANA_IMAGE="grafana/grafana:6.5.2"
export REDIS_IMAGE="redis:6.0"
export FLINK_IMAGE="flink_outliers"
#Variables
export myid=$(id -u)
export ARTIFACT_PATH="/opt/artifacts"
#Redis connection
export REDIS_HOST="redis"
export REDIS_PORT="6379"
export REDIS_DB="flink_adapt"
#Influx connection
export INFLUX_HOST="influxdb"
export INFLUX_PORT="8086"
export INFLUX_SERVER="http://$INFLUX_HOST:$INFLUX_PORT"
export INFLUX_USER="admin"
export INFLUX_PASSWORD="flink"
export INFLUX_DB="outliers"
export INFLUX_DB_METRICS="metrics"
#Grafana connection
export GRAFANA_PASSWORD="flink"
#Kafka connection
export KAFKA_TOPIC="outliers_input"
export KAFKA_BROKERS="kafka:9092"
#Flink properties
export FLINK_PROPERTIES="FLINK_PROPERTIES=metrics.reporter.influxdb.factory.class: org.apache.flink.metrics.influxdb.InfluxdbReporterFactory
metrics.reporter.influxdb.scheme: http
metrics.reporter.influxdb.host: ${INFLUX_HOST}
metrics.reporter.influxdb.port: ${INFLUX_PORT}
metrics.reporter.influxdb.db: ${INFLUX_DB_METRICS}
metrics.reporter.influxdb.username: ${INFLUX_USER}
metrics.reporter.influxdb.password: ${INFLUX_PASSWORD}"

#Function to build the custom flink image
function build_flink () {
	echo "Checking Flink image"
	found_image=$(docker image ls | grep "$FLINK_IMAGE" | grep "$OUTLIER_JOB_VERSION" -c)
	if [ "$found_image" -ne 1 ]; then #Image not found
		echo "Image not found"
		#Build flink image
		echo "Building the Flink image ${FLINK_IMAGE}:${OUTLIER_JOB_VERSION}"
		docker build  --build-arg FLINK_TAG=${FLINK_JOB_VERSION} --build-arg SCALA_TAG=${SCALA_JOB_VERSION} -t ${FLINK_IMAGE}:${OUTLIER_JOB_VERSION} .
	else
		echo "Image found"
	fi
}

if [ "$1" = "start" ]; then
	build_flink
	echo "Starting the stack"
	#Start the stack
	(cd docker ; docker-compose up -d)
	#echo "Starting the UI"
	#Find the container names
	container_manager=$(docker ps -f name=jobmanager --format "{{.ID}}")
	#Start outliers
	docker container exec -d $container_manager python3 ${ARTIFACT_PATH}/server/server.py
	echo "Stack is up!"
	echo "Flink Web UI is available at port 8081"
	echo "Grafana Web UI is available at port 3000"
	echo "UI is ready. It is available at port 8000"
elif [ "$1" = "stop" ]; then
	echo "Stoping the stack"
	#Start the stack
	(cd docker ; docker-compose stop)
	(cd docker ; yes | docker-compose rm)
elif [ "$1" = "build" ]; then
	#Find the container names
	build_flink
else
	echo "Usage: $(basename "$0") (start|stop|build)"
fi
exit 0
