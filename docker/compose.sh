#!/bin/bash

#Source variable folder
source variables.sh

#Function to build the custom flink image
function build_flink () {
	echo "Checking Flink image"
	found_image=$(docker image ls | grep "$FLINK_IMAGE" | grep "$OUTLIER_JOB_VERSION" -c)
	if [ "$found_image" -ne 1 ]; then #Image not found
		echo "Image not found"
		#Build flink image
		echo "Building the fat jar"
		(cd ../ ; sbt clean assembly)
		cp -r ../data/ outliers_flink/assets
		cp ../target/scala-${SCALA_JOB_VERSION}/${OUTLIER_JOB_NAME} outliers_flink/assets/${OUTLIER_JOB_NAME}
		echo "Building the Flink image ${FLINK_IMAGE}:${OUTLIER_JOB_VERSION}"
		sed "s/%%FLINK_VERSION%%/$FLINK_VERSION/" Dockerfile.template > outliers_flink/Dockerfile
		sed -i "s/%%JOB_NAME%%/$OUTLIER_JOB_NAME/" outliers_flink/Dockerfile
		(cd outliers_flink ; docker build -t ${FLINK_IMAGE}:${OUTLIER_JOB_VERSION} .)
	else
		echo "Image found"
	fi
}

if [ "$1" = "start" ]; then
	build_flink
	echo "Starting the stack"
	#Start the stack
	docker-compose up -d
	echo "Stack is up!"
	echo "Flink Web UI is available at port 8081"
	echo "Grafana Web UI is available at port 3000"
elif [ "$1" = "stop" ]; then
	echo "Stoping the stack"
	#Start the stack
	docker-compose stop
	yes | docker-compose rm
elif [ "$1" = "build" ]; then
	#Find the container names
	build_flink
elif [ "$1" = "ui" ]; then
	echo "Starting the UI"
	#Find the container names
	container_manager=$(docker ps -f name=jobmanager --format "{{.ID}}")
	#Start outliers
	docker container exec -it $container_manager python3 ${ARTIFACT_PATH}/server/server.py
	echo "UI is ready. It is available at port 8000"
elif [ "$1" = "debug" ]; then
 	echo "Starting Flink jobs"
	#Find the container names
	container_manager=$(docker ps -f name=jobmanager --format "{{.ID}}")
	#Copy job jar for quick DEBUG
	cp ../target/scala-2.11/$OUTLIER_JOB_NAME outliers.jar
	docker cp outliers.jar $container_manager:${ARTIFACT_PATH}/outliers.jar
	#Start outliers
#	docker container exec -it $container_manager bin/flink run --parallelism 16 -d ${ARTIFACT_PATH}/outliers.jar --policy static --algorithm pmcod --W 10000 --S 500 --k 50 --R 0.45 --dataset STK -partitioning tree --partitions 3
	docker container exec -it $container_manager bin/flink run --parallelism 16 -d ${ARTIFACT_PATH}/outliers.jar --policy advanced --algorithm pmcod --W 10000 --S 500 --k 50 --R 0.45 --dataset STK -partitioning tree --partitions 3
	#Start custom source
	docker container exec -it $container_manager bin/flink run -d -c custom_source.Custom_source ${ARTIFACT_PATH}/outliers.jar --dataset STK
else
	echo "Usage: $(basename "$0") (start|stop|build|ui|debug)"
fi
exit 0
