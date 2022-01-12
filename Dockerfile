ARG OPENJDK_TAG=11
ARG FLINK_TAG=1.13.1
ARG SCALA_TAG=2.11

FROM openjdk:${OPENJDK_TAG} AS builder
ARG SBT_VERSION=1.3.13
# Install sbt
RUN \
mkdir /working/ && \
cd /working/ && \
curl -L -o sbt-$SBT_VERSION.deb https://repo.scala-sbt.org/scalasbt/debian/sbt-$SBT_VERSION.deb && \
dpkg -i sbt-$SBT_VERSION.deb && \
rm sbt-$SBT_VERSION.deb && \
apt-get update && \
apt-get install sbt && \
cd && \
rm -r /working/ && \
sbt sbtVersion
RUN mkdir /opt/project
COPY src /opt/src
COPY build.sbt /opt
COPY project/assembly.sbt /opt/project
COPY project/build.properties /opt/project
WORKDIR /opt
RUN sbt clean assembly

FROM flink:${FLINK_TAG}-scala_${SCALA_TAG}-java${OPENJDK_TAG}
#Install python
RUN apt-get update; \
  apt-get install -y python3
# Configure container
ENV ARTIFACTS=/opt/artifacts
ENV JOB_NAME=outlier_job.jar
RUN echo alias flink=$FLINK_HOME/bin/flink >> ~/.bashrc
COPY --from=builder /opt/target/scala-2.11/*.jar $ARTIFACTS/$JOB_NAME
#Copy the input data for the UI-based approach
COPY data/STK $ARTIFACTS/data/STK
COPY data/TAO $ARTIFACTS/data/TAO
COPY form_ui/server $ARTIFACTS/server
ENV JOB_INPUT=$ARTIFACTS/data
EXPOSE 8000
CMD ["help"]
