ThisBuild / resolvers ++= Seq("Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/", Resolver.mavenLocal)

ThisBuild / version := "3.1.0"
ThisBuild / organization := "org.auth.csd.datalab"
ThisBuild / scalaVersion := "2.11.12"

val flinkVersion = "1.9.0"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-connector-kafka" % flinkVersion,
  "org.apache.bahir" %% "flink-connector-influxdb" % "1.1-SNAPSHOT"
)

lazy val root = (project in file(".")).
  settings(
    name := "PROUD",
    libraryDependencies ++= flinkDependencies,
    libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.2.1"
  )

assembly / mainClass := Some("outlier_detection.Outlier_detection")

// make run command include the provided dependencies
Compile / run  := Defaults.runTask(
  Compile / fullClasspath,
  Compile / run / mainClass,
  Compile / run / runner
).evaluated

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true

// exclude Scala library from assembly
assembly / assemblyOption  := (assembly / assemblyOption).value.copy(includeScala = false)

