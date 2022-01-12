ThisBuild / resolvers ++= Seq("Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/", Resolver.mavenLocal)

ThisBuild / version := "3.3.0"
ThisBuild / organization := "org.auth.csd.datalab"
ThisBuild / scalaVersion := "2.11.12"

val flinkVersion = "1.13.1"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-clients" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-connector-kafka" % flinkVersion,
  "org.apache.bahir" %% "flink-connector-influxdb" % "1.1-SNAPSHOT",
  "org.apache.bahir" %% "flink-connector-redis" % "1.1-SNAPSHOT"
)

lazy val root = (project in file(".")).
  settings(
    name := "PROUD",
    libraryDependencies ++= flinkDependencies,
    libraryDependencies += "net.debasishg" %% "redisclient" % "3.9",
    libraryDependencies += "nz.ac.waikato.cms.weka" % "weka-stable" % "3.8.5"
  )

assembly / mainClass := Some("main_job.Outlier_detection")

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

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}