package outlier_detection

import java.util.Properties

import utils.Helpers.{find_gcd, readEnvVariable}
import models._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.influxdb.InfluxDBConfig
import partitioning.Grid.grid_partitioning
import partitioning.Replication.replication_partitioning
import partitioning.Tree.Tree_partitioning
import utils.Utils.{Evict_before, GroupMetadataAdvanced, GroupMetadataNaive, PrintOutliers, Query, WriteOutliers}
import org.apache.flink.streaming.connectors.influxdb._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema

import scala.collection.mutable.ListBuffer

object Outlier_detection {

  val delimiter = ";"
  val file_delimiter = ","
  val line_delimiter = "&"

  def main(args: Array[String]) {

    val parameters: ParameterTool = ParameterTool.fromArgs(args)
    val DEBUG = parameters.get("DEBUG", "false").toBoolean
    //Won't work without these two parameter
    val parameter_space = parameters.getRequired("space") //single, rk, rkws
    val algorithm = parameters.getRequired("algorithm") //naive, advanced, advanced_extended, slicing, pmcod, pmcod_net, amcod, sop, psod, pmcsky
    //Windowing Parameters
    val window_W = parameters.getRequired("W").split(delimiter).toList.map(_.toInt)
    val window_S = parameters.getRequired("S").split(delimiter).to[ListBuffer].map(_.toInt)
    //Application Parameters
    val app_k = parameters.getRequired("k").split(delimiter).toList.map(_.toInt)
    val app_R = parameters.getRequired("R").split(delimiter).toList.map(_.toDouble)
    //Input Parameters
    val dataset = parameters.getRequired("dataset") //STK, TAO
    //Partitioning Parameters
    val partitioning = parameters.getRequired("partitioning") //replication, grid, tree
    val tree_init = parameters.get("tree_init", "10000").toInt
    //Hardcoded parameter for partitioning and windowing
    val partitions = 16
    //Input file
    val file_input = readEnvVariable("JOB_INPUT") //File input

    //Pre-process parameters for initializations
    val common_W = if (window_W.length > 1) window_W.max else window_W.head
    val common_S = if (window_S.length > 1) find_gcd(window_S) else window_S.head
    val common_R = if (app_R.length > 1) app_R.max else app_R.head
    //Variable to allow late data points within the specific time be ingested by the job
    val allowed_lateness = common_S

    //Create query/queries
    val myQueries = new ListBuffer[Query]()
    for (w <- window_W)
      for (s <- window_S)
        for (r <- app_R)
          for (k <- app_k)
            myQueries += Query(r, k, w, s, 0)


    //Create the tree for the specified partitioning type
    val myTree = if (partitioning == "tree") {
      val tree_input = s"$file_input/$dataset/tree_input.txt"
      new Tree_partitioning(tree_init, partitions, tree_input)
    }
    else null

    //InfluxDB
    val influxdb_host = readEnvVariable("INFLUXDB_HOST")
    val influxdb_user = readEnvVariable("INFLUXDB_USER")
    val influxdb_pass = readEnvVariable("INFLUXDB_PASSWORD")
    val influxdb_db = readEnvVariable("INFLUXDB_DB")
    val influx_conf: InfluxDBConfig =
      InfluxDBConfig.builder(influxdb_host, influxdb_user, influxdb_pass, influxdb_db).createDatabase(true)
        .build()
    //Kafka parameters
    val kafka_brokers = readEnvVariable("KAFKA_BROKERS")
    val kafka_topic = readEnvVariable("KAFKA_TOPIC")
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", kafka_brokers)

    //Start context
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(partitions)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //Start reading from source
    //Either kafka or input file for debug
    val data: DataStream[Data_basis] = if (DEBUG) {
      val myInput = s"$file_input/$dataset/input.txt"
      env
        .readTextFile(myInput)
        .map { record =>
          val splitLine = record.split(line_delimiter)
          val id = splitLine(0).toInt
          val value = splitLine(1).split(file_delimiter).map(_.toDouble).to[ListBuffer]
          val timestamp = id.toLong
          new Data_basis(id, value, timestamp, 0)
        }
    } else {
      env.addSource(new FlinkKafkaConsumer[ObjectNode](kafka_topic, new JSONKeyValueDeserializationSchema(false), properties))
        .map{record =>
          val id = record.get("value").get("id").asInt
          val timestamp = record.get("value").get("timestamp").asLong
          val value_iter = record.get("value").get("value").elements
          val value = ListBuffer[Double]()
          while(value_iter.hasNext) {
            value += value_iter.next().asDouble()
          }
          new Data_basis(id, value, timestamp, 0)
        }
    }

    //Start partitioning
    val partitioned_data = partitioning match {
      case "replication" =>
        data
          .flatMap(record => replication_partitioning(partitions, record))
      case "grid" =>
        data
          .flatMap(record => grid_partitioning(partitions, record, common_R, dataset))
      case "tree" =>
        data
          .flatMap(record => myTree.tree_partitioning(partitions, record, common_R))
    }

    //Timestamp the data
    val timestamped_data = partitioned_data
      .assignAscendingTimestamps(_._2.arrival)

    //Start algorithm
    //Start by mapping the basic data point to the algorithm's respective class
    //Key the data by partition and window them
    //Call the algorithm process
    val output_data = parameter_space match {
      case "single" =>
        algorithm match {
          case "naive" =>
            val firstWindow: DataStream[Data_naive] = timestamped_data
              .map(record => (record._1, new Data_naive(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .evictor(new Evict_before(common_S))
              .process(new single_query.Naive(myQueries.head))
            firstWindow
              .keyBy(_.id % partitions)
              .timeWindow(Time.milliseconds(common_S))
              .process(new GroupMetadataNaive(myQueries.head))
          case "advanced" =>
            val firstWindow: DataStream[Data_advanced] = timestamped_data
              .map(record => (record._1, new Data_advanced(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .evictor(new Evict_before(common_S))
              .process(new single_query.Advanced(myQueries.head))
            firstWindow
              .keyBy(_.id % partitions)
              .timeWindow(Time.milliseconds(common_S))
              .process(new GroupMetadataAdvanced(myQueries.head))
          case "advanced_extended" =>
            timestamped_data
              .map(record => (record._1, new Data_advanced(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .process(new single_query.Advanced_extended(myQueries.head))
          case "slicing" =>
            timestamped_data
              .map(record => (record._1, new Data_slicing(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .process(new single_query.Slicing(myQueries.head))
          case "pmcod" =>
            timestamped_data
              .map(record => (record._1, new Data_mcod(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .process(new single_query.Pmcod(myQueries.head))
          case "pmcod_net" =>
            timestamped_data
              .map(record => (record._1, new Data_mcod(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .process(new single_query.Pmcod_net(myQueries.head))
        }
      case "rk" =>
        algorithm match {
          case "amcod" =>
            timestamped_data
              .map(record => (record._1, new Data_amcod(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .process(new rk_query.Amcod(myQueries))
          case "sop" =>
            timestamped_data
              .map(record => (record._1, new Data_lsky(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .process(new rk_query.Sop(myQueries))
          case "psod" =>
            timestamped_data
              .map(record => (record._1, new Data_lsky(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .process(new rk_query.Psod(myQueries))
          case "pmcsky" =>
            timestamped_data
              .map(record => (record._1, new Data_mcsky(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .process(new rk_query.PmcSky(myQueries))
        }
      case "rkws" =>
        algorithm match {
          case "sop" =>
            timestamped_data
              .map(record => (record._1, new Data_lsky(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .process(new rkws_query.Sop(myQueries, common_S))
          case "psod" =>
            timestamped_data
              .map(record => (record._1, new Data_lsky(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .process(new rkws_query.Psod(myQueries, common_S))
          case "pmcsky" =>
            timestamped_data
              .map(record => (record._1, new Data_mcsky(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .process(new rkws_query.PmcSky(myQueries, common_S))
        }
    }

    //Start writing output
    if(DEBUG){
    output_data
      .keyBy(_._1)
      .timeWindow(Time.milliseconds(common_S))
      .process(new PrintOutliers)
      .print
    } else {
      output_data
        .keyBy(_._1)
        .timeWindow(Time.milliseconds(common_S))
        .process(new WriteOutliers)
        .addSink(new InfluxDBSink(influx_conf))
    }
    env.execute("Distance-based outlier detection with Flink")
  }

}
