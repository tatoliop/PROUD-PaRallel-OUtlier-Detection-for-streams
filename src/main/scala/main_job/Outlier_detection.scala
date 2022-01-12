package main_job

import java.util.Properties
import models.Data_basis
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.influxdb.{InfluxDBConfig, InfluxDBSink}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import partitioning.Grid.Grid_partitioning
import partitioning.Tree.Tree_partitioning
import utils.Helpers.{find_gcd, readEnvVariable}
import utils.Utils.{GroupCostFunction, PrintOutliersNo, Query, WriteAdaptations, WriteOutliers}
import utils.sinks.MyRedisSink
import utils.sources.MyRedisSource

import scala.collection.mutable.ListBuffer

object Outlier_detection {

  val delimiter = ";"
  val file_delimiter = ","
  val line_delimiter = "&"


  def main(args: Array[String]) {

    val parameters: ParameterTool = ParameterTool.fromArgs(args)
    val DEBUG = parameters.get("DEBUG", "false").toBoolean
    //Won't work without these two parameter
    val algorithm = parameters.getRequired("algorithm") //advanced_extended, slicing, pmcod, pmcod_net, amcod, sop, psod, pmcsky
    //Windowing Parameters
    val window_W = parameters.getRequired("W").split(delimiter).toList.map(_.toInt)
    val window_S = parameters.getRequired("S").split(delimiter).to[ListBuffer].map(_.toInt)
    //Application Parameters
    val app_k = parameters.getRequired("k").split(delimiter).toList.map(_.toInt)
    val app_R = parameters.getRequired("R").split(delimiter).toList.map(_.toDouble)
    //Partitioning Parameters
    val partition_policy = parameters.get("policy", "static") //static, naive, advanced
    val partitioning = parameters.getRequired("partitioning") //grid, tree
    //Partitions parameter will depend on the technique
    //For grid technique this parameter should have the number of cuts for each dimension (e.g. 2;2;2;3 for 4-dimensions)
    //For tree technique this parameter should indicate the height of the tree that the split will stop (e.g. height equals to 4 means 2 ^ 4 = 16 partitions)
    val partitions = parameters.getRequired("partitions").split(delimiter).toList.map(_.toInt)
    val sample_size = parameters.get("sample_size", "10000").toInt
    val distance_type = parameters.get("distance", "euclidean")
    //Adaptation parameters
    val adapt_range = parameters.get("adapt_range", "1").toDouble
    val adapt_queue = parameters.get("adapt_queue", "5").toInt
    val adapt_over = parameters.get("adapt_over", "130").toDouble
    val adapt_under = parameters.get("adapt_under", "30").toDouble
    val adapt_cost = parameters.get("adapt_cost", "1").toInt // (1): only non-replicas, (2): process time (3): non-replicas + process time
    val buffer_period = parameters.get("buffer_period", "1").toDouble // For the duration of the buffer period (multiplied by window size)
    //Input files
    val file_input = readEnvVariable("JOB_INPUT") //File input
    val dataset = parameters.getRequired("dataset") //STK, TAO, GAU2 etc
    val myInput = s"$file_input/$dataset/input.txt"
    val partitioning_file = s"$file_input/$dataset/tree_input.txt"
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
    //Redis stuff
    val redis_host = readEnvVariable("REDIS_HOST")
    val redis_port = readEnvVariable("REDIS_PORT").toInt
    val redis_db = readEnvVariable("REDIS_DB")
    //Pre-process parameters for initializations
    val common_W = if (window_W.length > 1) window_W.max else window_W.head
    val common_S = if (window_S.length > 1) find_gcd(window_S) else window_S.head
    val common_R = if (app_R.length > 1) app_R.max else app_R.head
    //Variable to allow late data points within the specific time be ingested by the job
    val allowed_lateness = common_S
    //Side output for adaptation decisions
    val outputTagAdapt = if (partition_policy != "static") OutputTag[(Long, String, String)]("adaptation-decision")
    else null
    //Side output for cost-function in OD
    val outputTagCost = if (partition_policy != "static") OutputTag[(Long, String)]("cost-function")
    else null
    //Read from redis should not be too slow or too fast
    //We need to have every window statistics for the incremental changes
    val redis_sleep = common_S / 2
    val redis_cancel = (common_W / redis_sleep) * 2
    val controlStateDescriptor = new MapStateDescriptor[String, MetadataAdaptation](
      "ControlStream",
      BasicTypeInfo.STRING_TYPE_INFO,
      TypeInformation.of(new TypeHint[MetadataAdaptation]() {})
    )

    //Create query/queries
    val myQueries = new ListBuffer[Query]()
    for (w <- window_W)
      for (s <- window_S)
        for (r <- app_R)
          for (k <- app_k)
            myQueries += Query(r, k, w, s, 0)

    //Create the partitioner for the specified type
    val myPartition = if (partitioning == "tree" || partition_policy != "static") new Tree_partitioning(common_R, sample_size, partitions, partitioning_file, file_delimiter, distance_type)
    else new Grid_partitioning(sample_size, partitions, partitioning_file, file_delimiter)

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //Broadcasted stream for control of balancing
    val control = if (partition_policy != "static")
      env
        //Read from redis every new entry with no duplicates passing through
        .addSource(new MyRedisSource(redis_host, redis_port, redis_db, redis_sleep, redis_cancel))
        .broadcast(controlStateDescriptor)
    else null

    //Start reading from source
    //Either kafka or input file for debug
    val data: DataStream[Data_basis] = if (DEBUG)
      env
        .readTextFile(myInput)
        .setParallelism(1)
        .map { record =>
          val splitLine = record.split(line_delimiter)
          val id = splitLine(0).toInt
          val value = splitLine(1).split(file_delimiter).map(_.toDouble).to[ListBuffer]
          val new_time = id.toLong
          Thread.sleep(1)
          new Data_basis(id, value, new_time, 0)
        }
    else
      env.addSource(new FlinkKafkaConsumer[ObjectNode](kafka_topic, new JSONKeyValueDeserializationSchema(false), properties))
        .setParallelism(env.getParallelism - 1) //Input tasks need to be less than the set parallelism by 1
        .map { record =>
          val id = record.get("value").get("id").asInt
          val timestamp = record.get("value").get("timestamp").asLong
          val value_iter = record.get("value").get("value").elements
          val value = ListBuffer[Double]()
          while (value_iter.hasNext) {
            value += value_iter.next().asDouble()
          }
          new Data_basis(id, value, timestamp, 0)
        }

    //Connect the input stream with the control stream
    val connection = if (partition_policy == "static")
      data
        .flatMap(row => {
          val (belongs, neigh) = myPartition.get_partition(row, common_R)
          val myList = ListBuffer[(Int, Data_basis)]()
          myList.append((belongs, row))
          if (neigh.nonEmpty) neigh.foreach(part => myList.append((part, new Data_basis(row.id, row.value, row.arrival, 1))))
          myList
        })
    else
      data
        .connect(control)
        .process(new Adaptive_partitioning_process(partition_policy, common_W, common_S, common_R, myPartition, adapt_range, adapt_queue, adapt_over, adapt_under, buffer_period, outputTagAdapt))

    //Assign timestamps to the stream
    val timestampedData =
      connection
        .assignAscendingTimestamps(r => r._2.arrival)
        .keyBy(_._1)
        .window(SlidingEventTimeWindows.of(Time.milliseconds(common_W), Time.milliseconds(common_S)))
        .allowedLateness(Time.milliseconds(allowed_lateness))

    //Output outliers
    val main_output =
      timestampedData
        .process(new Outlier_detection_process(myQueries, common_S, algorithm, distance_type, outputTagCost, adapt_cost))

    //Print or store outliers
    if (DEBUG) {
      main_output
        .keyBy(_._1)
        .window(TumblingEventTimeWindows.of(Time.milliseconds(common_S)))
        .process(new PrintOutliersNo)
        .print
    } else {
      main_output
        .keyBy(_._1)
        .window(TumblingEventTimeWindows.of(Time.milliseconds(common_S)))
        .process(new WriteOutliers)
        .addSink(new InfluxDBSink(influx_conf))
    }
    if (partition_policy != "static") {
      //Side output with metrics for balancing
      val side_output = main_output
        .getSideOutput(outputTagCost)
        .keyBy(_._1)
        .window(TumblingEventTimeWindows.of(Time.milliseconds(common_S)))
        .process(new GroupCostFunction)
      //Create redis conf
      val redis_conf = new FlinkJedisPoolConfig.Builder().setHost(redis_host).setPort(redis_port).build()
      //Write side output to redis to create a cycle
      side_output
        .addSink(new RedisSink[String](redis_conf, new MyRedisSink(redis_db)))
      //Side output with adaptation decisions
      val side_output_adapt = connection
        .getSideOutput(outputTagAdapt)
        .map(new WriteAdaptations)
      side_output_adapt
        .addSink(new InfluxDBSink(influx_conf))
    }

    env.execute("Distance-based outlier detection job")
  }

}
