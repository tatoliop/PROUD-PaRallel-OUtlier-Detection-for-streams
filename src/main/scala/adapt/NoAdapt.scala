package adapt

import models.{Data_basis, Data_cod, Data_mcod, Data_slicing}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.util.Collector
import outlier_detection.Outlier_detection.file_delimiter
import partitioning.Grid_adapt.Grid_partitioning
import partitioning.Partitioning
import partitioning.Tree_adapt.Tree_partitioning
import utils.Helpers
import utils.Helpers.readEnvVariable
import utils.Utils.{PrintOutliers, Query}

import scala.collection.mutable.ListBuffer

object NoAdapt {

  def main(args: Array[String]) {

    val parameters: ParameterTool = ParameterTool.fromArgs(args)
    //Parameters
    val window = parameters.getRequired("W").toInt
    val slide = parameters.getRequired("S").toInt
    val range = parameters.getRequired("R").toDouble
    val k = parameters.getRequired("k").toInt
    val distance_type = parameters.get("distance", "euclidean")
    val algorithm = parameters.get("algorithm", "pmcod")
    //Partitioning Parameters
    val partitioning = parameters.getRequired("partitioning") //replication, grid, tree
    //Partitions parameter will depend on the technique
    //For grid technique this parameter should have the number of cuts for each dimension (e.g. 2;2;2;3 for 4-dimensions)
    //For tree technique this parameter should indicate the height of the tree that the split will stop (e.g. height equals to 4 means 2 ^ 4 = 16 partitions)
    val partitions = parameters.getRequired("partitions").split(";").toList.map(_.toInt)
    //Input file
    val file_input = readEnvVariable("JOB_INPUT") //File input
    val dataset = parameters.getRequired("dataset") //STK
    val myInput = s"$file_input/$dataset/input.txt"
    val partitioning_file = s"$file_input/$dataset/tree_input.txt"
    val myQueries = new ListBuffer[Query]()
    myQueries += Query(range, k, window, slide, 0)

    //Create the partitioner for the specified type
    val myPartition = if (partitioning == "tree") new Tree_partitioning(range,10000, partitions, partitioning_file, file_delimiter, distance_type)
    else new Grid_partitioning(10000, partitions, partitioning_file, file_delimiter)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val data = env.readTextFile(myInput).setParallelism(1)
      .map { value =>
        val splitLine = value.split("&")
        val id = splitLine(0).toInt
        val valuePoint = splitLine(1).split(",").map(_.toDouble).to[ListBuffer]
        val new_time: Long = id
        val myTruePoint = new Data_basis(id, valuePoint, new_time, 0)
        //Need to slow down stream when reading from file
        Thread.sleep(1)
        myTruePoint
      }

    //Connect the input stream with the control stream
    val connection = data
      .flatMap(row => {
        val (belongs, neigh) = myPartition.get_partition(row, range)
        val myList = ListBuffer[(Int,Data_basis)]()
        myList.append((belongs, row))
        if (neigh.nonEmpty) neigh.foreach(part => myList.append((part, new Data_basis(row.id, row.value, row.arrival, 1))))
        myList
      })

    //Assign timestamps to the stream
    val timestampedData =
      connection
        .assignAscendingTimestamps(r => r._2.arrival)

    //Output outliers
    val main_output = algorithm match {
      case "cod" =>
        timestampedData
          .map(record => (record._1, new Data_cod(record._2)))
          .keyBy(_._1)
          .timeWindow(Time.milliseconds(window), Time.milliseconds(slide))
          .process(new Adapt_Cod(myQueries.head, c_distance_type = distance_type))
      case "slicing" =>
        timestampedData
          .map(record => (record._1, new Data_slicing(record._2)))
          .keyBy(_._1)
          .timeWindow(Time.milliseconds(window), Time.milliseconds(slide))
          .process(new Adapt_Slicing(myQueries.head, c_distance_type = distance_type))
      case "pmcod" =>
        timestampedData
          .map(record => (record._1, new Data_mcod(record._2)))
          .keyBy(_._1)
          .timeWindow(Time.milliseconds(window), Time.milliseconds(slide))
          .process(new Adapt_Pmcod(myQueries.head, c_distance_type = distance_type))
    }

    val groupedOutliers = main_output
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.milliseconds(slide)))
      .process(new PrintOutliers)

    groupedOutliers.print

    env.execute("Not Adaptive-load-balancing-flink")

  }

  class GroupSideOutput extends ProcessWindowFunction[(Long, String), String, Long, TimeWindow] {
    override def process(key: Long, context: Context, elements: scala.Iterable[(Long, String)], out: Collector[String]): Unit = {
      var result = ""
      elements.foreach(r => result += (r._2 + ";"))
      result = result.substring(0, result.length - 1)
      out.collect(s"$key;$result")
    }
  }

}
