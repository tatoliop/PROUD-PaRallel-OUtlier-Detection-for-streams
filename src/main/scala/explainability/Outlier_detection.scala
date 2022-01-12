package explainability

import models.Data_basis
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.influxdb.{InfluxDBConfig, InfluxDBSink}
import partitioning.Grid_test.Grid_test_partitioning
import partitioning.Tree_test.Tree_test_partitioning
import utils.Helpers.{find_gcd, getCombinations, getSampleData, readEnvVariable}
import utils.Utils.{Query, WriteOutliersEx}
import utils.traits.Partitioning

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Outlier_detection {

  val delimiter = ";"
  val file_delimiter = ","
  val line_delimiter = "&"


  def main(args: Array[String]) {

    val parameters: ParameterTool = ParameterTool.fromArgs(args)
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
    //Dimensionality subspaces
    val dimension_split = parameters.get("dimensionality", "false").toBoolean
    val dimension_size = parameters.get("dimensionality_size", "2").toInt
    //Explainability input type
    val explain_input = parameters.get("explain_input", "3").toInt
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

    //Pre-process parameters for initializations
    val common_W = if (window_W.length > 1) window_W.max else window_W.head
    val common_S = if (window_S.length > 1) find_gcd(window_S) else window_S.head
    val common_R = if (app_R.length > 1) app_R.max else app_R.head

    //Create query/queries
    val myQueries = new ListBuffer[Query]()
    for (w <- window_W)
      for (s <- window_S)
        for (r <- app_R)
          for (k <- app_k)
            myQueries += Query(r, k, w, s, 0)

    //Create sample input
    val (sample, dimensions) = getSampleData(sample_size, partitioning_file, file_delimiter)
    //Create dimension space
    val subspaces = if (dimensions > 2 && dimension_split && (dimension_size == 2 || dimension_size == 3))
      getCombinations(dimensions, dimension_size)
    else List(List[Int](0))
    val myPartitions: mutable.HashMap[List[Int], Partitioning] = mutable.HashMap()
    if (subspaces.size == 1) {
      //Create the partitioner for the specified type
      val myPartition = if (partitioning == "tree" || partition_policy != "static") new Tree_test_partitioning(common_R, partitions, sample, distance_type)
      else new Grid_test_partitioning(partitions, sample)
      myPartitions.put(subspaces.head, myPartition)
    } else {
      subspaces.foreach { p =>
        val tmp = sample.map {
          el => (for(i <- p) yield el(i - 1)).toArray
        }
        //Create the partitioner for the specified type
        val myPartition = if (partitioning == "tree" || partition_policy != "static") new Tree_test_partitioning(common_R, partitions, tmp, distance_type)
        else new Grid_test_partitioning(partitions, tmp)
        myPartitions.put(p, myPartition)
      }
    }
    val detectionParallelism = subspaces.size * myPartitions.maxBy(_._2.get_list_partitions().size)._2.get_list_partitions().size

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //Start reading from source
    val data: DataStream[(List[Int], Data_basis)] = {
      env
        .readTextFile(myInput)
        .setParallelism(1)
        .startNewChain()
        .flatMap { record =>
          val splitLine = record.split(line_delimiter)
          val id = splitLine(0).toInt
          val value = splitLine(1).split(file_delimiter).map(_.toDouble)
          val new_time = id.toLong
          Thread.sleep(1)
          if (subspaces.size == 1) {
            List((subspaces.head, new Data_basis(id, value.to[ListBuffer], new_time, 0)))
          }
          else {
            val resultList = ListBuffer[(List[Int], Data_basis)]()
            subspaces.foreach { subspace =>
              val tmpValue = for(i <- subspace) yield value(i - 1)
              resultList += ((subspace, new Data_basis(id, tmpValue.to[ListBuffer], new_time, 0)))
            }
            resultList
          }
        }
    }.setParallelism(1)

    val connection =
      data
        .flatMap(row => {
          val (belongs, neigh) = myPartitions(row._1).get_partition(row._2, common_R)
          val myList = ListBuffer[(List[Int], Int, Data_basis)]()
          myList.append((row._1, belongs, row._2))
          if (neigh.nonEmpty) neigh.foreach(part => myList.append((row._1, part, new Data_basis(row._2.id, row._2.value, row._2.arrival, 1))))
          myList
        })
        .slotSharingGroup("Partitioning")

    //Assign timestamps to the stream
    val timestampedData =
      connection
        .assignAscendingTimestamps(r => r._3.arrival)
        .keyBy(r => (r._1, r._2))
        .window(SlidingEventTimeWindows.of(Time.milliseconds(common_W), Time.milliseconds(common_S)))

    //Output outliers
      timestampedData
        .process(new Outlier_detection_process(myQueries, common_S, algorithm, distance_type, explain_input))
        .setMaxParallelism(detectionParallelism)
        .slotSharingGroup("Detection")
        .startNewChain()
        .map(new WriteOutliersEx)
        .slotSharingGroup("Detection")
        .startNewChain()
        .addSink(new InfluxDBSink(influx_conf))
        .slotSharingGroup("Detection")

    env.execute("Distance-based outlier detection job with explanations")
  }

}
