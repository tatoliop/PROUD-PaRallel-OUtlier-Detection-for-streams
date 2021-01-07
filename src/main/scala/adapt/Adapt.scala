package adapt

import java.util

import models.{Data_basis, Data_cod, Data_mcod, Data_slicing}
import utils.Helpers.readEnvVariable
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.influxdb.{InfluxDBConfig, InfluxDBSink}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.util.Collector
import partitioning.Grid_adapt.Grid_partitioning
import outlier_detection.Outlier_detection.file_delimiter
import partitioning.Partitioning
import partitioning.Tree_adapt.Tree_partitioning
import utils.Utils.{PrintOutliers, Query, WriteOutliers}

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Queue}

object Adapt {

  def main(args: Array[String]) {

    val parameters: ParameterTool = ParameterTool.fromArgs(args)
    //Detection Parameters
    val window = parameters.getRequired("W").toInt
    val slide = parameters.getRequired("S").toInt
    val range = parameters.getRequired("R").toDouble
    val k = parameters.getRequired("k").toInt
    val distance_type = parameters.get("distance", "euclidean")
    val algorithm = parameters.get("algorithm", "pmcod")
    //Adaptation parameters
    val adapt_range = parameters.get("adapt_range", "1").toDouble
    val adapt_queue = parameters.get("adapt_queue", "5").toInt
    val adapt_over = parameters.get("adapt_over", "130").toDouble
    val adapt_under = parameters.get("adapt_under", "30").toDouble
    val adapt_cost = parameters.get("adapt_cost", "1").toInt // (1): only non-replicas, (2): process time (3): non-replicas + process time
    //Partitioning Parameters
    //Partitions parameter will depend on the technique
    //For tree technique this parameter should indicate the height of the tree that the split will stop (e.g. height equals to 4 means 2 ^ 4 = 16 partitions)
    val partitions = parameters.getRequired("partitions").split(";").toList.map(_.toInt)
    //Input file
    val file_input = readEnvVariable("JOB_INPUT") //The datasets' path
    val dataset = parameters.getRequired("dataset") //The dataset's folder
    val myInput = s"$file_input/$dataset/input.txt"
    val partitioning_file = s"$file_input/$dataset/tree_input.txt"
    //Redis stuff
    val redis_host = readEnvVariable("REDIS_HOST")
    val redis_port = readEnvVariable("REDIS_PORT").toInt
    val redis_db = readEnvVariable("REDIS_DB")
    //Read from redis should not be too slow or too fast
    //We need to have every window statistics for the incremental changes
    val redis_sleep = slide / 2
    val redis_cancel = (window / redis_sleep) * 2

    val myQueries = new ListBuffer[Query]()
    myQueries += Query(range, k, window, slide, 0)

    //Create the partitioner for the specified type
    val myPartition = new Tree_partitioning(range,10000, partitions, partitioning_file, file_delimiter, distance_type)

    //Create redis conf
    val redis_conf = new FlinkJedisPoolConfig.Builder().setHost(redis_host).setPort(redis_port).build()

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val outputTag = OutputTag[(Long, String)]("cost-function")

    val controlStateDescriptor = new MapStateDescriptor[String, MetadataAdaptation](
      "ControlStream",
      BasicTypeInfo.STRING_TYPE_INFO,
      TypeInformation.of(new TypeHint[MetadataAdaptation]() {})
    )

    //Broadcasted stream for control of balancing
    val control = env
      //Read from redis every new entry with no duplicates passing through
      .addSource(new MyRedisSource(redis_host, redis_port, redis_db, redis_sleep, redis_cancel))
      .broadcast(controlStateDescriptor)

    val data = env
      .readTextFile(myInput)
      .setParallelism(1)
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
      .connect(control)
      .process(new AdaptivePartitioning(window, slide, range, myPartition, adapt_range, adapt_queue, adapt_over, adapt_under))

    //Assign timestamps to the stream
    val timestampedData = connection
      .assignAscendingTimestamps(r => r._2.arrival)

    //Output outliers
    val main_output = algorithm match {
      case "cod" =>
        timestampedData
          .map(record => (record._1, new Data_cod(record._2)))
          .keyBy(_._1)
          .timeWindow(Time.milliseconds(window), Time.milliseconds(slide))
          .process(new Adapt_Cod(myQueries.head, outputTag, adapt_cost, distance_type))
      case "slicing" =>
        timestampedData
          .map(record => (record._1, new Data_slicing(record._2)))
          .keyBy(_._1)
          .timeWindow(Time.milliseconds(window), Time.milliseconds(slide))
          .process(new Adapt_Slicing(myQueries.head, outputTag, adapt_cost, distance_type))
      case "pmcod" =>
        timestampedData
          .map(record => (record._1, new Data_mcod(record._2)))
          .keyBy(_._1)
          .timeWindow(Time.milliseconds(window), Time.milliseconds(slide))
          .process(new Adapt_Pmcod(myQueries.head, outputTag, adapt_cost, distance_type))
    }

    //Side output with metrics for balancing
    val side_output = main_output
      .getSideOutput(outputTag)
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.milliseconds(slide)))
      .process(new GroupSideOutput)

    val groupedOutliers = main_output
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.milliseconds(slide)))
      .process(new PrintOutliers)

    groupedOutliers.print

    //Write side output to redis to create a cycle
    side_output
      .addSink(new RedisSink[String](redis_conf, new MyRedisSink(redis_db)))

    env.execute("Adaptive-load-balancing-flink")

  }

  //Case class for metadata needed on the broadcast state
  case class MetadataAdaptation(change: List[Int], bufferSlide: Long, transientSlide: Long, stabilizationSlide: Long, history: mutable.Map[Int, mutable.Queue[Double]])

  //New adaptation class
  class AdaptivePartitioning(c_win: Int, c_slide: Int, c_r: Double, c_myPartitioner: Partitioning with Serializable, c_adapt_range: Double, c_adapt_queue: Int, c_adapt_over: Double, c_adapt_under: Double) extends BroadcastProcessFunction[Data_basis, String, (Int, Data_basis)] {

    //Metric for adapts
    @transient private var counter: Counter = _

    lazy val controlStateDescriptor = new MapStateDescriptor[String, MetadataAdaptation](
      "ControlStream",
      BasicTypeInfo.STRING_TYPE_INFO,
      TypeInformation.of(new TypeHint[MetadataAdaptation]() {})
    )
    //Process broadcast element function variables
    val queueSize: Int = c_adapt_queue //Queue size for the metric history. Take into account only the last X slides (queue space) for the cost function
    val window: Int = c_win //For the adaptation periods
    val slide: Int = c_slide //For the adaptation periods
    val adaptation: Double = c_adapt_range //For the adaptation border range
    //Cost function threshold
    val upperThreshold: Double = c_adapt_over //Every key above X percentage from the expected cost is considered overworked
    val lowerThreshold: Double = c_adapt_under //Every key below X percentage from the expected cost is considered under-worked
    val similarityThreshold: Double = 5 // The threshold of similarity when comparing a value with its historical values
    //Process element function variables
    val range: Double = c_r
    val partitioner: Partitioning = c_myPartitioner
    var reverse_borders = false

    override def processBroadcastElement(value: String, ctx: BroadcastProcessFunction[Data_basis, String, (Int, Data_basis)]#Context, out: Collector[(Int, Data_basis)]): Unit = {
      val control = ctx.getBroadcastState(controlStateDescriptor)
      //Get metrics
      val (time_slide, metrics) = extractMetrics(value)

      //-----------------------------------------------
      //println(s"METRIC;$time_slide;${metrics(1)};${metrics(2)};${metrics(3)};${metrics(4)};${metrics(5)};${metrics(6)};${metrics(7)};${metrics(8)}")
      //-----------------------------------------------

      //Update history state
      if (control.contains("history")) {
        //Get all metrics
        val allMetrics = control.get("history").history
        //Update metric history
        metrics.foreach { r =>
          if (allMetrics(r._1).size == queueSize) allMetrics(r._1).dequeue()
          allMetrics(r._1).enqueue(r._2)
        }
        //Check if change is in play
        if (control.contains("change")) { //If state is changing
          val splitted = value.split(";")
          val cur_time = splitted(0).toLong
          val sleep_time = control.get("change").stabilizationSlide
          if (cur_time >= sleep_time) { //Free the state
            control.remove("change")
          }
        } else {
          //Here we check the history of the changes on te processing tasks and decide whether to update the borders or not
          val latestMetrics = allMetrics.map(r => (r._1, r._2.last))
          val totalCost = latestMetrics.values.sum
          val expectedCost = totalCost / latestMetrics.keys.size
          //Get the overworked nodes
          val overworked = latestMetrics.filter( r => r._2 * 100 / expectedCost >= upperThreshold)
          val chronicOverworked = overworked.filter { r =>
            //Get the difference with the current cost after dividing them
            val keyHistory = allMetrics(r._1).filter(e => Math.abs((e * 100 / expectedCost) - (r._2 * 100 / expectedCost)) <= similarityThreshold)
            keyHistory.size == queueSize
          }
          //Get the underworked nodes
          val underworked = latestMetrics.filter( r => r._2 * 100 / expectedCost < lowerThreshold)
          val chronicUnderworked = underworked.filter { r =>
            //Get the difference with the current cost after dividing them
            val keyHistory = allMetrics(r._1).filter(e => Math.abs((e * 100 / expectedCost) - (r._2 * 100 / expectedCost)) <= similarityThreshold)
            keyHistory.size == queueSize
          }
          if(chronicOverworked.nonEmpty || chronicUnderworked.nonEmpty){
            val buffer = time_slide + window
            val transient = buffer + window

            //-----------------------------------------------
            //val stabilization = transient + window
            // -----------------------------------------------

            val stabilization = buffer + window
            //Change borders
            partitioner.adapt_partition(chronicOverworked.keys.toList, chronicUnderworked.keys.toList, adaptation * range)
            reverse_borders = true
            val res = MetadataAdaptation(chronicOverworked.keys.toList ::: chronicUnderworked.keys.toList, buffer, transient, stabilization, null)
            control.put("change", res)
            //Metrics
            counter.inc()
            //val adapt_msg = s"ADAPT;${chronicOverworked.toList};${chronicUnderworked.toList};$buffer"
            val adapt_msg = s"ADAPT;$time_slide;${chronicOverworked.keys};${chronicUnderworked.keys}"
            println(adapt_msg)
          }
        }
      } else { //If state is free it means that it is the first input from redis
        //Give access to history
        val metametrics = metrics.map(r => (r._1, mutable.Queue[Double](r._2)))
        val res = MetadataAdaptation(null, 0, 0, 0, metametrics)
        control.put("history", res)
      }
    }

    private def extractMetrics(value: String): (Long, mutable.Map[Int, Double]) = {
      val keys = partitioner.get_list_partitions()
      val splitted = value.split(";")
      val time_slide = splitted(0).toLong
      val metrics = (for (i: Int <- 1 until splitted.length - 1 by 2) yield (splitted(i).toInt, splitted(i + 1).toDouble)).toMap
      val res: mutable.Map[Int, Double] = mutable.Map[Int, Double]()
      for (i <- keys) {
        if (metrics.contains(i)) res.put(i, metrics(i))
        else res.put(i, 0)
      }
      (time_slide, res)
    }

    override def processElement(value: Data_basis, ctx: BroadcastProcessFunction[Data_basis, String, (Int, Data_basis)]#ReadOnlyContext, out: Collector[(Int, Data_basis)]): Unit = {
      val control = ctx.getBroadcastState(controlStateDescriptor)
      if (control.contains("change")) { //Adapt to changes
        val start_time = control.get("change").bufferSlide
        val end_time = control.get("change").transientSlide
        if (value.arrival < start_time) { //Still an old element, process with the old method
          val (belongs, neigh) = partitioner.get_partition(value, range)
          out.collect(belongs, value)
          if (neigh.nonEmpty) neigh.foreach(part => out.collect(part, new Data_basis(value.id, value.value, value.arrival, 1)))
        } else if (value.arrival < end_time) { //For arrivals inside the change period
          //Check where the point belongs with the new borders
          val (belongs, neigh) = partitioner.get_partition(value, range, true)
          //Check where the point belongs with the old borders
          val (oldBelongs, oldNeigh) = partitioner.get_partition(value, range)
          if (belongs == oldBelongs) { //Send it to the partition it belongs to and to the partitions from the most number of neighbors
            out.collect(belongs, value)
            val neighbors: ListBuffer[Int] = (neigh ++ oldNeigh).distinct
            neighbors.foreach(r => if (r != belongs && r != oldBelongs) out.collect(r, new Data_basis(value.id, value.value, value.arrival, 1)))
            //if (neigh.nonEmpty && neigh.size >= oldNeigh.size) neigh.foreach(part => out.collect(part, new Data_basis(value.id, value.value, value.arrival, 1)))
            //else if (oldNeigh.nonEmpty && oldNeigh.size >= neigh.size) oldNeigh.foreach(part => out.collect(part, new Data_basis(value.id, value.value, value.arrival, 1)))
          } else { //It is between the gap from the change of borders
            //Get neighbors
            val neighbors: ListBuffer[Int] = (neigh ++ oldNeigh).distinct
            out.collect(belongs, new Data_basis(value.id, value.value, value.arrival, 0, end_time))
            out.collect(oldBelongs, new Data_basis(value.id, value.value, value.arrival, 1, end_time))
            neighbors.foreach(r => if (r != belongs && r != oldBelongs) out.collect(r, new Data_basis(value.id, value.value, value.arrival, 1)))
          }
        } else { //After the end time and during sleep
          if (reverse_borders) {
            partitioner.reverse()
            reverse_borders = false
          }
          val (belongs, neigh) = partitioner.get_partition(value, range)
          out.collect(belongs, value)
          if (neigh.nonEmpty) neigh.foreach(part => out.collect(part, new Data_basis(value.id, value.value, value.arrival, 1)))
        }
      } else { //Normal flow
        if (reverse_borders) {
          partitioner.reverse()
          reverse_borders = false
        }
        val (belongs, neigh) = partitioner.get_partition(value, range)
        out.collect(belongs, value)
        if (neigh.nonEmpty) neigh.foreach(part => out.collect(part, new Data_basis(value.id, value.value, value.arrival, 1)))
      }

    }

    override def open(parameters: Configuration): Unit = {
      counter= getRuntimeContext
        .getMetricGroup
        .counter("MyAdaptCounter")
    }
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
