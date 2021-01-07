package adapt

import models.{Data_basis, Data_cod, Data_mcod, Data_slicing}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
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
import utils.Helpers.readEnvVariable
import utils.Utils.{PrintOutliers, Query}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object NaiveAdapt {

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
    val adapt_over = parameters.get("adapt_over", "130").toDouble
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
    val myPartition = new Tree_partitioning(range, 10000, partitions, partitioning_file, file_delimiter, distance_type)

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
      .process(new AdaptivePartitioningNaive(window, slide, range, myPartition, adapt_range, adapt_over))

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

    env.execute("Naive-Adaptive-load-balancing-flink")

  }

  //Case class for metadata needed on the broadcast state
  case class MetadataAdaptation(change: List[Int], bufferSlide: Long, transientSlide: Long, stabilizationSlide: Long, history: mutable.Map[Int, mutable.Queue[Double]])

  //Old naive adaptation with changed state (instead of old [String,String])
  class AdaptivePartitioningNaive(c_win: Int, c_slide: Int, c_r: Double, c_myPartitioner: Partitioning with Serializable, c_adapt_range: Double, c_adapt_over: Double) extends BroadcastProcessFunction[Data_basis, String, (Int, Data_basis)] {

    @transient private var counter: Counter = _

    lazy val controlStateDescriptor = new MapStateDescriptor[String, MetadataAdaptation](
      "ControlStream",
      BasicTypeInfo.STRING_TYPE_INFO,
      TypeInformation.of(new TypeHint[MetadataAdaptation]() {})
    )
    //Process broadcast element variables
    val window: Int = c_win
    val slide: Int = c_slide
    val adaptation: Double = c_adapt_range //For the adaptation border range
    //Cost function threshold
    val upperThreshold: Double = c_adapt_over //Every key above X percentage from the expected cost is considered overworked
    //Process element function variables
    val range: Double = c_r
    val partitioner: Partitioning = c_myPartitioner
    var reverse_borders = false

    override def processBroadcastElement(value: String, ctx: BroadcastProcessFunction[Data_basis, String, (Int, Data_basis)]#Context, out: Collector[(Int, Data_basis)]): Unit = {
      val control = ctx.getBroadcastState(controlStateDescriptor)
      //Get metrics
      val (time_slide, metrics) = extractMetrics(value)
      if (control.contains("change")) { //If state is changing
        val splitted = value.split(";")
        val cur_time = splitted(0).toLong
        val sleep_time = control.get("change").stabilizationSlide
        if (cur_time >= sleep_time) { //Free the state
          control.remove("change")
        }
      } else {
        val totalCost = metrics.values.sum
        val expectedCost = totalCost / metrics.keys.size
        //Get the overworked node
        val overworked = metrics.maxBy(_._2)
        if(overworked._2 * 100 / expectedCost >= upperThreshold) {
          val buffer = time_slide + window
          val transient = buffer + window
          val stabilization = transient + window
          //Change borders
          partitioner.adapt_partition(List(overworked._1), List(), adaptation * range)
          reverse_borders = true
          val res = MetadataAdaptation(List(overworked._1), buffer, transient, stabilization, null)
          control.put("change", res)
          //Metrics
          counter.inc()
          val adapt_msg = s"ADAPT;$time_slide;${overworked._1}"
          println(adapt_msg)
        }
      }
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
            if (neigh.nonEmpty && neigh.size >= oldNeigh.size) neigh.foreach(part => out.collect(part, new Data_basis(value.id, value.value, value.arrival, 1)))
            else if (oldNeigh.nonEmpty && oldNeigh.size >= neigh.size) oldNeigh.foreach(part => out.collect(part, new Data_basis(value.id, value.value, value.arrival, 1)))
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

    override def open(parameters: Configuration): Unit = {
      counter = getRuntimeContext
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
