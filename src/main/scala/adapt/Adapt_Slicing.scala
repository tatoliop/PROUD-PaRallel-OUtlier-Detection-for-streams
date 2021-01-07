package adapt

import utils.Utils.Query
import mtree.{utils, _}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import models.Data_slicing
import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.streaming.api.scala.OutputTag

import scala.collection.mutable

case class SlicingState(var trees: mutable.HashMap[Long, MTree[Data_slicing]], var triggers: mutable.HashMap[Long, mutable.Set[Int]])

class Adapt_Slicing(c_query: Query, c_output: OutputTag[(Long, String)] = null, c_cost: Int = 0, c_distance_type: String = "euclidean") extends ProcessWindowFunction[(Int, Data_slicing), (Long, Query), Int, TimeWindow] {

  @transient private var counter_replicas: Counter = _
  @transient private var counter_non_replicas: Counter = _
  @transient private var counter: Counter = _
  @transient private var cpu_time: Long = 0L

  val query: Query = c_query
  val slide: Int = query.S
  val R: Double = query.R
  val k: Int = query.k
  val outliers_trigger: Long = -1L
  val outputTag: OutputTag[(Long, String)] = c_output
  val costFunction: Int = c_cost
  val distance_type: String = c_distance_type

  lazy val state: ValueState[SlicingState] = getRuntimeContext
    .getState(new ValueStateDescriptor[SlicingState]("myTree", classOf[SlicingState]))

  override def process(key: Int, context: Context, elements: scala.Iterable[(Int, Data_slicing)], out: Collector[(Long, Query)]): Unit = {

    val window = context.window

    //Metrics
    counter.inc()
    counter_replicas.inc(elements.count(el => el._2.flag == 1 && el._2.arrival >= window.getEnd - slide))
    counter_non_replicas.inc(elements.count(el => el._2.flag == 0 && el._2.arrival >= window.getEnd - slide))
    val time_init = System.currentTimeMillis()

    //new variables
    val latest_slide = window.getEnd - slide
    val nonRandomPromotion = new PromotionFunction[Data_slicing] {
      /**
        * Chooses (promotes) a pair of objects according to some criteria that is
        * suitable for the application using the M-Tree.
        *
        * @param dataSet          The set of objects to choose a pair from.
        * @param distanceFunction A function that can be used for choosing the
        *                         promoted objects.
        * @return A pair of chosen objects.
        */
      override def process(dataSet: java.util.Set[Data_slicing], distanceFunction: DistanceFunction[_ >: Data_slicing]): utils.Pair[Data_slicing] = {
        utils.Utils.minMax[Data_slicing](dataSet)
      }
    }
    val mySplit = new ComposedSplitFunction[Data_slicing](nonRandomPromotion, new PartitionFunctions.BalancedPartition[Data_slicing])
    val myTree = if (distance_type == "euclidean") new MTree[Data_slicing](k, DistanceFunctions.EUCLIDEAN, mySplit)
    else new MTree[Data_slicing](k, DistanceFunctions.JACCARD, mySplit)
    //populate mtree
    if (state.value() == null) {
      var myTrigger = mutable.HashMap[Long, mutable.Set[Int]]()
      myTrigger.+=((outliers_trigger, mutable.Set()))
      var next_slide = window.getStart
      while (next_slide <= window.getEnd - slide) {
        myTrigger.+=((next_slide, mutable.Set()))
        next_slide += slide
      }
      for (el <- elements) {
        myTree.add(el._2)
      }
      val myTrees = mutable.HashMap[Long, MTree[Data_slicing]]((latest_slide, myTree))
      state.update(SlicingState(myTrees, myTrigger))
    } else {
      elements
        .filter(el => el._2.arrival >= window.getEnd - slide)
        .foreach(el => {
          myTree.add(el._2)
        })
      var max = state.value().triggers.keySet.max + slide
      while (max <= window.getEnd - slide) {
        state.value().triggers.+=((max, mutable.Set[Int]()))
        max += slide
      }
      state.value().trees.+=((latest_slide, myTree))
    }

    //Trigger leftover slides
    val slow_triggers = state.value().triggers.keySet.filter(p => p < window.getStart && p != -1L).toList
    for (slow <- slow_triggers) {
      val slow_triggers_points = state.value().triggers(slow).toList
      elements
        .filter(p => slow_triggers_points.contains(p._2.id))
        .foreach(p => trigger_point(p._2, window))
      state.value().triggers.remove(slow)
    }

    //Insert new points
    elements
      .filter(p => p._2.arrival >= window.getEnd - slide && p._2.flag == 0)
      .foreach(p => {
        insert_point(p._2, window)
      })

    //Trigger previous outliers
    val triggered_outliers = state.value().triggers(outliers_trigger).toList
    state.value().triggers(outliers_trigger).clear()
    elements
      .filter(p => triggered_outliers.contains(p._2.id))
      .foreach(p => trigger_point(p._2, window))

    //Report outliers
    val outliers = elements.count(p => {
      p._2.flag == 0 &&
        !p._2.safe_inlier &&
        p._2.count_after + p._2.slices_before.filter(_._1 >= window.getStart).values.sum < k
    })

    val tmpQuery = Query(query.R, query.k, query.W, query.S, outliers)
    out.collect((window.getEnd, tmpQuery))

    //Trigger expiring list
    state.value().trees.remove(window.getStart)
    val triggered: List[Int] = state.value().triggers(window.getStart).toList
    state.value().triggers.remove(window.getStart)
    elements
      .filter(p => triggered.contains(p._2.id))
      .foreach(p => trigger_point(p._2, window))

    //Metrics
    val time_final = System.currentTimeMillis()
    cpu_time += (time_final - time_init)

    //Cost functions for adaptation
    if (outputTag != null) {
      val cost = costFunction match {
        case 1 => //Just the non-replicas
          elements.count(_._2.flag == 0)
        case 2 => //Process time
          time_final - time_init
        case 3 => //Non-replicas + process time
          (time_final - time_init) * elements.size
      }
      context.output(outputTag, (context.window.getEnd, s"$key;$cost"))
    }

  }

  def trigger_point(point: Data_slicing, window: TimeWindow): Unit = {
    var next_slide = //find starting slide
      if (point.last_check != 0L) point.last_check + slide
      else get_slide(point.arrival, window) + slide
    //Find no of neighbors
    var neigh_counter = point.count_after +
      point.slices_before.filter(_._1 >= window.getStart + slide).values.sum
    while (neigh_counter < k && next_slide <= window.getEnd - slide) {
      val myTree = state.value().trees.getOrElse(next_slide, null)
      if (myTree != null) {
        val query: MTree[Data_slicing]#Query = myTree.getNearestByRange(point, R)
        val iter = query.iterator()
        //Update point's metadata
        while (iter.hasNext) {
          iter.next()
          point.count_after += 1
          neigh_counter += 1
        }
        if (point.count_after >= k) point.safe_inlier = true
      }
      point.last_check = next_slide
      next_slide += slide
    }
    if (neigh_counter < k) state.value().triggers(outliers_trigger).+=(point.id)
  }

  def insert_point(point: Data_slicing, window: TimeWindow): Unit = {
    var (neigh_counter, next_slide) = (0, window.getEnd - slide)
    while (neigh_counter < k && next_slide >= window.getStart) { //Query each slide's MTREE
      val myTree = state.value().trees.getOrElse(next_slide, null)
      if (myTree != null) {
        val query: MTree[Data_slicing]#Query = myTree.getNearestByRange(point, R)
        val iter = query.iterator()
        //If it has neighbors insert it into the slide's trigger
        if (iter.hasNext)
          state.value().triggers(next_slide).+=(point.id)
        //Update point's metadata
        while (iter.hasNext) {
          val node = iter.next().data
          if (next_slide == window.getEnd - slide) {
            if (node.id != point.id) {
              point.count_after += 1
              neigh_counter += 1
            }
          } else {
            point.slices_before.update(next_slide, point.slices_before.getOrElse(next_slide, 0) + 1)
            neigh_counter += 1
          }
        }
        if (next_slide == window.getEnd - slide && neigh_counter >= k) point.safe_inlier = true
      }
      next_slide -= slide
    }
    //If it is an outlier insert into trigger list
    if (neigh_counter < k) state.value().triggers(outliers_trigger).+=(point.id)
  }

  def get_slide(arrival: Long, window: TimeWindow): Long = {
    val first = arrival - window.getStart
    val div = first / slide
    val int_div = div.toInt
    window.getStart + (int_div * slide)
  }

  override def open(parameters: Configuration): Unit = {
    counter = getRuntimeContext()
      .getMetricGroup()
      .counter("MySlideCounter")

    counter_replicas = getRuntimeContext
      .getMetricGroup
      .counter("MyReplicasCounter")

    counter_non_replicas = getRuntimeContext
      .getMetricGroup
      .counter("MyNonReplicasCounter")

    getRuntimeContext()
      .getMetricGroup()
      .gauge[Long, ScalaGauge[Long]]("MyTotalTime", ScalaGauge[Long](() => cpu_time))

    getRuntimeContext()
      .getMetricGroup()
      .gauge[Double, ScalaGauge[Double]]("MyAverageTime", ScalaGauge[Double](() => {
        if (counter.getCount == 0) cpu_time.toDouble
        else cpu_time.toDouble / counter.getCount
      }))
  }
}

