package rk_query

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import utils.Helpers.distance
import utils.Utils.Query
import models.Data_lsky
import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class SopState(var index: mutable.LinkedHashMap[Int, Data_lsky])

class Sop(c_queries: ListBuffer[Query]) extends ProcessWindowFunction[(Int, Data_lsky), (Long, Query), Int, TimeWindow] {

  @transient private var counter: Counter = _
  @transient private var cpu_time: Long = 0L

  val queries: ListBuffer[Query] = c_queries
  val slide: Int = queries.head.S

  val R_distinct_list = queries.map(_.R).distinct.sorted
  val k_distinct_list = queries.map(_.k).distinct.sorted
  val R_max = R_distinct_list.max
  val R_min = R_distinct_list.min
  val k_max = k_distinct_list.max
  val k_min = k_distinct_list.min
  val k_size = k_distinct_list.size
  val R_size = R_distinct_list.size

  lazy val state: ValueState[SopState] = getRuntimeContext
    .getState(new ValueStateDescriptor[SopState]("myState", classOf[SopState]))

  override def process(key: Int, context: Context, elements: Iterable[(Int, Data_lsky)], out: Collector[(Long, Query)]): Unit = {

    //Metrics
    counter.inc()
    val time_init = System.currentTimeMillis()

    val window = context.window

    //Create state
    if (state.value == null) {
      val index = mutable.LinkedHashMap[Int, Data_lsky]()
      val current = SopState(index)
      state.update(current)
    }

    val all_queries = Array.ofDim[Int](R_size, k_size)

    //Insert new elements to state
    elements
      .filter(_._2.arrival >= window.getEnd - slide)
      //Sort is needed when each point has a different timestamp
      //In our case every point in the same slide has the same timestamp
      .toList
      .sortBy(_._2.arrival)
      .foreach(p => state.value().index += ((p._2.id, p._2)))

    //Update elements
    elements.foreach(p => {
      if (!p._2.safe_inlier && p._2.flag == 0) {
        checkPoint(p._2, window)
        if (p._2.lSky.getOrElse(1, ListBuffer()).count(_._2 >= p._2.arrival) >= k_max) p._2.safe_inlier = true
        else {
          var i, y: Int = 0
          var count = p._2.lSky.getOrElse(i+1, ListBuffer()).count(_._2 >= window.getStart)
          do{
            if(count >= k_distinct_list(y)){ //inlier for all i
              y += 1
            }else{ //outlier for all y
              for(z <- y until k_size){
                all_queries(i)(z) += 1
              }
              i += 1
              count += p._2.lSky.getOrElse(i+1, ListBuffer()).count(_._2 >= window.getStart)
            }
          }while(i < R_size && y < k_size)
        }
      }
    })

    for (i <- 0 until R_size){
      for (y <- 0 until k_size){
        val tmpQuery = Query(R_distinct_list(i),k_distinct_list(y),queries.head.W,queries.head.S,all_queries(i)(y))
        out.collect(window.getEnd, tmpQuery)
      }
    }

    //Remove old points
    elements
      .filter(p => p._2.arrival < window.getStart + slide)
      .foreach(p => {
        deletePoint(p._2)
      })

    //Metrics
    val time_final = System.currentTimeMillis()
    cpu_time += (time_final - time_init)
  }

  def checkPoint(el: Data_lsky, window: TimeWindow): Unit = {
    if (el.lSky.isEmpty) { //It's a new point!
      insertPoint(el)
    } else { //It's an old point
      updatePoint(el, window)
    }
  }

  def insertPoint(el: Data_lsky): Unit = {
    state.value().index.values.toList.reverse //get the points so far from latest to earliest
      .takeWhile(p => {
      var res = true //variable to stop skyband loop
      if (p.id != el.id) {
        val thisDistance = distance(el.value.toArray, p.value.toArray)
        if (thisDistance <= R_max) {
          val skyRes = neighborSkyband(el, p, thisDistance)
          if (!skyRes && thisDistance <= R_min) {
            res = false
          }
        }
      }
      res
    })
  }

  def updatePoint(el: Data_lsky, window: TimeWindow): Unit = {
    //Remove old points from lSky
    el.lSky.keySet.foreach(p => el.lSky.update(p, el.lSky(p).filter(_._2 >= window.getStart)))
    //Create input
    val old_sky = el.lSky.values.flatten.toList.sortWith((p1, p2) => p1._2 > p2._2).map(_._1)
    el.lSky.clear()

    var res = true //variable to stop skyband loop
    state.value().index.values.toList.reverse
      .takeWhile(p => {
        var tmpRes = true
        if (p.arrival >= window.getEnd - slide) {
          val thisDistance = distance(el.value.toArray, p.value.toArray)
          if (thisDistance <= R_max) {
            val skyRes = neighborSkyband(el, p, thisDistance)
            if (!skyRes && thisDistance <= R_min) res = false
          }
        } else tmpRes = false
        res && tmpRes
      })
    if (res)
      old_sky
        .takeWhile(l => { //Time to check the old skyband elements
          val p = state.value().index(l)
          val thisDistance = distance(el.value.toArray, p.value.toArray)
          if (thisDistance <= R_max) {
            val skyRes = neighborSkyband(el, p, thisDistance)
            if (!skyRes && thisDistance < R_min) res = false
          }
          res
        })
  }

  def deletePoint(el: Data_lsky): Unit = {
    state.value().index.remove(el.id)
  }

  def neighborSkyband(el: Data_lsky, neigh: Data_lsky, distance: Double): Boolean = {
    val norm_dist = normalizeDistance(distance)
    var count = 0
    for (i <- 1 to norm_dist) {
      count += el.lSky.getOrElse(i, ListBuffer[Long]()).size
    }
    if (count <= k_max - 1) {
      el.lSky.update(norm_dist, el.lSky.getOrElse(norm_dist, ListBuffer[(Int, Long)]()) += ((neigh.id, neigh.arrival)))
      true
    } else {
      false
    }
  }

  def normalizeDistance(distance: Double): Int = {
    var res, i = 0
    do {
      if (distance <= R_distinct_list(i)) res = i + 1
      i += 1
    } while (i < R_distinct_list.size && res == 0)
    res
  }

  override def open(parameters: Configuration): Unit = {
    counter = getRuntimeContext()
      .getMetricGroup()
      .counter("MySlideCounter")

    getRuntimeContext()
      .getMetricGroup()
      .gauge[Long, ScalaGauge[Long]]("MyTotalTime", ScalaGauge[Long]( () => cpu_time ) )

    getRuntimeContext()
      .getMetricGroup()
      .gauge[Double, ScalaGauge[Double]]("MyAverageTime", ScalaGauge[Double]( () => {
        if(counter.getCount == 0) cpu_time.toDouble
        else cpu_time.toDouble / counter.getCount
      } ) )
  }
}
