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

case class PsodState(var index: mutable.LinkedHashMap[Int, Data_lsky])

class Psod(c_queries: ListBuffer[Query]) extends ProcessWindowFunction[(Int, Data_lsky), (Long, Query), Int, TimeWindow] {

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

  lazy val state: ValueState[PsodState] = getRuntimeContext
    .getState(new ValueStateDescriptor[PsodState]("myState", classOf[PsodState]))

  override def process(key: Int, context: Context, elements: Iterable[(Int, Data_lsky)], out: Collector[(Long, Query)]): Unit = {

    //Metrics
    counter.inc()
    val time_init = System.currentTimeMillis()

    val window = context.window

    //Create state
    if (state.value == null) {
      val index = mutable.LinkedHashMap[Int, Data_lsky]()
      val current = PsodState(index)
      state.update(current)
    }

    val all_queries = Array.ofDim[Int](R_size, k_size)

    //Remove old points from each lSky
    state.value().index.values.foreach(p => {
      p.lSky.keySet.foreach(l => p.lSky.update(l, p.lSky(l).filter(_._2 >= window.getStart)))
    })
    //Insert new elements to state
    elements
      .filter(_._2.arrival >= window.getEnd - slide)
      .foreach(p => {
        insertPoint(p._2)
      })

    //Update elements
    elements.foreach(p => {
      if (!p._2.safe_inlier && p._2.flag == 0) {
        if (p._2.lSky.getOrElse(0, ListBuffer()).count(_._2 >= p._2.arrival) >= k_max) p._2.safe_inlier = true
        else {
          var i, y: Int = 0
          var count = p._2.lSky.getOrElse(i, ListBuffer()).size
          do {
            if (count >= k_distinct_list(y)) { //inlier for all i
              y += 1
            } else { //outlier for all y
              for (z <- y until k_size) {
                all_queries(i)(z) += 1
              }
              i += 1
              count += p._2.lSky.getOrElse(i, ListBuffer()).size
            }
          } while (i < R_size && y < k_size)
        }
      }
    })

    for (i <- 0 until R_size) {
      for (y <- 0 until k_size) {
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

  def insertPoint(el: Data_lsky): Unit = {
    state.value().index.values.toList.reverse
      .foreach(p => {
        val thisDistance = distance(el.value.toArray, p.value.toArray)
        if (thisDistance <= R_max) {
          addNeighbor(el, p, thisDistance)
        }
      })
    state.value().index += ((el.id, el))
  }

  def deletePoint(el: Data_lsky): Unit = {
    state.value().index.remove(el.id)
  }

  def addNeighbor(el: Data_lsky, neigh: Data_lsky, distance: Double): Unit = {
    val norm_dist = normalizeDistance(distance)
    if (el.flag == 0 && el.lSky.getOrElse(norm_dist, ListBuffer()).size < k_max){
      el.lSky.update(norm_dist, el.lSky.getOrElse(norm_dist, ListBuffer[(Int, Long)]()) += ((neigh.id, neigh.arrival)))
    }else if(el.flag == 0 && el.lSky.getOrElse(norm_dist, ListBuffer()).size == k_max){
      val (minId, minArr) = el.lSky(norm_dist).minBy(_._2)
      if(neigh.arrival > minArr){
        el.lSky.update(norm_dist, el.lSky(norm_dist).filter(_._1 != minId) += ((neigh.id, neigh.arrival)))
      }
    }
    if (!neigh.safe_inlier && neigh.flag == 0 && neigh.lSky.getOrElse(norm_dist, ListBuffer()).size < k_max){
      neigh.lSky.update(norm_dist, neigh.lSky.getOrElse(norm_dist, ListBuffer[(Int, Long)]()) += ((el.id, el.arrival)))
    }else if(!neigh.safe_inlier && neigh.flag == 0 && neigh.lSky.getOrElse(norm_dist, ListBuffer()).size == k_max){
      val (minId, minArr) = neigh.lSky(norm_dist).minBy(_._2)
      if(el.arrival > minArr){
        neigh.lSky.update(norm_dist, neigh.lSky(norm_dist).filter(_._1 != minId) += ((el.id, el.arrival)))
      }
    }
  }

  def normalizeDistance(distance: Double): Int = {
    var res = -1
    var i = 0
    do {
      if (distance <= R_distinct_list(i)) res = i
      i += 1
    } while (i < R_distinct_list.size && res == -1)
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
