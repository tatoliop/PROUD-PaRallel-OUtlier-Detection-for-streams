package single_query

import utils.Utils.Query
import utils.Helpers.distance
import models.Data_mcod
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.{Counter, Gauge}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class PmcodNetMicroCluster(var center: ListBuffer[Double], var points: ListBuffer[Data_mcod])

case class PmcodNetState(var PD: mutable.HashMap[Int, Data_mcod], var MC: mutable.HashMap[Int, PmcodNetMicroCluster])

class Pmcod_net(c_query: Query) extends ProcessWindowFunction[(Int, Data_mcod), (Long, Query), Int, TimeWindow] {

  @transient private var counter: Counter = _
  @transient private var cpu_time: Long = 0L

  val query: Query = c_query
  val slide: Int = query.S
  val R: Double = query.R
  val k: Int = query.k
  var mc_counter: Int = 1

  lazy val state: ValueState[PmcodNetState] = getRuntimeContext
    .getState(new ValueStateDescriptor[PmcodNetState]("myState", classOf[PmcodNetState]))

  override def process(key: Int, context: Context, elements: Iterable[(Int, Data_mcod)], out: Collector[(Long, Query)]): Unit = {

    //Metrics
    counter.inc()
    val time_init = System.currentTimeMillis()

    val window = context.window
    //create state
    if (state.value == null) {
      val PD = mutable.HashMap[Int, Data_mcod]()
      val MC = mutable.HashMap[Int, PmcodNetMicroCluster]()
      val current = PmcodNetState(PD, MC)
      state.update(current)
    }

    //insert new elements
    elements
      .filter(_._2.arrival >= window.getEnd - slide)
      .foreach(p => insertPoint(p._2, true))

    //Check if there are clusters without the necessary points
    var reinsert = ListBuffer[Data_mcod]()
    state.value().MC.foreach(mc => {
      if(mc._2.points.size <= k) {
        reinsert = reinsert ++ mc._2.points
        state.value().MC.remove(mc._1)
      }
    })
    val reinsertIndexes = reinsert.map(_.id)
    //Reinsert points from deleted MCs
    reinsert.foreach(p => insertPoint(p, false, reinsertIndexes))

    //Find outliers
    var outliers = 0
    state.value().PD.values.foreach(p => {
      if (!p.safe_inlier && p.flag == 0)
        if (p.count_after + p.nn_before.count(_ >= window.getStart) < k) {
          outliers += 1
        }
    })

    val tmpQuery = Query(query.R, query.k, query.W, query.S, outliers)
    out.collect((window.getEnd, tmpQuery))

    //Remove expiring points without removing clusters
    elements
      .filter(p => p._2.arrival < window.getStart + slide)
      .foreach(p => {
        deletePoint(p._2)
      })

    //Metrics
    val time_final = System.currentTimeMillis()
    cpu_time += (time_final - time_init)
  }

  def insertPoint(el: Data_mcod, newPoint: Boolean, reinsert: ListBuffer[Int] = null): Unit = {
    var state = this.state.value()
    if (!newPoint) el.clear(-1)
    //Check against MCs on 3/2R
    val closeMCs = findCloseMCs(el)
    //Check if closer MC is within R/2
    val closerMC = if (closeMCs.nonEmpty)
      closeMCs.minBy(_._2)
    else
      (0, Double.MaxValue)
    if (closerMC._2 <= R / 2) { //Insert element to MC
      if (newPoint) {
        insertToMC(el, closerMC._1, true)
      }
      else {
        insertToMC(el, closerMC._1, false, reinsert)
      }
    }
    else { //Check against PD
      val NC = ListBuffer[Data_mcod]()
      val NNC = ListBuffer[Data_mcod]()
      state.PD.values
        .foreach(p => {
          val thisDistance = distance(el.value.toArray, p.value.toArray)
          if (thisDistance <= 3 * R / 2) {
            if (thisDistance <= R) { //Update metadata
              addNeighbor(el, p)
              if (newPoint) {
                addNeighbor(p, el)
              }
              else {
                if (reinsert.contains(p.id)) {
                  addNeighbor(p, el)
                }
              }
            }
            if (thisDistance <= R / 2) NC += p
            else NNC += p
          }
        })

      if (NC.size >= k) { //Create new MC
        createMC(el, NC, NNC)
      }
      else { //Insert in PD
        closeMCs.foreach(mc => el.Rmc += mc._1)
        state.MC.filter(mc => closeMCs.contains(mc._1))
          .foreach(mc => {
            mc._2.points.foreach(p => {
              val thisDistance = distance(el.value.toArray, p.value.toArray)
              if (thisDistance <= R) {
                addNeighbor(el, p)
              }
            })
          })
        state.PD += ((el.id, el))
      }
    }
  }

  def deletePoint(el: Data_mcod): Int = {
    var res = 0
    if (el.mc <= 0) { //Delete it from PD
      state.value().PD.remove(el.id)
    } else {
      state.value().MC(el.mc).points -= el
      if (state.value().MC(el.mc).points.size <= k) res = el.mc
    }
    res
  }

  def createMC(el: Data_mcod, NC: ListBuffer[Data_mcod], NNC: ListBuffer[Data_mcod]): Unit = {
    NC.foreach(p => {
      p.clear(mc_counter)
      state.value().PD.remove(p.id)
    })
    el.clear(mc_counter)
    NC += el
    val newMC = PmcodNetMicroCluster(el.value, NC)
    state.value().MC += ((mc_counter, newMC))
    NNC.foreach(p => p.Rmc += mc_counter)
    mc_counter += 1
  }

  def insertToMC(el: Data_mcod, mc: Int, update: Boolean, reinsert: ListBuffer[Int] = null): Unit = {
    el.clear(mc)
    state.value().MC(mc).points += el
    if (update) {
      state.value.PD.values.filter(p => p.Rmc.contains(mc)).foreach(p => {
        if (distance(p.value.toArray, el.value.toArray) <= R) {
          addNeighbor(p, el)
        }
      })
    }
    else {
      state.value.PD.values.filter(p => p.Rmc.contains(mc) && reinsert.contains(p.id)).foreach(p => {
        if (distance(p.value.toArray, el.value.toArray) <= R) {
          addNeighbor(p, el)
        }
      })
    }
  }

  def findCloseMCs(el: Data_mcod): mutable.HashMap[Int, Double] = {
    val res = mutable.HashMap[Int, Double]()
    state.value().MC.foreach(mc => {
      val thisDistance = distance(el.value.toArray, mc._2.center.toArray)
      if (thisDistance <= (3 * R) / 2) res.+=((mc._1, thisDistance))
    })
    res
  }

  def addNeighbor(el: Data_mcod, neigh: Data_mcod): Unit = {
    if (el.arrival > neigh.arrival) {
      el.insert_nn_before(neigh.arrival, k)
    } else {
      el.count_after += 1
      if (el.count_after >= k) el.safe_inlier = true
    }
  }

  override def open(parameters: Configuration): Unit = {
    counter = getRuntimeContext()
      .getMetricGroup()
      .counter("MySlideCounter")

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
