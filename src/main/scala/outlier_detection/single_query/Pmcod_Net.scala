package outlier_detection.single_query

import models.{Data_basis, Data_mcod}
import utils.Helpers.distance
import utils.Utils.Query
import utils.states.{Pmcod_microcluster, Pmcod_state}
import utils.traits.OutlierDetection

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class Pmcod_Net(c_query: Query, c_distance: String) extends OutlierDetection {

  //Algorithm state
  val state: Pmcod_state = new Pmcod_state
  var mc_counter = 1
  //OD vars
  val query: Query = c_query
  val k: Int = query.k
  val R: Double = query.R
  val distance_type: String = c_distance

  override def new_slide(points: scala.Iterable[Data_basis], window_end: Long, window_start: Long): Unit = {
    points
      .foreach { p =>
        val transformed_point = new Data_mcod(p)
        insertPoint(transformed_point, true)
      }

    //Check if there are clusters without the necessary points
    var reinsert = ListBuffer[Data_mcod]()
    state.MC.foreach(mc => {
      if(mc._2.points.size <= k) {
        reinsert ++= mc._2.points.values
        state.MC.remove(mc._1)
      }
    })
    val reinsertIndexes = reinsert.map(_.id)
    //Reinsert points from deleted MCs
    reinsert.foreach(p => insertPoint(p, false, reinsertIndexes))
  }

  override def old_slide(points: scala.Iterable[Int], window_end: Long, window_start: Long): Unit = {
    //Remove old points
    points
      .foreach(p => {
        deletePoint(p)
      })
  }

  override def assess_outliers(window_end: Long, window_start: Long): mutable.HashMap[Query, ListBuffer[Int]] = {
    var outliers = ListBuffer[Int]()
    state.PD.values.foreach(p => {
      if (!p.safe_inlier) {
        if (check_functionality(p, window_end)) //Necessary function for all algos for adaptivity mechanisms
          if (p.count_after + p.nn_before.count(_ >= window_start) < k) outliers += p.id
      }
    })
    mutable.HashMap[Query, ListBuffer[Int]](query -> outliers)
  }

  private def insertPoint(el: Data_mcod, newPoint: Boolean, reinsert: ListBuffer[Int] = null): Unit = {
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
          val thisDistance = distance(el.value.toArray, p.value.toArray, distance_type)
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
              val thisDistance = distance(el.value.toArray, p._2.value.toArray, distance_type)
              if (thisDistance <= R) {
                addNeighbor(el, p._2)
              }
            })
          })
        state.PD += ((el.id, el))
      }
    }
  }

  private def deletePoint(el: Int): Int = {
    var res = 0
    if (state.PD.contains(el)) { //Delete it from PD
      state.PD.remove(el)
    } else {
      var not_found = true
      state.MC.takeWhile(r => {
        if (r._2.points.contains(el)) {
          r._2.points -= el
          if (r._2.points.size <= k) res = r._1
          not_found = false
        }
        not_found
      })
    }
    res
  }

  private def createMC(el: Data_mcod, NC: ListBuffer[Data_mcod], NNC: ListBuffer[Data_mcod]): Unit = {
    NC.foreach(p => {
      p.clear(mc_counter)
      state.PD.remove(p.id)
    })
    el.clear(mc_counter)
    NC += el
    val map: mutable.HashMap[Int, Data_mcod] = new mutable.HashMap[Int, Data_mcod]()
    NC.foreach(r => map.put(r.id, r))
    val newMC = Pmcod_microcluster(el.value, map)
    state.MC.put(mc_counter, newMC)
    NNC.foreach(p => p.Rmc += mc_counter)
    mc_counter += 1
  }

  private def insertToMC(el: Data_mcod, mc: Int, update: Boolean, reinsert: ListBuffer[Int] = null): Unit = {
    el.clear(mc)
    state.MC(mc).points.put(el.id, el)
    if (update) {
      state.PD.values.filter(p => p.Rmc.contains(mc)).foreach(p => {
        if (distance(p.value.toArray, el.value.toArray, distance_type) <= R) {
          addNeighbor(p, el)
        }
      })
    }
    else {
      state.PD.values.filter(p => p.Rmc.contains(mc) && reinsert.contains(p.id)).foreach(p => {
        if (distance(p.value.toArray, el.value.toArray, distance_type) <= R) {
          addNeighbor(p, el)
        }
      })
    }
  }

  private def findCloseMCs(el: Data_mcod): mutable.HashMap[Int, Double] = {
    val res = mutable.HashMap[Int, Double]()
    state.MC.foreach(mc => {
      val thisDistance = distance(el.value.toArray, mc._2.center.toArray, distance_type)
      if (thisDistance <= (3 * R) / 2) res.+=((mc._1, thisDistance))
    })
    res
  }

  private def addNeighbor(el: Data_mcod, neigh: Data_mcod): Unit = {
    if (el.arrival > neigh.arrival) {
      el.insert_nn_before(neigh.arrival, k)
    } else {
      el.count_after += 1
      if (el.count_after >= k) el.safe_inlier = true
    }
  }


}
