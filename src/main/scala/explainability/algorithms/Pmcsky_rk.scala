package explainability.algorithms

import models.{Data_basis, Data_mcsky}
import utils.Helpers.distance
import utils.Utils.Query
import utils.states.{Pmcsky_microcluster, Pmcsky_state}
import utils.traits.ExplainOutlierDetection

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class Pmcsky_rk(c_queries: ListBuffer[Query], c_distance: String, c_gcd_slide: Int) extends ExplainOutlierDetection {

  val state: Pmcsky_state = new Pmcsky_state
  //OD vars
  val queries: ListBuffer[Query] = c_queries
  val slide: Int = c_gcd_slide
  val distance_type: String = c_distance
  //Multi-params vars
  val R_distinct_list = queries.map(_.R).distinct.sorted
  val k_distinct_list = queries.map(_.k).distinct.sorted
  val R_max = R_distinct_list.max
  val R_min = R_distinct_list.min
  val k_max = k_distinct_list.max
  val k_min = k_distinct_list.min
  val k_size = k_distinct_list.size
  val R_size = R_distinct_list.size


  override def new_slide(points: scala.Iterable[Data_basis], window_end: Long, window_start: Long): Unit = {
    //Insert new elements to state
    points
      .map(p => new Data_mcsky(p))
      //Sort is needed when each point has a different timestamp
      //In our case every point in the same slide has the same timestamp
      .toList
      .sortBy(_.arrival)
      .foreach(p => state.index += ((p.id, p)))
  }

  override def old_slide(points: scala.Iterable[Int], window_end: Long, window_start: Long): Unit = {
    //Remove points
    points.foreach(p => deletePoint(state.index(p)))
  }

  override def assess_outliers(window_end: Long, window_start: Long): (mutable.HashMap[Query, ListBuffer[(Int, Int)]], Int) = {
    //Create table of all queries
    val all_queries = Array.ofDim[ListBuffer[(Int, Int)]](R_size, k_size)
    //Update elements
    state.index.values.foreach(p => {
      if (!p.safe_inlier && p.mc == -1) {
        checkPoint(p, window_end, window_start)
        if (p.mc == -1) {
          if (p.lSky.getOrElse(1, ListBuffer()).count(_._2 >= p.arrival) >= k_max) p.safe_inlier = true
          else {
            if (check_functionality(p, window_end)) {
              var i, y: Int = 0
              var count = p.lSky.getOrElse(i + 1, ListBuffer()).count(_._2 >= window_start)
              do {
                if (count >= k_distinct_list(y)) { //inlier for all i
                  y += 1
                } else { //outlier for all y
                  for (z <- y until k_size) {
                    if (all_queries(i)(z) == null)
                      all_queries(i)(z) = ListBuffer((p.id,0))
                    else
                      all_queries(i)(z) += ((p.id,0))
                  }
                  i += 1
                  count += p.lSky.getOrElse(i + 1, ListBuffer()).count(_._2 >= window_start)
                }
              } while (i < R_size && y < k_size)
            }
          }
        }
      }
    })

    val result = mutable.HashMap[Query, ListBuffer[(Int, Int)]]()
    for (i <- 0 until R_size) {
      for (y <- 0 until k_size) {
        if (all_queries(i)(y) != null)
          result.put(Query(R_distinct_list(i), k_distinct_list(y), queries.head.W, queries.head.S, all_queries(i)(y).size), all_queries(i)(y))
        else
          result.put(Query(R_distinct_list(i), k_distinct_list(y), queries.head.W, queries.head.S, 0), ListBuffer())
      }
    }
    (result, 0)
  }

  private def checkPoint(el: Data_mcsky, window_end: Long, window_start: Long): Unit = {
    if (el.lSky.isEmpty && el.mc == -1) { //It's a new point!
      insertPoint(el, window_end)
    } else if (el.mc == -1) { //It's an old point
      updatePoint(el, window_end, window_start)
    }
  }

  private def insertPoint(el: Data_mcsky, window_end: Long): Unit = {
    //Check against MCs on 3 / 2 * R_max
    val closeMCs = findCloseMCs(el)
    //Check if closer MC is within R_min / 2
    val closerMC = if (closeMCs.nonEmpty)
      closeMCs.minBy(_._2)
    else
      (0, Double.MaxValue)
    if (closerMC._2 <= R_min / 2) { //Insert element to MC
      insertToMC(el, closerMC._1)
    } else { //Check against PD
      val NC = ListBuffer[Data_mcsky]() //List to hold points for new cluster formation
      state.index.values.toList.reverse //get the points so far from latest to earliest
        .takeWhile(p => {
          var res = true
          if (p.id != el.id) {
            if (closeMCs.keySet.contains(p.mc) || p.mc == -1) { //check only the points in PD and close MCs
              val thisDistance = distance(el.value.toArray, p.value.toArray, distance_type)
              if (thisDistance <= R_max) {
                val skyRes = neighborSkyband(el, p, thisDistance)
                if (!skyRes && thisDistance <= R_min) res = false
                if (p.mc == -1 && thisDistance <= R_min / 2) NC += p
              }
            }
          }
          res
        })
      if (NC.size >= k_max) { //Create new MC
        createNewMC(el, NC)
      }
      else { //Insert in PD
        state.PD += el.id
      }
    }
  }

  private def updatePoint(el: Data_mcsky, window_end: Long, window_start: Long): Unit = {
    //Remove old points from lSky
    el.lSky.keySet.foreach(p => el.lSky.update(p, el.lSky(p).filter(_._2 >= window_start)))
    //Create input
    val old_sky = el.lSky.values.flatten.toList.sortWith((p1, p2) => p1._2 > p2._2).map(_._1)
    el.lSky.clear()

    var res = true //variable to stop skyband loop
    val NC = ListBuffer[Data_mcsky]() //List to hold points for new cluster formation
    state.index.values.toList.reverse //Check new points
      .takeWhile(p => {
        var tmpRes = true
        if (p.arrival >= window_end - slide) {
          val thisDistance = distance(el.value.toArray, p.value.toArray, distance_type)
          if (thisDistance <= R_max) {
            val skyRes = neighborSkyband(el, p, thisDistance)
            if (!skyRes && thisDistance <= R_min) res = false
            if (state.PD.contains(p.id) && thisDistance <= R_min / 2) NC += p
          }
        } else tmpRes = false
        res && tmpRes
      })

    if (res)
      old_sky.foreach(l => { //Check the old skyband elements
        val p = state.index(l)
        val thisDistance = distance(el.value.toArray, p.value.toArray, distance_type)
        if (thisDistance <= R_max) {
          val skyRes = neighborSkyband(el, p, thisDistance)
          if (!skyRes && thisDistance <= R_min) res = false
          if (state.PD.contains(p.id) && thisDistance <= R_min / 2) NC += p
        }
      })
    if (NC.size >= k_max) createNewMC(el, NC) //Create new MC
    else state.PD += el.id //Insert in PD
  }

  private def findCloseMCs(el: Data_mcsky): mutable.HashMap[Int, Double] = {
    val res = mutable.HashMap[Int, Double]()
    state.MC.foreach(mc => {
      val thisDistance = distance(el.value.toArray, mc._2.center.toArray, distance_type)
      if (thisDistance <= (3 * R_max) / 2) res.+=((mc._1, thisDistance))
    })
    res
  }

  private def insertToMC(el: Data_mcsky, mc: Int): Unit = {
    el.clear(mc)
    state.MC(mc).points += el.id
    state.PD.remove(el.id)
  }

  private def createNewMC(el: Data_mcsky, NC: ListBuffer[Data_mcsky]): Unit = {
    NC += el
    NC.foreach(p => {
      p.clear(state.cluster_id)
      state.PD.remove(p.id)
    })
    val newMC = Pmcsky_microcluster(el.value, NC.map(_.id))
    state.MC += ((state.cluster_id, newMC))
    state.cluster_id += 1
  }

  private def deletePoint(el: Data_mcsky): Unit = {
    if (el.mc == -1) { //Delete it from PD
      state.PD.remove(el.id)
    } else {
      state.MC(el.mc).points -= el.id
      if (state.MC(el.mc).points.size <= k_max) {
        state.MC(el.mc).points.foreach(p => {
          state.index(p).clear(-1)
        })
        state.MC.remove(el.mc)
      }
    }
    state.index.remove(el.id)
  }

  private def neighborSkyband(el: Data_mcsky, neigh: Data_mcsky, distance: Double): Boolean = {
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

  private def normalizeDistance(distance: Double): Int = {
    var res, i = 0
    do {
      if (distance <= R_distinct_list(i)) res = i + 1
      i += 1
    } while (i < R_distinct_list.size && res == 0)
    res
  }

}
