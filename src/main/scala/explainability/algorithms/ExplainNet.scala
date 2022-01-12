package explainability.algorithms

import models.{Data_basis, Data_mcsky}
import utils.Helpers.{distance, explain}
import utils.Utils.Query
import utils.states.{Explain_state, Pmcsky_microcluster, Pmcsky_state}
import utils.traits.ExplainOutlierDetection

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class ExplainNet(c_query: Query, c_distance: String, c_status: Int) extends ExplainOutlierDetection {

  val state: Pmcsky_state = new Pmcsky_state
  val explainState: Explain_state = new Explain_state
  //Explain vars
  val distribution: List[Double] = List(0.25, 0.5, 1, 2, 4)
  val status: Int = c_status
  //OD vars
  val query: Query = c_query
  val k: Int = query.k
  val R: Double = query.R
  val slide: Int = query.S
  val distance_type: String = c_distance
  //Multi-params vars
  val R_distinct_list: List[Double] = for (distr <- distribution) yield R * distr
  val k_distinct_list: List[Double] = for (distr <- distribution) yield k * distr
  val R_max: Double = R_distinct_list.max
  val R_min: Double = R_distinct_list.min
  val k_max: Double = k_distinct_list.max
  val k_min: Double = k_distinct_list.min
  val k_size: Int = k_distinct_list.size
  val R_size: Int = R_distinct_list.size

  var explanation_count: Int = 0

  override def new_slide(points: scala.Iterable[Data_basis], window_end: Long, window_start: Long): Unit = {
    //Insert new elements to state
    points
      .map(p => new Data_mcsky(p))
      //Sort is needed when each point has a different timestamp
      //In our case every point in the same slide has the same timestamp
      .toList
      .sortBy(_.arrival)
      .foreach { p =>
        state.index += ((p.id, p))
        explainState.netChange += ((p.id, (-1, 0)))
      }
  }

  override def old_slide(points: scala.Iterable[Int], window_end: Long, window_start: Long): Unit = {
    //Remove points
    points.foreach { p =>
      deletePoint(state.index(p))
      explainState.netChange.remove(p)
    }
  }

  override def assess_outliers(window_end: Long, window_start: Long): (mutable.HashMap[Query, ListBuffer[(Int, Int)]], Int) = {
    val outliers = ListBuffer[(Int, Int)]()
    //Update elements
    state.index.values.foreach(p => {
      if (!p.safe_inlier && p.mc == -1) {
        checkPoint(p, window_end, window_start)
        if (p.mc == -1) {
          if (p.lSky.getOrElse(1, ListBuffer()).count(_._2 >= p.arrival) >= k_max) p.safe_inlier = true
          else {
            if (check_functionality(p, window_end)) {
              //R-k table
              var rk_table = Array.ofDim[Boolean](R_size, k_size)
              var i, y: Int = 0
              var count = p.lSky.getOrElse(i + 1, ListBuffer()).count(_._2 >= window_start)
              do {
                if (count >= k_distinct_list(y)) { //inlier for all i
                  for (z <- i until R_size) rk_table(z)(y) = true
                  y += 1
                } else { //outlier for all y
                  for (z <- y until k_size) rk_table(i)(z) = false
                  i += 1
                  count += p.lSky.getOrElse(i + 1, ListBuffer()).count(_._2 >= window_start)
                }
              } while (i < R_size && y < k_size)
              //Explain if outlier
              if (!rk_table(2)(2)) {
                val category = if(explainState.netChange(p.id)._1 == -1 || explainState.netChange(p.id)._2 != 0) {
                  explanation_count += 1
                  explain(rk_table, p.lSky.map(el => (el._1, el._2.size)), status, k)
                } else explainState.netChange(p.id)._1
                explainState.resetNet(p.id, category)
                outliers += ((p.id, category))
              }
            }
          }
        }
      }
    })
    //Output results
    (mutable.HashMap[Query, ListBuffer[(Int, Int)]](query -> outliers), explanation_count)
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
    val checkNet = if(explainState.netChange(el.id)._1 != -1) true else false
    //Remove old points from lSky
    el.lSky.keySet.foreach{p =>
      if(checkNet) explainState.reduceNet(el.id, el.lSky(p).count(_._2 < window_start))
      el.lSky.update(p, el.lSky(p).filter(_._2 >= window_start))
    }

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
            if(checkNet) explainState.addNet(el.id)
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
