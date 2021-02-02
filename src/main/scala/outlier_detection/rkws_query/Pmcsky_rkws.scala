package outlier_detection.rkws_query

import models.{Data_basis, Data_mcsky}
import utils.Helpers.distance
import utils.Utils.Query
import utils.states.{Pmcsky_microcluster, Pmcsky_state}
import utils.traits.OutlierDetection

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class Pmcsky_rkws(c_queries: ListBuffer[Query], c_distance: String, c_gcd_slide: Int) extends OutlierDetection {

  val state: Pmcsky_state = new Pmcsky_state
  //OD vars
  val queries: ListBuffer[Query] = c_queries
  val slide: Int = c_gcd_slide
  val distance_type: String = c_distance
  //Multi-params vars
  val R_distinct_list = queries.map(_.R).distinct.sorted
  val k_distinct_list = queries.map(_.k).distinct.sorted
  val W_distinct_list = queries.map(_.W).distinct.sorted
  val S_distinct_list = queries.map(_.S).distinct.sorted
  val R_max = R_distinct_list.max
  val R_min = R_distinct_list.min
  val k_max = k_distinct_list.max
  val k_min = k_distinct_list.min
  val W_min = W_distinct_list.min
  val k_size = k_distinct_list.size
  val R_size = R_distinct_list.size
  val W_size = W_distinct_list.size
  val S_size = S_distinct_list.size
  val S_report_times = S_distinct_list.map(p => p / slide).sorted
  val S_max_report = S_report_times.max


  override def new_slide(points: scala.Iterable[Data_basis],window_end: Long, window_start: Long): Unit = {
    //Get slide counter
    state.slide_count = window_end / slide
    //Insert new elements to state
    points
      .map(p => new Data_mcsky(p))
      //Sort is needed when each point has a different timestamp
      //In our case every point in the same slide has the same timestamp
      .toList
      .sortBy(_.arrival)
      .foreach(p => state.index += ((p.id, p)))
  }

  override def old_slide(points: scala.Iterable[Int],window_end: Long, window_start: Long): Unit = {
    //Remove points
    state.index.values
      .filter(p => p.arrival < window_end - W_min + slide)
      .foreach(p => {
        //Remove small window points from clusters
        if(p.mc != -1 && p.arrival >= window_end - W_min)
          deleteSmallWindowPoint(p)
        //Remove old points
        if (p.arrival < window_start + slide)
          deletePoint(p)
      })
  }

  override def assess_outliers(window_end: Long, window_start: Long): mutable.HashMap[Query, ListBuffer[Int]] = {
    //Check which slides to output
    var output_slide = ListBuffer[Int]()
    S_report_times.foreach(p => {
      if (state.slide_count % p == 0) output_slide += p
    })
    //Create table of all queries
    val all_queries = Array.ofDim[ListBuffer[Int]](R_size, k_size, W_size)
    //Update elements
    state.index.values.foreach(p => {
      if (!p.safe_inlier && p.mc == -1) {
        checkPoint(p, window_end, window_start)
        if(p.mc == -1){
          if (p.lSky.getOrElse(1, ListBuffer()).count(_._2 >= p.arrival) >= k_max) p.safe_inlier = true
          else{
            if (output_slide.nonEmpty ) {
              var w: Int = 0
              do {
                if (p.arrival >= window_end - W_distinct_list(w) && check_functionality(p, window_end)) {
                  var i, y: Int = 0
                  var count = p.lSky.getOrElse(i + 1, ListBuffer()).count(_._2 >= window_end - W_distinct_list(w))
                  do {
                    if (count >= k_distinct_list(y)) { //inlier for all i
                      y += 1
                    } else { //outlier for all y
                      for (z <- y until k_size) {
                        if(all_queries(i)(z)(w) == null)
                          all_queries(i)(z)(w) = ListBuffer(p.id)
                        else
                          all_queries(i)(z)(w) += p.id
                      }
                      i += 1
                      count += p.lSky.getOrElse(i + 1, ListBuffer()).count(_._2 >= window_end - W_distinct_list(w))
                    }
                  } while (i < R_size && y < k_size)
                }
                w += 1
              } while (w < W_size)
            }
          }
        }
      }
    })

    val result = mutable.HashMap[Query, ListBuffer[Int]]()
    if (output_slide.nonEmpty) {
      val slide_to_report = output_slide.map(_ * slide)
      for (i <- 0 until R_size) {
        for (y <- 0 until k_size) {
          for (z <- 0 until W_size) {
            slide_to_report.foreach(p => {
              if(all_queries(i)(y)(z) != null)
                result.put(Query(R_distinct_list(i),k_distinct_list(y),W_distinct_list(z), p, all_queries(i)(y)(z).size), all_queries(i)(y)(z))
              else
                result.put(Query(R_distinct_list(i),k_distinct_list(y),W_distinct_list(z), p, 0), ListBuffer())
            })
          }
        }
      }
    }
    result
  }

  private def checkPoint(el: Data_mcsky, window_end: Long, window_start: Long): Unit = {
    if (el.lSky.isEmpty && el.mc == -1) { //It's a new point!
      insertPoint(el, window_end)
    } else if (el.mc == -1) { //It's an old point
      updatePoint(el, window_end, window_start)
    }
  }

  private def insertPoint(el: Data_mcsky, window_end: Long): Unit = {
    val small_window = if( el.arrival >= window_end - W_min) true else false
    //Check against MCs on 3 / 2 * R_max
    val closeMCs = findCloseMCs(el)
    //Check if closer MC is within R_min / 2
    val closerMC = if (closeMCs.nonEmpty)
      closeMCs.minBy(_._2)
    else
      (0, Double.MaxValue)
    if (closerMC._2 <= R_min / 2 && small_window) { //Insert element to MC
      //Create MCs only on the smaller window parameter
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
                if (p.arrival >= window_end - W_min && p.mc == -1 && thisDistance <= R_min / 2) NC += p
              }
            }
          }
          res
        })
      if (small_window && NC.size >= k_max) { //Create new MC
        createNewMC(el, NC)
      }
      else { //Insert in PD
        state.PD += el.id
      }
    }
  }

  private def updatePoint(el: Data_mcsky, window_end: Long, window_start: Long): Unit = {
    val small_window = if( el.arrival >= window_end - W_min) true else false
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
          if (p.arrival >= window_end - W_min && state.PD.contains(p.id) && thisDistance <= R_min / 2) NC += p
        }
      })
    if (small_window && NC.size >= k_max) createNewMC(el, NC) //Create new MC
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
    }
    state.index.remove(el.id)
  }

  private def deleteSmallWindowPoint(el: Data_mcsky): Unit = {
    if (el.mc != -1) { //Delete it from MC
      state.MC(el.mc).points -= el.id
      if (state.MC(el.mc).points.size <= k_max) {
        state.MC(el.mc).points.foreach(p => {
          state.index(p).clear(-1)
        })
        state.MC.remove(el.mc)
      }
      el.clear(-1)
    }
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
