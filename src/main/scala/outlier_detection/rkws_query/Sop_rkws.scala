package outlier_detection.rkws_query

import models.{Data_basis, Data_lsky}
import utils.Helpers.distance
import utils.Utils.Query
import utils.states.Sop_state
import utils.traits.OutlierDetection

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class Sop_rkws(c_queries: ListBuffer[Query], c_distance: String, c_gcd_slide: Int) extends OutlierDetection {

  val state: Sop_state = new Sop_state
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


  override def new_slide(points: scala.Iterable[Data_basis], window_end: Long, window_start: Long): Unit = {
    //Get slide counter
    state.slide_count = window_end / slide
    //Insert new elements to state
    points
      .map(p => new Data_lsky(p))
      //Sort is needed when each point has a different timestamp
      //In our case every point in the same slide has the same timestamp
      .toList
      .sortBy(_.arrival)
      .foreach(p => state.index += ((p.id, p)))
  }

  override def old_slide(points: scala.Iterable[Int], window_end: Long, window_start: Long): Unit = {
    //Remove points
    points.foreach { p =>
      //Remove old points
      deletePoint(state.index(p))
    }
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
      if (!p.safe_inlier) {
        checkPoint(p, window_end, window_start)
        if (p.lSky.getOrElse(1, ListBuffer()).count(_._2 >= p.arrival) >= k_max) p.safe_inlier = true
        else {
          if (output_slide.nonEmpty) {
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
                      if (all_queries(i)(z)(w) == null)
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
    })

    val result = mutable.HashMap[Query, ListBuffer[Int]]()
    if (output_slide.nonEmpty) {
      val slide_to_report = output_slide.map(_ * slide)
      for (i <- 0 until R_size) {
        for (y <- 0 until k_size) {
          for (z <- 0 until W_size) {
            slide_to_report.foreach(p => {
              if (all_queries(i)(y)(z) != null)
                result.put(Query(R_distinct_list(i), k_distinct_list(y), W_distinct_list(z), p, all_queries(i)(y)(z).size), all_queries(i)(y)(z))
              else
                result.put(Query(R_distinct_list(i), k_distinct_list(y), W_distinct_list(z), p, 0), ListBuffer())
            })
          }
        }
      }
    }
    result
  }

  private def checkPoint(el: Data_lsky, window_end: Long, window_start: Long): Unit = {
    if (el.lSky.isEmpty) { //It's a new point!
      insertPoint(el, window_end)
    } else { //It's an old point
      updatePoint(el, window_end, window_start)
    }
  }

  private def insertPoint(el: Data_lsky, window_end: Long): Unit = {
    state.index.values.toList.reverse //get the points so far from latest to earliest
      .takeWhile(p => {
        var res = true
        if (p.id != el.id) {
          val thisDistance = distance(el.value.toArray, p.value.toArray, distance_type)
          if (thisDistance <= R_max) {
            val skyRes = neighborSkyband(el, p, thisDistance)
            if (!skyRes && thisDistance <= R_min) res = false
          }
        }
        res
      })
  }

  private def updatePoint(el: Data_lsky, window_end: Long, window_start: Long): Unit = {
    //Remove old points from lSky
    el.lSky.keySet.foreach(p => el.lSky.update(p, el.lSky(p).filter(_._2 >= window_start)))
    //Create input
    val old_sky = el.lSky.values.flatten.toList.sortWith((p1, p2) => p1._2 > p2._2).map(_._1)
    el.lSky.clear()

    var res = true //variable to stop skyband loop
    state.index.values.toList.reverse //Check new points
      .takeWhile(p => {
        var tmpRes = true
        if (p.arrival >= window_end - slide) {
          val thisDistance = distance(el.value.toArray, p.value.toArray, distance_type)
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
          val p = state.index(l)
          val thisDistance = distance(el.value.toArray, p.value.toArray, distance_type)
          if (thisDistance <= R_max) {
            val skyRes = neighborSkyband(el, p, thisDistance)
            if (!skyRes && thisDistance < R_min) res = false
          }
          res
        })
  }

  private def deletePoint(el: Data_lsky): Unit = {
    state.index.remove(el.id)
  }

  private def neighborSkyband(el: Data_lsky, neigh: Data_lsky, distance: Double): Boolean = {
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
