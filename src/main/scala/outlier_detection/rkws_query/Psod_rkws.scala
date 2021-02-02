package outlier_detection.rkws_query

import models.{Data_basis, Data_lsky}
import utils.Helpers.distance
import utils.Utils.Query
import utils.states.Psod_state
import utils.traits.OutlierDetection

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class Psod_rkws(c_queries: ListBuffer[Query], c_distance: String, c_gcd_slide: Int) extends OutlierDetection {

  val state: Psod_state = new Psod_state
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
  val S_distinct_downgraded = S_distinct_list.map(_ / slide)
  val S_var = List.range(1, S_distinct_downgraded.product + 1).filter(p => {
    var done = false
    S_distinct_downgraded.foreach(l => {
      if (p % l == 0) done = true
    })
    done
  }).distinct.sorted
  val S_var_max = S_var.max

  override def new_slide(points: scala.Iterable[Data_basis], window_end: Long, window_start: Long): Unit = {
    //Get slide counter
    if(state.slide_count == 0) {
      state.slide_count = (window_end / slide)
      if(state.slide_count > S_var_max) state.slide_count = state.slide_count % S_var_max
      state.last_window = window_end
    }else{
      val diff = (window_end - state.last_window) / slide
      state.slide_count = state.slide_count + diff
      if(state.slide_count > S_var_max) state.slide_count = state.slide_count % S_var_max
      state.last_window = window_end
    }

    //Remove old points from each lSky
    state.index.values.foreach(p => {
      p.lSky.keySet.foreach(l => p.lSky.update(l, p.lSky(l).filter(_._2 >= window_start)))
    })
    //Insert new elements to state
    points
      .map(p => new Data_lsky(p))
      .foreach(p => insertPoint(p, window_end))
  }

  override def old_slide(points: scala.Iterable[Int], window_end: Long, window_start: Long): Unit = {
    //Remove points
    points
      .foreach(p => deletePoint(state.index(p)))
  }

  override def assess_outliers(window_end: Long, window_start: Long): mutable.HashMap[Query, ListBuffer[Int]] = {
    //Create table of all queries
    val all_queries = Array.ofDim[ListBuffer[Int]](R_size, k_size, W_size)
    //Update elements
    state.index.values.foreach(p => {
      if (!p.safe_inlier) {
        if (p.lSky.getOrElse(0, ListBuffer()).count(_._2 >= p.arrival) >= k_max) p.safe_inlier = true
        else {
          if (S_var.contains(state.slide_count)) {
            var w: Int = 0
            do {
              if (p.arrival >= window_end - W_distinct_list(w) && check_functionality(p, window_end)) {
                var i, y: Int = 0
                var count = p.lSky.getOrElse(i, ListBuffer()).count(_._2 >= window_end - W_distinct_list(w))
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
                    count += p.lSky.getOrElse(i, ListBuffer()).count(_._2 >= window_end - W_distinct_list(w))
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
    if (S_var.contains(state.slide_count)) {
      val slide_to_report = S_distinct_downgraded.filter(state.slide_count % _ == 0).map(_ * slide)
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

  private def insertPoint(el: Data_lsky, window_end: Long): Unit = {
    state.index.values.toList.reverse
      .foreach(p => {
        val thisDistance = distance(el.value.toArray, p.value.toArray, distance_type)
        if (thisDistance <= R_max) {
          addNeighbor(el, p, thisDistance, window_end: Long)
        }
      })
    state.index += ((el.id, el))
  }

  private def deletePoint(el: Data_lsky): Unit = {
    state.index.remove(el.id)
  }

  private def addNeighbor(el: Data_lsky, neigh: Data_lsky, distance: Double, window_end: Long): Unit = {
    val norm_dist = normalizeDistance(distance)
    if (el.flag == 0 || check_functionality(el, window_end)) {
      if (el.lSky.getOrElse(norm_dist, ListBuffer()).size < k_max) {
        el.lSky.update(norm_dist, el.lSky.getOrElse(norm_dist, ListBuffer[(Int, Long)]()) += ((neigh.id, neigh.arrival)))
      } else if (el.lSky.getOrElse(norm_dist, ListBuffer()).size == k_max) {
        val (minId, minArr) = el.lSky(norm_dist).minBy(_._2)
        if (neigh.arrival > minArr) {
          el.lSky.update(norm_dist, el.lSky(norm_dist).filter(_._1 != minId) += ((neigh.id, neigh.arrival)))
        }
      }
    }
    if (neigh.flag == 0 || check_functionality(neigh, window_end)) {
      if (!neigh.safe_inlier && neigh.lSky.getOrElse(norm_dist, ListBuffer()).size < k_max) {
        neigh.lSky.update(norm_dist, neigh.lSky.getOrElse(norm_dist, ListBuffer[(Int, Long)]()) += ((el.id, el.arrival)))
      } else if (!neigh.safe_inlier && neigh.lSky.getOrElse(norm_dist, ListBuffer()).size == k_max) {
        val (minId, minArr) = neigh.lSky(norm_dist).minBy(_._2)
        if (el.arrival > minArr) {
          neigh.lSky.update(norm_dist, neigh.lSky(norm_dist).filter(_._1 != minId) += ((el.id, el.arrival)))
        }
      }
    }
  }

  private def normalizeDistance(distance: Double): Int = {
    var res = -1
    var i = 0
    do {
      if (distance <= R_distinct_list(i)) res = i
      i += 1
    } while (i < R_distinct_list.size && res == -1)
    res
  }


}
