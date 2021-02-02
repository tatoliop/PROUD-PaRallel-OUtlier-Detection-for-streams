package outlier_detection.rk_query

import models.{Data_basis, Data_amcod}
import utils.Helpers.distance
import utils.Utils.Query
import utils.states.{Amcod_state, Amcod_microcluster}
import utils.traits.OutlierDetection

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class Amcod(c_queries: ListBuffer[Query], c_distance: String, c_gcd_slide: Int) extends OutlierDetection {

  val state: Amcod_state = new Amcod_state
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
      .map(p => new Data_amcod(p))
      //Sort is needed when each point has a different timestamp
      //In our case every point in the same slide has the same timestamp
      .toList
      .sortBy(_.arrival)
      .foreach(p => insertPoint(p, true))
  }

  override def old_slide(points: scala.Iterable[Int], window_end: Long, window_start: Long): Unit = {
    //Remove old points
    var deletedMCs = mutable.HashSet[Int]()
    points
      .foreach(p => {
        val delete = deletePoint(p)
        if (delete > 0) deletedMCs += delete
      })

    //Delete MCs
    if (deletedMCs.nonEmpty) {
      var reinsert = ListBuffer[Data_amcod]()
      deletedMCs.foreach(mc => {
        reinsert = reinsert ++ state.MC(mc).points.values
        state.MC.remove(mc)
      })
      val reinsertIndexes = reinsert.sortBy(_.arrival).map(_.id)

      //Reinsert points from deleted MCs
      reinsert.foreach(p => insertPoint(p, false, reinsertIndexes))
    }
  }

  override def assess_outliers(window_end: Long, window_start: Long): mutable.HashMap[Query, ListBuffer[Int]] = {
    //Create table of all queries
    val all_queries = Array.ofDim[ListBuffer[Int]](R_size, k_size)
    //Update elements
    state.PD.values.foreach(p => {
      if (!p.safe_inlier) {
        if (p.count_after >= k_max) {
          p.nn_before_set.clear()
          p.safe_inlier = true
        }
        else {
          if (check_functionality(p, window_end)) {
            var i, y: Int = 0
            var b_count = p.nn_before_set.count(p => p._1 >= window_start && p._2 <= R_distinct_list(i))
            var a_count = p.count_after_set.count(_ <= R_distinct_list(i))
            var count = b_count + a_count

            do {
              if (count >= k_distinct_list(y)) { //inlier for all i
                y += 1
              } else { //outlier for all y
                for (z <- y until k_size) {
                  if (all_queries(i)(z) == null)
                    all_queries(i)(z) = ListBuffer(p.id)
                  else
                    all_queries(i)(z) += p.id
                }
                i += 1
                if (i < R_size) {
                  b_count = p.nn_before_set.count(p => p._1 >= window_start && p._2 <= R_distinct_list(i))
                  a_count = p.count_after_set.count(_ <= R_distinct_list(i))
                  count = b_count + a_count
                }
              }
            } while (i < R_size && y < k_size)
          }
        }
      }
    })

    val result = mutable.HashMap[Query, ListBuffer[Int]]()
    for (i <- 0 until R_size) {
      for (y <- 0 until k_size) {
        if (all_queries(i)(y) != null)
          result.put(Query(R_distinct_list(i), k_distinct_list(y), queries.head.W, queries.head.S, all_queries(i)(y).size), all_queries(i)(y))
        else
          result.put(Query(R_distinct_list(i), k_distinct_list(y), queries.head.W, queries.head.S, 0), ListBuffer())
      }
    }
    result
  }

  private def insertPoint(el: Data_amcod, newPoint: Boolean, reinsert: ListBuffer[Int] = null): Unit = {
    if (!newPoint) el.clear(-1)
    //Check against MCs on 3 / 2 * R_max
    val closeMCs = findCloseMCs(el)
    //Check if closer MC is within R_min / 2
    val closerMC = if (closeMCs.nonEmpty)
      closeMCs.minBy(_._2)
    else
      (0, Double.MaxValue)
    if (closerMC._2 <= R_min / 2) { //Insert element to MC
      if (newPoint) {
        insertToMC(el, closerMC._1, true)
      }
      else {
        insertToMC(el, closerMC._1, false, reinsert)
      }
    } else { //Check against PD
      val NC = ListBuffer[Data_amcod]()
      val NNC = ListBuffer[Data_amcod]()
      state.PD.values
        .foreach(p => {
          val thisDistance = distance(el.value.toArray, p.value.toArray, distance_type)
          if (thisDistance <= 3 * R_max / 2) {
            if (thisDistance <= R_max) {
              addNeighbor(el, p, thisDistance)
              if (newPoint) {
                addNeighbor(p, el, thisDistance)
              }
              else {
                if (reinsert.contains(p.id)) {
                  addNeighbor(p, el, thisDistance)
                }
              }
            }
            if (thisDistance <= R_min / 2) NC += p
            else NNC += p
          }
        })
      if (NC.size >= k_max) { //Create new MC
        createMC(el, NC, NNC)
      }
      else { //Insert in PD
        closeMCs.foreach(mc => el.Rmc += mc._1)
        state.MC.filter(mc => closeMCs.contains(mc._1))
          .foreach(mc => {
            mc._2.points.foreach(p => {
              val thisDistance = distance(el.value.toArray, p._2.value.toArray, distance_type)
              if (thisDistance <= R_max) {
                addNeighbor(el, p._2, thisDistance)
              }
            })
          })
        //Do the skyband
        val tmp_nn_before = kSkyband(k_max - el.count_after - 1, el.nn_before_set)
        el.nn_before_set.clear()
        el.nn_before_set = tmp_nn_before
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
          if (r._2.points.size <= k_max) res = r._1
          not_found = false
        }
        not_found
      })
    }
    res
  }

  private def createMC(el: Data_amcod, NC: ListBuffer[Data_amcod], NNC: ListBuffer[Data_amcod]): Unit = {
    NC.foreach(p => {
      p.clear(state.cluster_id)
      state.PD.remove(p.id)
    })
    el.clear(state.cluster_id)
    NC += el
    val map: mutable.HashMap[Int, Data_amcod] = new mutable.HashMap[Int, Data_amcod]()
    NC.foreach(r => map.put(r.id, r))
    val newMC = Amcod_microcluster(el.value, map)
    state.MC += ((state.cluster_id, newMC))
    NNC.foreach(p => p.Rmc += state.cluster_id)
    state.cluster_id += 1
  }

  private def insertToMC(el: Data_amcod, mc: Int, update: Boolean, reinsert: ListBuffer[Int] = null): Unit = {
    el.clear(mc)
    state.MC(mc).points.put(el.id, el)
    if (update) {
      state.PD.values.filter(p => p.Rmc.contains(mc)).foreach(p => {
        val thisDistance = distance(p.value.toArray, el.value.toArray, distance_type)
        if (thisDistance <= R_max) {
          addNeighbor(p, el, thisDistance)
        }
      })
    }
    else {
      state.PD.values.filter(p => p.Rmc.contains(mc) && reinsert.contains(p.id)).foreach(p => {
        val thisDistance = distance(p.value.toArray, el.value.toArray, distance_type)
        if (thisDistance <= R_max) {
          addNeighbor(p, el, thisDistance)
        }
      })
    }
  }

  private def findCloseMCs(el: Data_amcod): mutable.HashMap[Int, Double] = {
    val res = mutable.HashMap[Int, Double]()
    state.MC.foreach(mc => {
      val thisDistance = distance(el.value.toArray, mc._2.center.toArray, distance_type)
      if (thisDistance <= (3 * R_max) / 2) res.+=((mc._1, thisDistance))
    })
    res
  }

  private def addNeighbor(el: Data_amcod, neigh: Data_amcod, distance: Double): Unit = {
    if (el.arrival > neigh.arrival) {
      el.nn_before_set.+=((neigh.arrival, distance))
    } else {
      el.count_after_set.+=(distance)
      if (distance <= R_min) {
        el.count_after += 1
      }
    }
  }

  private def kSkyband(k: Int, neighborsC: ListBuffer[(Long, Double)]): ListBuffer[(Long, Double)] = {
    //neighbors should be in ascending order of distances
    val neighbors = neighborsC.sortBy(_._2)
    val res: ListBuffer[(Long, Double)] = ListBuffer()
    for (i <- neighbors.indices) {
      var counter = 0
      for (y <- 0 until i) {
        if (neighbors(y)._1 > neighbors(i)._1) counter += 1
      }
      if (counter <= k) {
        res.append(neighbors(i))
      }
    }
    res
  }


}
