package outlier_detection.single_query

import models.{Data_advanced, Data_basis}
import mtree.MTree
import utils.Utils.Query
import utils.states.Advanced_state
import utils.traits.OutlierDetection

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

//TODO sometimes an element is not found in the hashmap of the state. Need to debug. Seems to be random
class Advanced(c_query: Query, c_distance: String) extends OutlierDetection {

  //OD vars
  val query: Query = c_query
  val k: Int = query.k
  val R: Double = query.R
  val slide: Int = query.S
  val distance_type: String = c_distance
  //Algorithm state
  val state: Advanced_state = new Advanced_state(distance_type, k)

  override def new_slide(points: scala.Iterable[Data_basis], window_end: Long, window_start: Long): Unit = {
    val transformed = points.map(p => new Data_advanced(p))
    transformed
      .foreach { p =>
            state.tree.add(p)
            state.hashMap.+=((p.id, p))
          }

    //Get neighbors
    transformed
      .foreach(p => {
        val tmpData = p
        val query: MTree[Data_advanced]#Query = state.tree.getNearestByRange(tmpData, R)
        val iter = query.iterator()
        while (iter.hasNext) {
          val node = iter.next().data
          if (node.id != tmpData.id) {
            if (node.arrival < (window_end - slide)) {
              if (tmpData.flag == 0 || check_functionality(tmpData, window_end)) {
                state.hashMap(tmpData.id).insert_nn_before(node.arrival, k)
              }
              if (node.flag == 0 || check_functionality(node, window_end)) {
                state.hashMap(node.id).count_after += 1
                if (state.hashMap(node.id).count_after >= k)
                  state.hashMap(node.id).safe_inlier = true
              }
            } else {
              if (tmpData.flag == 0 || check_functionality(tmpData, window_end)) {
                state.hashMap(tmpData.id).count_after += 1
                if (state.hashMap(tmpData.id).count_after >= k)
                  state.hashMap(tmpData.id).safe_inlier = true
              }
            }
          }
        }
      })
  }

  override def old_slide(points: scala.Iterable[Int], window_end: Long, window_start: Long): Unit = {
    //Remove expiring objects and flagged ones from state
    points
      .foreach(el => {
        state.tree.remove(state.hashMap(el))
        state.hashMap.-=(el)
      })
  }

  override def assess_outliers(window_end: Long, window_start: Long): mutable.HashMap[Query, ListBuffer[Int]] = {
    //Variable for number of outliers
    val outliers: ListBuffer[Int] = state.hashMap.filter(p => {
      check_functionality(p._2, window_end) && !p._2.safe_inlier && (p._2.count_after + p._2.nn_before.count(_ >= window_start) < k)
    }).keys.to[ListBuffer]
    mutable.HashMap[Query, ListBuffer[Int]](query -> outliers)
  }



}
