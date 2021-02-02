package outlier_detection.single_query

import models.{Data_basis, Data_cod, NodeType}
import mtree.MTree
import utils.Utils.Query
import utils.states.Cod_state.{Cod_EventQElement, Cod_state}
import utils.traits.OutlierDetection

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class Cod(c_query: Query, c_distance: String) extends OutlierDetection {

  //OD vars
  val query: Query = c_query
  val k: Int = query.k
  val R: Double = query.R
  val slide: Int = query.S
  val distance_type: String = c_distance
  //Algorithm state
  val state: Cod_state = new Cod_state(distance_type, k)

  override def new_slide(points: scala.Iterable[Data_basis], window_end: Long, window_start: Long): Unit = {
    val transformed = points.map(p => new Data_cod(p))
    transformed
      .foreach { p =>
            state.tree.add(p)
            state.hashMap.+=((p.id, p))
          }

    //Get neighbors
    transformed
      .foreach(p => {
        val tmpData = p
        val query: MTree[Data_cod]#Query = state.tree.getNearestByRange(tmpData, R)
        val iter = query.iterator()
        while (iter.hasNext) {
          val node = iter.next().data
          if (node.id != tmpData.id && node.arrival >= window_start) { //Needed because tree does not remove correctly some elements
            if (node.arrival < (window_end - slide)) {
              if (tmpData.flag == 0 || check_functionality(tmpData, window_end)) {
                state.hashMap(tmpData.id).insert_nn_before(node.arrival, k)
              }
              if (node.flag == 0 || check_functionality(node, window_end)) {
                state.hashMap(node.id).count_after += 1
                if (state.hashMap(node.id).count_after >= k) {
                  state.hashMap(node.id).node_type = NodeType.SAFE_INLIER
                }else if(state.hashMap(node.id).node_type == NodeType.OUTLIER){
                  //Check if it becomes inlier and update Q too
                  if(state.hashMap(node.id).count_after + state.hashMap(node.id).nn_before.count(_ >= window_start) >= k) {
                    state.hashMap(node.id).node_type = NodeType.INLIER
                    val tmin = state.hashMap(node.id).get_min_nn_before(window_start)
                    if (tmin != 0L) {
                      state.eventQ.add(new Cod_EventQElement(node.id, tmin))
                    }
                  }
                }
              }
            } else {
              if (tmpData.flag == 0 || check_functionality(tmpData, window_end)) {
                state.hashMap(tmpData.id).count_after += 1
              }
            }
          }
        }
        //Update new data point's status
        if(tmpData.flag == 0 || check_functionality(tmpData, window_end)){
          if (state.hashMap(tmpData.id).count_after >= k)
            state.hashMap(tmpData.id).node_type = NodeType.SAFE_INLIER
          else if(state.hashMap(tmpData.id).count_after + state.hashMap(tmpData.id).nn_before.count(_ >= window_start) < k)
            state.hashMap(tmpData.id).node_type = NodeType.OUTLIER
          else{
            state.hashMap(tmpData.id).node_type = NodeType.INLIER
            //Add it to event Q
            val tmin = state.hashMap(tmpData.id).get_min_nn_before(window_start)
            if (tmin != 0L) {
              state.eventQ.add(new Cod_EventQElement(tmpData.id, tmin))
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

    //Get nodes from eventQ that need rechecking
    val expiringSlide = window_start + slide
    val recheckList = mutable.Set[Int]()
    var topQ = if(state.eventQ.nonEmpty) state.eventQ.min else null
    while (topQ != null && topQ.time < expiringSlide) {
      state.eventQ.remove(topQ)
      recheckList += topQ.id
      topQ = if(state.eventQ.nonEmpty) state.eventQ.min else null
    }
    //Recheck the nodes
    recheckList.foreach{ r =>
      val el = state.hashMap.getOrElse(r, null)
      if (el != null) {
        if (el.node_type == NodeType.INLIER) {
          if (el.count_after + el.nn_before.count(_ >= expiringSlide) >= k) {
            val newTime = el.get_min_nn_before(expiringSlide)
            if (newTime != 0L)
              state.eventQ.add(new Cod_EventQElement(r, newTime))
          } else {
            el.node_type = NodeType.OUTLIER
          }
        }
      }
    }
  }

  override def assess_outliers(window_end: Long, window_start: Long): mutable.HashMap[Query, ListBuffer[Int]] = {
    //Variable for number of outliers
    val outliers: ListBuffer[Int] = state.hashMap.filter(p => check_functionality(p._2, window_end) && p._2.node_type == NodeType.OUTLIER).keys.to[ListBuffer]
    mutable.HashMap[Query, ListBuffer[Int]](query -> outliers)
  }



}
