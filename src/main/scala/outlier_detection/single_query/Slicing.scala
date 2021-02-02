package outlier_detection.single_query

import models.{Data_basis, Data_slicing}
import utils.Utils.Query
import utils.states.Slicing_state
import utils.traits.OutlierDetection
import mtree.{ComposedSplitFunction, DistanceFunction, DistanceFunctions, MTree, PartitionFunctions, PromotionFunction, utils}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class Slicing(c_query: Query, c_distance: String) extends OutlierDetection {

  //Algorithm state
  val state: Slicing_state = new Slicing_state
  val distance_type: String = c_distance
  //OD vars
  val query: Query = c_query
  val k: Int = query.k
  val R: Double = query.R
  val slide: Int = query.S
  val outliers_trigger: Long = -1L

  override def new_slide(points: scala.Iterable[Data_basis], window_end: Long, window_start: Long): Unit = {
    if(state.triggers.isEmpty){ //It is the first time this process is called
      state.triggers.+=((outliers_trigger, mutable.Set()))
      var next_slide = window_start
      while (next_slide <= window_end - slide) {
        state.triggers.+=((next_slide, mutable.Set()))
        next_slide += slide
      }
    }else{ //Insert new trigger
      var max = state.triggers.keySet.max + slide
      while (max <= window_end - slide) {
        state.triggers.+=((max, mutable.Set[Int]()))
        max += slide
      }
    }
    //Get the new slide and create the tree
    val latest_slide = window_end - slide
    val nonRandomPromotion = new PromotionFunction[Data_slicing] {
      /**
        * Chooses (promotes) a pair of objects according to some criteria that is
        * suitable for the application using the M-Tree.
        *
        * @param dataSet          The set of objects to choose a pair from.
        * @param distanceFunction A function that can be used for choosing the
        *                         promoted objects.
        * @return A pair of chosen objects.
        */
      override def process(dataSet: java.util.Set[Data_slicing], distanceFunction: DistanceFunction[_ >: Data_slicing]): utils.Pair[Data_slicing] = {
        utils.Utils.minMax[Data_slicing](dataSet)
      }
    }
    val mySplit = new ComposedSplitFunction[Data_slicing](nonRandomPromotion, new PartitionFunctions.BalancedPartition[Data_slicing])
    val myTree = if (distance_type == "euclidean") new MTree[Data_slicing](k, DistanceFunctions.EUCLIDEAN, mySplit)
    else new MTree[Data_slicing](k, DistanceFunctions.JACCARD, mySplit)
    //Transform and insert the data points to the tree
    val transformed = points.map(p => new Data_slicing(p))
    state.elements ++= transformed
    transformed
      .foreach(p => myTree.add(p))
    state.trees.+=((latest_slide, myTree))

    //Trigger leftover slides
    val slow_triggers = state.triggers.keySet.filter(p => p < window_start && p != -1L).toList
    for (slow <- slow_triggers) {
      val slow_triggers_points = state.triggers(slow).toList
      state.elements
        .filter(p => slow_triggers_points.contains(p.id))
        .foreach(p => trigger_point(p, window_end, window_start))
      state.triggers.remove(slow)
    }

    //Insert new points
    transformed
      .filter(p => p.flag == 0 || check_functionality(p, window_end))
      .foreach(p => {
        insert_point(p, window_end, window_start)
      })
  }

  override def old_slide(points: scala.Iterable[Int],window_end: Long, window_start: Long): Unit = {
    //Trigger expiring list
    state.trees.remove(window_start)
    val triggered: List[Int] = state.triggers(window_start).toList
    state.triggers.remove(window_start)
    state.elements
      .filter(p => triggered.contains(p.id))
      .foreach(p => trigger_point(p, window_end, window_start))
    state.elements --= state.elements.filter(p => points.toList.contains(p.id))
  }

  override def assess_outliers(window_end: Long, window_start: Long): mutable.HashMap[Query, ListBuffer[Int]] = {
    //Trigger previous outliers
    val triggered_outliers = state.triggers(outliers_trigger).toList
    state.triggers(outliers_trigger).clear()
    state.elements
      .filter(p => triggered_outliers.contains(p.id))
      .foreach(p => trigger_point(p, window_end, window_start))
    val outliers: ListBuffer[Int] = state.elements.filter(p => {
      check_functionality(p, window_end) &&
        !p.safe_inlier &&
        p.count_after + p.slices_before.filter(_._1 >= window_start).values.sum < k
    })
      .map(_.id)
    mutable.HashMap[Query, ListBuffer[Int]](query -> outliers)
  }

  private def insert_point(point: Data_slicing, window_end: Long, window_start: Long): Unit = {
    var (neigh_counter, next_slide) = (0, window_end - slide)
    while (neigh_counter < k && next_slide >= window_start) { //Query each slide's MTREE
      val myTree = state.trees.getOrElse(next_slide, null)
      if (myTree != null) {
        val query: MTree[Data_slicing]#Query = myTree.getNearestByRange(point, R)
        val iter = query.iterator()
        //If it has neighbors insert it into the slide's trigger
        if (iter.hasNext)
          state.triggers(next_slide).+=(point.id)
        //Update point's metadata
        while (iter.hasNext) {
          val node = iter.next().data
          if (next_slide == window_end - slide) {
            if (node.id != point.id) {
              point.count_after += 1
              neigh_counter += 1
            }
          } else {
            point.slices_before.update(next_slide, point.slices_before.getOrElse(next_slide, 0) + 1)
            neigh_counter += 1
          }
        }
        if (next_slide == window_end - slide && neigh_counter >= k) point.safe_inlier = true
      }
      next_slide -= slide
    }
    //If it is an outlier insert into trigger list
    if (neigh_counter < k) state.triggers(outliers_trigger).+=(point.id)
  }

  private   def trigger_point(point: Data_slicing, window_end: Long, window_start: Long): Unit = {
    var next_slide = //find starting slide
      if (point.last_check != 0L) point.last_check + slide
      else get_slide(point.arrival, window_start) + slide
    //Find no of neighbors
    var neigh_counter = point.count_after +
      point.slices_before.filter(_._1 >= window_start + slide).values.sum
    while (neigh_counter < k && next_slide <= window_end - slide) {
      val myTree = state.trees.getOrElse(next_slide, null)
      if (myTree != null) {
        val query: MTree[Data_slicing]#Query = myTree.getNearestByRange(point, R)
        val iter = query.iterator()
        //Update point's metadata
        while (iter.hasNext) {
          iter.next()
          point.count_after += 1
          neigh_counter += 1
        }
        if (point.count_after >= k) point.safe_inlier = true
      }
      point.last_check = next_slide
      next_slide += slide
    }
    if (neigh_counter < k) state.triggers(outliers_trigger).+=(point.id)
  }

  def get_slide(arrival: Long, window_start: Long): Long = {
    val first = arrival - window_start
    val div = first / slide
    val int_div = div.toInt
    window_start + (int_div * slide)
  }


}
