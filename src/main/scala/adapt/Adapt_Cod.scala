package adapt

import models.Data_cod
import models.NodeType
import utils.Utils.Query
import mtree.{utils, _}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

class EventQElement(myId: Int, myTime: Long) extends Comparable[EventQElement]{
  val id: Int = myId
  val time: Long = myTime

  override def toString = s"EventQElement($id, $time)"

  override def compareTo(t: EventQElement): Int = {
    if(this.time > t.time) return 1
    else if(this.time < t.time) return -1
    else{
      if(this.id > t.id) return 1
      else if(this.id < t.id) return -1
    }
    return 0
  }
}


case class CodState(var tree: MTree[Data_cod], var hashMap: mutable.HashMap[Int, Data_cod], eventQ: mutable.TreeSet[EventQElement])



class Adapt_Cod(c_query: Query, c_output: OutputTag[(Long, String)] = null, c_cost: Int = 0, c_distance_type: String = "euclidean") extends ProcessWindowFunction[(Int, Data_cod), (Long, Query), Int, TimeWindow] {

  @transient private var counter_replicas: Counter = _
  @transient private var counter_non_replicas: Counter = _
  @transient private var counter: Counter = _
  @transient private var cpu_time: Long = 0L

  val query: Query = c_query
  val slide: Int = query.S
  val R: Double = query.R
  val k: Int = query.k
  val outputTag: OutputTag[(Long, String)] = c_output
  val costFunction: Int = c_cost
  val distance_type: String = c_distance_type

  lazy val state: ValueState[CodState] = getRuntimeContext
    .getState(new ValueStateDescriptor[CodState]("myTree", classOf[CodState]))

  override def process(key: Int, context: Context, elements: scala.Iterable[(Int, Data_cod)], out: Collector[(Long, Query)]): Unit = {

    val window = context.window

    //Metrics
    counter.inc()
    counter_replicas.inc(elements.count(el => el._2.flag == 1 && el._2.arrival >= window.getEnd - slide))
    counter_non_replicas.inc(elements.count(el => el._2.flag == 0 && el._2.arrival >= window.getEnd - slide))
    val time_init = System.currentTimeMillis()

    //populate Mtree
    var current: CodState = state.value
    if (current == null) {
      val nonRandomPromotion = new PromotionFunction[Data_cod] {
        /**
          * Chooses (promotes) a pair of objects according to some criteria that is
          * suitable for the application using the M-Tree.
          *
          * @param dataSet          The set of objects to choose a pair from.
          * @param distanceFunction A function that can be used for choosing the
          *                         promoted objects.
          * @return A pair of chosen objects.
          */
        override def process(dataSet: java.util.Set[Data_cod], distanceFunction: DistanceFunction[_ >: Data_cod]): utils.Pair[Data_cod] = {
          utils.Utils.minMax[Data_cod](dataSet)
        }
      }
      val mySplit = new ComposedSplitFunction[Data_cod](nonRandomPromotion, new PartitionFunctions.BalancedPartition[Data_cod])
      val myTree = if (distance_type == "euclidean") new MTree[Data_cod](k, DistanceFunctions.EUCLIDEAN, mySplit)
      else new MTree[Data_cod](k, DistanceFunctions.JACCARD, mySplit)
      var myHash = new mutable.HashMap[Int, Data_cod]()
      for (el <- elements) {
        myTree.add(el._2)
        myHash.+=((el._2.id, el._2))
      }
      val eventQ = mutable.TreeSet[EventQElement]()
      current = CodState(myTree, myHash, eventQ)
    } else {
      elements
        .filter(el => el._2.arrival >= window.getEnd - slide)
        .foreach(el => {
          current.tree.add(el._2)
          current.hashMap.+=((el._2.id, el._2))
        })
    }

    //Get neighbors
    elements
      .filter(p => p._2.arrival >= (window.getEnd - slide))
      .foreach(p => {
        val tmpData = p._2
        val query: MTree[Data_cod]#Query = current.tree.getNearestByRange(tmpData, R)
        val iter = query.iterator()
        while (iter.hasNext) {
          val node = iter.next().data
          if (node.id != tmpData.id && node.arrival >= window.getStart) { //Needed because tree does not remove correctly some elements
            if (node.arrival < (window.getEnd - slide)) {
              if (tmpData.flag == 0) {
                current.hashMap(tmpData.id).insert_nn_before(node.arrival, k)
              }
              if (node.flag == 0) {
                current.hashMap(node.id).count_after += 1
                if (current.hashMap(node.id).count_after >= k) {
                  current.hashMap(node.id).node_type = NodeType.SAFE_INLIER
                }else if(current.hashMap(node.id).node_type == NodeType.OUTLIER){
                  //Check if it becomes inlier and update Q too
                  if(current.hashMap(node.id).count_after + current.hashMap(node.id).nn_before.count(_ >= window.getStart) >= k) {
                    current.hashMap(node.id).node_type = NodeType.INLIER
                    val tmin = current.hashMap(node.id).get_min_nn_before(window.getStart)
                    if (tmin != 0L) {
                      current.eventQ.add(new EventQElement(node.id, tmin))
                    }
                  }
                }
              }
            } else {
              if (tmpData.flag == 0) {
                current.hashMap(tmpData.id).count_after += 1
              }
            }
          }
        }
        //Update new data point's status
        if(tmpData.flag == 0){
          if (current.hashMap(tmpData.id).count_after >= k)
            current.hashMap(tmpData.id).node_type = NodeType.SAFE_INLIER
          else if(current.hashMap(tmpData.id).count_after + current.hashMap(tmpData.id).nn_before.count(_ >= window.getStart) < k)
            current.hashMap(tmpData.id).node_type = NodeType.OUTLIER
          else{
            current.hashMap(tmpData.id).node_type = NodeType.INLIER
            //Add it to event Q
            val tmin = current.hashMap(tmpData.id).get_min_nn_before(window.getStart)
            if (tmin != 0L) {
              current.eventQ.add(new EventQElement(tmpData.id, tmin))
            }
          }
        }
      })

    //Variable for number of outliers
    val outliers = current.hashMap.values.count(p => p.flag == 0 && p.node_type == NodeType.OUTLIER)

    val tmpQuery = Query(query.R,query.k,query.W,query.S,outliers)
    out.collect((window.getEnd, tmpQuery))

    //Remove expiring objects and flagged ones from state
    elements
      .filter(el => el._2.arrival < window.getStart + slide)
      .foreach(el => {
        current.tree.remove(el._2)
        current.hashMap.-=(el._2.id)
      })

    //Get nodes from eventQ that need rechecking
    val expiringSlide = window.getStart + slide
    val recheckList = mutable.Set[Int]()
    var topQ = if(current.eventQ.nonEmpty) current.eventQ.min else null
    while (topQ != null && topQ.time < expiringSlide) {
      current.eventQ.remove(topQ)
      recheckList += topQ.id
      topQ = if(current.eventQ.nonEmpty) current.eventQ.min else null
    }
    //Recheck the nodes
    recheckList.foreach{ r =>
      val el = current.hashMap.getOrElse(r, null)
      if (el != null) {
        if (el.node_type == NodeType.INLIER) {
          if (el.count_after + el.nn_before.count(_ >= expiringSlide) >= k) {
            val newTime = el.get_min_nn_before(expiringSlide)
            if (newTime != 0L)
              current.eventQ.add(new EventQElement(r, newTime))
          } else {
            el.node_type = NodeType.OUTLIER
          }
        }
      }
    }

    //update state
    state.update(current)

    //Metrics
    val time_final = System.currentTimeMillis()
    cpu_time += (time_final - time_init)

    //Cost functions for adaptation
    if (outputTag != null) {
      val cost = costFunction match {
        case 1 => //Just the non-replicas
          elements.count(_._2.flag == 0)
        case 2 => //Process time
          time_final - time_init
        case 3 => //Non-replicas + process time
          (time_final - time_init) * elements.size
      }
      context.output(outputTag, (context.window.getEnd, s"$key;$cost"))
    }
  }

  override def open(parameters: Configuration): Unit = {
    counter = getRuntimeContext
      .getMetricGroup
      .counter("MySlideCounter")

    counter_replicas = getRuntimeContext
      .getMetricGroup
      .counter("MyReplicasCounter")

    counter_non_replicas = getRuntimeContext
      .getMetricGroup
      .counter("MyNonReplicasCounter")

    getRuntimeContext
      .getMetricGroup
      .gauge[Long, ScalaGauge[Long]]("MyTotalTime", ScalaGauge[Long]( () => cpu_time ) )

    getRuntimeContext
      .getMetricGroup
      .gauge[Double, ScalaGauge[Double]]("MyAverageTime", ScalaGauge[Double]( () => {
        if(counter.getCount == 0) cpu_time.toDouble
        else cpu_time.toDouble / counter.getCount
      } ) )
  }
}
