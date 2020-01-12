package single_query

import utils.Utils.Query
import mtree.{utils, _}
import models.Data_advanced
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

case class AdvancedState(var tree: MTree[Data_advanced], var hashMap: mutable.HashMap[Int, Data_advanced])

class Advanced(c_query: Query) extends ProcessWindowFunction[(Int, Data_advanced), Data_advanced, Int, TimeWindow] {

  @transient private var counter: Counter = _
  @transient private var cpu_time: Long = 0L

  val slide: Int = c_query.S
  val R: Double = c_query.R
  val k: Int = c_query.k

  lazy val state: ValueState[AdvancedState] = getRuntimeContext
    .getState(new ValueStateDescriptor[AdvancedState]("myTree", classOf[AdvancedState]))

  override def process(key: Int, context: Context, elements: scala.Iterable[(Int, Data_advanced)], out: Collector[Data_advanced]): Unit = {

    //Metrics
    counter.inc()
    val time_init = System.currentTimeMillis()

    val window = context.window
    //populate Mtree
    var current: AdvancedState = state.value
    if (current == null) {
      val nonRandomPromotion = new PromotionFunction[Data_advanced] {
        /**
          * Chooses (promotes) a pair of objects according to some criteria that is
          * suitable for the application using the M-Tree.
          *
          * @param dataSet          The set of objects to choose a pair from.
          * @param distanceFunction A function that can be used for choosing the
          *                         promoted objects.
          * @return A pair of chosen objects.
          */
        override def process(dataSet: java.util.Set[Data_advanced], distanceFunction: DistanceFunction[_ >: Data_advanced]): utils.Pair[Data_advanced] = {
          utils.Utils.minMax[Data_advanced](dataSet)
        }
      }
      val mySplit = new ComposedSplitFunction[Data_advanced](nonRandomPromotion, new PartitionFunctions.BalancedPartition[Data_advanced])
      val myTree = new MTree[Data_advanced](k, DistanceFunctions.EUCLIDEAN, mySplit)
      var myHash = new mutable.HashMap[Int, Data_advanced]()
      for (el <- elements) {
        myTree.add(el._2)
        myHash.+=((el._2.id, el._2))
      }
      current = AdvancedState(myTree, myHash)
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
        val query: MTree[Data_advanced]#Query = current.tree.getNearestByRange(tmpData, R)
        val iter = query.iterator()
        while (iter.hasNext) {
          val node = iter.next().data
          if (node.id != tmpData.id) {
            if (node.arrival < (window.getEnd - slide)) {
              current.hashMap(tmpData.id).insert_nn_before(node.arrival, k)
              current.hashMap(node.id).count_after += 1
              if (current.hashMap(node.id).count_after >= k)
                current.hashMap(node.id).safe_inlier = true
            } else {
              if (tmpData.flag == 0) {
                current.hashMap(tmpData.id).count_after += 1
                if (current.hashMap(tmpData.id).count_after >= k)
                  current.hashMap(tmpData.id).safe_inlier = true
              }
            }
          }
        }
      })

    current.hashMap.values.foreach(p => {
      out.collect(p)
    })

    //Remove expiring objects from tree and flagged ones
    elements
      .filter(el => el._2.arrival < window.getStart + slide || el._2.flag == 1)
      .foreach(el => {
        current.tree.remove(el._2)
        current.hashMap.-=(el._2.id)
      })
    //update state
    state.update(current)

    //Metrics
    val time_final = System.currentTimeMillis()
    cpu_time += (time_final - time_init)
  }

  override def open(parameters: Configuration): Unit = {
    counter = getRuntimeContext()
      .getMetricGroup()
      .counter("MySlideCounter")

    getRuntimeContext()
      .getMetricGroup()
      .gauge[Long, ScalaGauge[Long]]("MyTotalTime", ScalaGauge[Long]( () => cpu_time ) )

    getRuntimeContext()
      .getMetricGroup()
      .gauge[Double, ScalaGauge[Double]]("MyAverageTime", ScalaGauge[Double]( () => {
        if(counter.getCount == 0) cpu_time.toDouble
        else cpu_time.toDouble / counter.getCount
      } ) )
  }

}
