package single_query

import utils.Utils.Query
import utils.Helpers.distance
import models.Data_naive
import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class Naive(c_query: Query) extends ProcessWindowFunction[(Int, Data_naive), Data_naive, Int, TimeWindow] {

  @transient private var counter: Counter = _
  @transient private var cpu_time: Long = 0L

  val slide: Int = c_query.S
  val R: Double = c_query.R
  val k: Int = c_query.k

  override def process(key: Int, context: Context, elements: scala.Iterable[(Int, Data_naive)], out: Collector[Data_naive]): Unit = {

    //Metrics
    counter.inc()
    val time_init = System.currentTimeMillis()

    val window = context.window
    val inputList = elements.map(_._2).toList

    inputList.filter(_.arrival >= window.getEnd - slide).foreach(p => {
      refreshList(p, inputList, context.window)
    })
    inputList.foreach(p => {
      if (!p.safe_inlier) {
        out.collect(p)
      }
    })

    //Metrics
    val time_final = System.currentTimeMillis()
    cpu_time += (time_final - time_init)

  }

  def refreshList(node: Data_naive, nodes: List[Data_naive], window: TimeWindow): Unit = {
    if (nodes.nonEmpty) {
      val neighbors = nodes
        .filter(_.id != node.id)
        .map(x => (x, distance(x.value.toArray, node.value.toArray)))
        .filter(_._2 <= R).map(_._1)

      neighbors
        .foreach(x => {
          if (x.arrival < window.getEnd - slide) {
            node.insert_nn_before(x.arrival, k)
          } else {
            node.count_after += 1
            if (node.count_after >= k) {
              node.safe_inlier = true
            }
          }
        })

      nodes
        .filter(x => x.arrival < window.getEnd - slide && neighbors.contains(x))
        .foreach(n => {
          n.count_after += 1
          if (n.count_after >= k) {
            n.safe_inlier = true
          }
        }) //add new neighbor to previous nodes
    }
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