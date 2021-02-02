package main_job

import models.Data_basis
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import outlier_detection.rk_query.{Amcod, Pmcsky_rk, Psod_rk, Sop_rk}
import outlier_detection.rkws_query.{Pmcsky_rkws, Psod_rkws, Sop_rkws}
import outlier_detection.single_query.{Advanced, Cod, Pmcod, Pmcod_Net, Slicing}
import utils.Utils.Query
import utils.traits.OutlierDetection

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class Outlier_detection_process(c_query: ListBuffer[Query], c_gcd_slide: Int, algo: String, c_distance_type: String, c_output: OutputTag[(Long, String)] = null, c_cost: Int = 0) extends ProcessWindowFunction[(Int, Data_basis), (Long, Query, ListBuffer[Int]), Int, TimeWindow] {

  //Metric vars
  @transient private var counter_replicas: Counter = _
  @transient private var counter_non_replicas: Counter = _
  @transient private var counter: Counter = _
  @transient private var cpu_time: Long = 0L

  val queries: ListBuffer[Query] = c_query
  val gcd_slide: Int = c_gcd_slide
  val distance_type: String = c_distance_type

  //Adaptive vars
  val outputTag: OutputTag[(Long, String)] = c_output
  val costFunction: Int = c_cost
  //State var
  lazy val outlier_detection: ValueState[OutlierDetection] = getRuntimeContext.getState(new ValueStateDescriptor[OutlierDetection]("OutlierDetection", classOf[OutlierDetection]))

  override def process(key: Int, context: Context, elements: Iterable[(Int, Data_basis)], out: Collector[(Long, Query, ListBuffer[Int])]): Unit = {

    val window = context.window
    //Initiate state with OD algorithm
    if(outlier_detection.value() == null) {
      val tmp_outlier_detection = algo match {
        case "advanced" => new Advanced(queries.head, distance_type) //Single query algorithm
        case "cod" => new Cod(queries.head, distance_type) //Single query algorithm
        case "slicing" => new Slicing(queries.head, distance_type) //Single query algorithm
        case "pmcod" => new Pmcod(queries.head, distance_type) //Single query algorithm
        case "pmcod_net" => new Pmcod_Net(queries.head, distance_type) //Single query algorithm
        case "amcod_rk" => new Amcod(queries, distance_type, gcd_slide) //rk query algorithm
        case "psod_rk" => new Psod_rk(queries, distance_type, gcd_slide) //rk query algorithm
        case "sop_rk" => new Sop_rk(queries, distance_type, gcd_slide) //rk query algorithm
        case "pmcsky_rk" => new Pmcsky_rk(queries, distance_type, gcd_slide) //rk query algorithm
        case "psod_rkws" => new Psod_rkws(queries, distance_type, gcd_slide) //rkws query algorithm
        case "sop_rkws" => new Sop_rkws(queries, distance_type, gcd_slide) //rkws query algorithm
        case "pmcsky_rkws" => new Pmcsky_rkws(queries, distance_type, gcd_slide) //rkws query algorithm
      }
      outlier_detection.update(tmp_outlier_detection)
    }

    //Metrics
    counter.inc()
    counter_replicas.inc(elements.count(el => el._2.flag == 1 && el._2.arrival >= window.getEnd - gcd_slide))
    counter_non_replicas.inc(elements.count(el => el._2.flag == 0 && el._2.arrival >= window.getEnd - gcd_slide))
    val time_init = System.currentTimeMillis()

    //Process new slide
    outlier_detection.value().new_slide(elements.filter(_._2.arrival>= window.getEnd - gcd_slide).map(_._2),window.getEnd, window.getStart)
    //Get list of outliers
    val outliers: mutable.HashMap[Query,ListBuffer[Int]] = outlier_detection.value().assess_outliers(window.getEnd, window.getStart)
    //Return outliers
    outliers.foreach(r => {
      val q = r._1
      q.outliers = r._2.size
      out.collect(window.getEnd, q, r._2)
    })
    //Process old slide
    outlier_detection.value().old_slide(elements.filter(_._2.arrival < window.getStart + gcd_slide).map(_._2.id),window.getEnd, window.getStart)

    //Metrics
    val time_final = System.currentTimeMillis()
    cpu_time += (time_final - time_init)

    //Cost functions for adaptation
    if(outputTag != null) {
      val cost = costFunction match {
        case 1 => //Just the non-replicas
          elements.count(_._2.flag == 0)
        case 2 => //Process time
          time_final - time_init
        case 3 => //Non-replicas + process time
          (time_final - time_init) * elements.count(_._2.flag == 0)
      }
      context.output(outputTag, (context.window.getEnd, s"$key;$cost"))
    }
  }

  override def open(parameters: Configuration): Unit = {
    counter= getRuntimeContext
      .getMetricGroup
      .counter("MySlideCounter")

    counter_replicas= getRuntimeContext
      .getMetricGroup
      .counter("MyReplicasCounter")

    counter_non_replicas= getRuntimeContext
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
