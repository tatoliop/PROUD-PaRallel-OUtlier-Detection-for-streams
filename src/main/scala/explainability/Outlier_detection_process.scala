package explainability

import explainability.algorithms.{Dode, Pmcod, Explain, ExplainNet, Pmcsky_rk}
import models.Data_basis
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import utils.Utils.Query
import utils.traits.ExplainOutlierDetection

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class Outlier_detection_process(c_query: ListBuffer[Query], c_gcd_slide: Int, algo: String, c_distance_type: String, c_explain_input: Int = 3, c_output: OutputTag[(Long, String)] = null, c_cost: Int = 0) extends ProcessWindowFunction[(List[Int], Int, Data_basis), (List[Int], Long, Query, ListBuffer[(Int,Int)]), (List[Int],Int), TimeWindow] {

  //Metric vars
  @transient private var counter_replicas: Counter = _
  @transient private var counter_non_replicas: Counter = _
  @transient private var counter: Counter = _
  @transient private var counter_explanations: Counter = _
  @transient private var cpu_time: Long = 0L

  val queries: ListBuffer[Query] = c_query
  val gcd_slide: Int = c_gcd_slide
  val distance_type: String = c_distance_type

  //Adaptive vars
  val outputTag: OutputTag[(Long, String)] = c_output
  val costFunction: Int = c_cost
  //State var
  lazy val outlier_detection: ValueState[ExplainOutlierDetection] = getRuntimeContext.getState(new ValueStateDescriptor[ExplainOutlierDetection]("OutlierDetection", classOf[ExplainOutlierDetection]))

  //Explainability
  val explain_input = c_explain_input


  override def process(key: (List[Int],Int), context: Context, elements: Iterable[(List[Int], Int, Data_basis)], out: Collector[(List[Int], Long, Query, ListBuffer[(Int,Int)])]): Unit = {

    val window = context.window
    //Initiate state with OD algorithm
    if(outlier_detection.value() == null) {
      val tmp_outlier_detection = algo match {
        case "pmcod" => new Pmcod(queries.head, distance_type) //Single query algorithm
        case "pmcsky_rk" => new Pmcsky_rk(queries, distance_type, gcd_slide) //rk query algorithm
          //Explainability algorithms
        case "dode" => new Dode(queries.head, distance_type) //DUMMY ALGORITHM FOR TESTING PURPOSES
        case "explain" => new Explain(queries.head, distance_type, explain_input) //Single query algo for explanations
        case "explainNet" => new ExplainNet(queries.head, distance_type, explain_input) //Single query algo for explanations
      }
      outlier_detection.update(tmp_outlier_detection)
    }

    //Metrics
    counter.inc()
    counter_replicas.inc(elements.count(el => el._3.flag == 1 && el._3.arrival >= window.getEnd - gcd_slide))
    counter_non_replicas.inc(elements.count(el => el._3.flag == 0 && el._3.arrival >= window.getEnd - gcd_slide))
    val time_init = System.currentTimeMillis()

    //Process new slide
    outlier_detection.value().new_slide(elements.filter(_._3.arrival>= window.getEnd - gcd_slide).map(_._3),window.getEnd, window.getStart)
    //Get list of outliers
    val (outliers: mutable.HashMap[Query,ListBuffer[(Int,Int)]], explanations: Int) = outlier_detection.value().assess_outliers(window.getEnd, window.getStart)
    //Return outliers
    outliers.foreach(r => {
      val q = r._1
      q.outliers = r._2.size
      out.collect(key._1, window.getEnd, q, r._2)
    })
    //Process old slide
    outlier_detection.value().old_slide(elements.filter(_._3.arrival < window.getStart + gcd_slide).map(_._3.id),window.getEnd, window.getStart)

    //Metrics
    val time_final = System.currentTimeMillis()
    cpu_time += (time_final - time_init)
    counter_explanations.inc(explanations)

    //Cost functions for adaptation
    if(outputTag != null) {
      val cost = costFunction match {
        case 1 => //Just the non-replicas
          elements.count(_._3.flag == 0)
        case 2 => //Process time
          time_final - time_init
        case 3 => //Non-replicas + process time
          (time_final - time_init) * elements.count(_._3.flag == 0)
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

    counter_explanations= getRuntimeContext
      .getMetricGroup
      .counter("MyExplanationCounter")

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
