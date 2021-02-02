package main_job

import models.Data_basis
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector
import utils.traits.Partitioning
import adapt_policies.{Advanced, Naive}
import org.apache.flink.streaming.api.scala.OutputTag

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

//Case class for metadata needed on the broadcast state
case class MetadataAdaptation(bufferSlide: Long, transientSlide: Long, stabilizationSlide: Long, history: mutable.Map[Int, mutable.Queue[Double]])

class Adaptive_partitioning_process(c_technique: String, c_win: Int, c_slide: Int, c_r: Double, c_myPartitioner: Partitioning, c_adapt_range: Double, c_adapt_queue: Int, c_adapt_over: Double, c_adapt_under: Double, c_sleep: Double, c_output: OutputTag[(Long, String, String)]) extends BroadcastProcessFunction[Data_basis, String, (Int, Data_basis)] {

  //Metric for adapts
  @transient private var counter: Counter = _

  lazy val controlStateDescriptor = new MapStateDescriptor[String, MetadataAdaptation](
    "ControlStream",
    BasicTypeInfo.STRING_TYPE_INFO,
    TypeInformation.of(new TypeHint[MetadataAdaptation]() {})
  )
  //Technique
  val technique: String = c_technique //naive, advanced
  //Process broadcast element function variables
  val queueSize: Int = c_adapt_queue //Queue size for the metric history. Take into account only the last X slides (queue space) for the cost function
  val window: Int = c_win //For the adaptation periods
  val slide: Int = c_slide //For the adaptation periods
  val adaptation: Double = c_adapt_range //For the adaptation border range
  val sleep_duration: Double = c_sleep //For the buffer period
  //Cost function threshold
  val upperThreshold: Double = c_adapt_over //Every key above X percentage from the expected cost is considered overworked
  val lowerThreshold: Double = c_adapt_under //Every key below X percentage from the expected cost is considered under-worked
  val similarityThreshold: Double = 5 // The threshold of similarity when comparing a value with its historical values
  //Process element function variables
  val range: Double = c_r
  val partitioner: Partitioning = c_myPartitioner
  var reverse_borders = false
  //Output for adaptation decisions
  val outputTag: OutputTag[(Long, String, String)] = c_output

  override def processElement(value: Data_basis, ctx: BroadcastProcessFunction[Data_basis, String, (Int, Data_basis)]#ReadOnlyContext, out: Collector[(Int, Data_basis)]): Unit = {
    val control = ctx.getBroadcastState(controlStateDescriptor)
    if (control.contains("change")) { //Adapt to changes
      val start_time = control.get("change").bufferSlide
      val end_time = control.get("change").transientSlide
      if (value.arrival < start_time) { //Still an old element, process with the old method
        val (belongs, neigh) = partitioner.get_partition(value, range)
        out.collect(belongs, value)
        if (neigh.nonEmpty) neigh.foreach(part => out.collect(part, new Data_basis(value.id, value.value, value.arrival, 1)))
      } else if (value.arrival < end_time) { //For arrivals inside the change period
        //Check where the point belongs with the new borders
        val (belongs, neigh) = partitioner.get_partition(value, range, temp = true)
        //Check where the point belongs with the old borders
        val (oldBelongs, oldNeigh) = partitioner.get_partition(value, range)
        if (belongs == oldBelongs) { //Send it to the partition it belongs to and to the partitions from the most number of neighbors
          out.collect(belongs, value)
          val neighbors: ListBuffer[Int] = (neigh ++ oldNeigh).distinct
          neighbors.foreach(r => if (r != belongs && r != oldBelongs) out.collect(r, new Data_basis(value.id, value.value, value.arrival, 1)))
          //if (neigh.nonEmpty && neigh.size >= oldNeigh.size) neigh.foreach(part => out.collect(part, new Data_basis(value.id, value.value, value.arrival, 1)))
          //else if (oldNeigh.nonEmpty && oldNeigh.size >= neigh.size) oldNeigh.foreach(part => out.collect(part, new Data_basis(value.id, value.value, value.arrival, 1)))
        } else { //It is between the gap from the change of borders
          //Get neighbors
          val neighbors: ListBuffer[Int] = (neigh ++ oldNeigh).distinct
          out.collect(belongs, new Data_basis(value.id, value.value, value.arrival, 0, end_time))
          out.collect(oldBelongs, new Data_basis(value.id, value.value, value.arrival, 1, end_time))
          neighbors.foreach(r => if (r != belongs && r != oldBelongs) out.collect(r, new Data_basis(value.id, value.value, value.arrival, 1)))
        }
      } else { //After the end time and during sleep
        if (reverse_borders) {
          partitioner.reverse()
          reverse_borders = false
        }
        val (belongs, neigh) = partitioner.get_partition(value, range)
        out.collect(belongs, value)
        if (neigh.nonEmpty) neigh.foreach(part => out.collect(part, new Data_basis(value.id, value.value, value.arrival, 1)))
      }
    } else { //Normal flow
      if (reverse_borders) {
        partitioner.reverse()
        reverse_borders = false
      }
      val (belongs, neigh) = partitioner.get_partition(value, range)
      out.collect(belongs, value)
      if (neigh.nonEmpty) neigh.foreach(part => out.collect(part, new Data_basis(value.id, value.value, value.arrival, 1)))
    }
  }

  override def processBroadcastElement(value: String, ctx: BroadcastProcessFunction[Data_basis, String, (Int, Data_basis)]#Context, out: Collector[(Int, Data_basis)]): Unit = {
    val control = ctx.getBroadcastState(controlStateDescriptor)
    //Get metrics
    val (time_slide, metrics) = extractMetrics(value)
    //Get history metrics
    val allMetrics = if(control.contains("history")) {
      control.get("history").history
    }
    else {
      val metametrics = metrics.map(r => (r._1, mutable.Queue[Double](r._2)))
      val res = MetadataAdaptation(0, 0, 0, metametrics)
      control.put("history", res)
      control.get("history").history
    }
    //Update metric history
    metrics.foreach { r =>
      if(allMetrics(r._1).nonEmpty && allMetrics(r._1).size == queueSize) allMetrics(r._1).dequeue()
      allMetrics(r._1).enqueue(r._2)
    }
    //Check adaptivity conditions
    if (control.contains("change")) { //If state is changing
      val splitted = value.split(";")
      val cur_time = splitted(0).toLong
      val sleep_time = control.get("change").stabilizationSlide
      if (cur_time >= sleep_time) { //Free the state
        control.remove("change")
      }
    }else{ //Run policy depending on technique
      //Get overloaded/underloaded keys based on policy
      val (overload, underload) = technique match {
        case "advanced" => Advanced.decidePolicy(allMetrics, upperThreshold, lowerThreshold, similarityThreshold, queueSize) //Advanced policy
        case "naive" => Naive.decidePolicy(allMetrics, upperThreshold, lowerThreshold, similarityThreshold, queueSize) //Advanced policy
      }
      //If adaptation is required
      if(overload.nonEmpty || underload.nonEmpty){
        //val buffer = time_slide + window
        val buffer: Long = (time_slide + sleep_duration * window).toLong
        val transient: Long = buffer + window
        val stabilization: Long = buffer + window
        //Change borders
        partitioner.adapt_partition(overload.keys.toList, underload.keys.toList, adaptation * range)
        reverse_borders = true
        val res = MetadataAdaptation(buffer, transient, stabilization, null)
        control.put("change", res)
        val over = if(overload.keys.nonEmpty) overload.keys.mkString(",") else "-"
        val under = if(underload.keys.nonEmpty) underload.keys.mkString(",") else "-"
        ctx.output(outputTag, (buffer, over, under))
        //Metrics
        counter.inc()
      }
    }
  }

  private def extractMetrics(value: String): (Long, mutable.Map[Int, Double]) = {
    val keys = partitioner.get_list_partitions()
    val splitted = value.split(";")
    val time_slide = splitted(0).toLong
    val metrics = (for (i: Int <- 1 until splitted.length - 1 by 2) yield (splitted(i).toInt, splitted(i + 1).toDouble)).toMap
    val res: mutable.Map[Int, Double] = mutable.Map[Int, Double]()
    for (i <- keys) {
      if (metrics.contains(i)) res.put(i, metrics(i))
      else res.put(i, 0)
    }
    (time_slide, res)
  }

  override def open(parameters: Configuration): Unit = {
    counter= getRuntimeContext
      .getMetricGroup
      .counter("MyAdaptCounter")
  }

}
