package utils

import java.lang

import models.{Data_advanced, Data_naive}
import utils.Helpers._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.evictors.Evictor
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue
import org.apache.flink.util.Collector
import org.apache.flink.streaming.connectors.influxdb.InfluxDBPoint
import scala.collection.JavaConverters._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Utils {

  case class Query(R: Double, k: Int, W: Int, S: Int, var outliers: Int)

  case class Metadata_naive(var outliers: mutable.HashMap[Int, Data_naive])

  case class Metadata_advanced(var outliers: mutable.HashMap[Int, Data_advanced])

  class GroupMetadataNaive(c_query: Query) extends ProcessWindowFunction[Data_naive, (Long, Query), Int, TimeWindow] {

    val query: Query = c_query
    val W: Int = query.W
    val slide: Int = query.S
    val R: Double = query.R
    val k: Int = query.k

    lazy val state: ValueState[Metadata_naive] = getRuntimeContext
      .getState(new ValueStateDescriptor[Metadata_naive]("metadata", classOf[Metadata_naive]))

    override def process(key: Int, context: Context, elements: scala.Iterable[Data_naive], out: Collector[(Long, Query)]): Unit = {
      val window = context.window
      var current: Metadata_naive = state.value
      if (current == null) { //populate list for the first time
        var newMap = mutable.HashMap[Int, Data_naive]()
        //all elements are new to the window so we have to combine the same ones
        //and add them to the map
        for (el <- elements) {
          val oldEl: Data_naive = newMap.getOrElse(el.id, null)
          if (oldEl == null) {
            newMap += ((el.id, el))
          } else {
            val newValue = combine_elements(oldEl, el, k)
            newMap += ((el.id, newValue))
          }
        }
        current = Metadata_naive(newMap)
      } else { //update list
        //first remove old elements and elements that are safe inliers
        var forRemoval = ListBuffer[Int]()
        for (el <- current.outliers.values) {
          if (elements.count(_.id == el.id) == 0) {
            forRemoval = forRemoval.+=(el.id)
          }
        }
        forRemoval.foreach(el => current.outliers -= el)
        //then insert or combine elements
        for (el <- elements) {
          val oldEl = current.outliers.getOrElse(el.id, null)
          if (oldEl == null) {
            current.outliers.+=((el.id, el))
          } else {
            if (el.arrival < window.getEnd - slide) {
              oldEl.count_after = el.count_after
              current.outliers += ((el.id, oldEl))
            } else {
              val newValue = combine_elements(oldEl, el, k)
              current.outliers += ((el.id, newValue))
            }
          }
        }
      }
      state.update(current)

      var outliers = 0
      for (el <- current.outliers.values) {
        val nnBefore = el.nn_before.count(_ >= window.getEnd - W)
        if (nnBefore + el.count_after < k) outliers += 1
      }
      val tmpQuery = Query(query.R,query.k,query.W,query.S,outliers)
      out.collect((window.getEnd, tmpQuery))
    }
  }

  class GroupMetadataAdvanced(c_query: Query) extends ProcessWindowFunction[Data_advanced, (Long, Query), Int, TimeWindow] {

    val query: Query = c_query
    val W: Int = query.W
    val slide: Int = query.S
    val R: Double = query.R
    val k: Int = query.k

    lazy val state: ValueState[Metadata_advanced] = getRuntimeContext
      .getState(new ValueStateDescriptor[Metadata_advanced]("metadata", classOf[Metadata_advanced]))

    override def process(key: Int, context: Context, elements: scala.Iterable[Data_advanced], out: Collector[(Long, Query)]): Unit = {
      val window = context.window
      var current: Metadata_advanced = state.value
      if (current == null) { //populate list for the first time
        var newMap = mutable.HashMap[Int, Data_advanced]()
        //all elements are new to the window so we have to combine the same ones
        //and add them to the map
        for (el <- elements) {
          val oldEl = newMap.getOrElse(el.id, null)
          val newValue = combine_new_elements(oldEl, el, k)
          newMap += ((el.id, newValue))
        }
        current = Metadata_advanced(newMap)
      } else { //update list

        //first remove old elements
        var forRemoval = ListBuffer[Int]()
        for (el <- current.outliers.values) {
          if (el.arrival < window.getEnd - W) {
            forRemoval = forRemoval.+=(el.id)
          }
        }
        forRemoval.foreach(el => current.outliers -= el)
        //then insert or combine elements
        for (el <- elements) {
          val oldEl = current.outliers.getOrElse(el.id, null)
          if (el.arrival >= window.getEnd - slide) {
            val newValue = combine_new_elements(oldEl, el, k)
            current.outliers += ((el.id, newValue))
          } else {
            if (oldEl != null) {
              val newValue = combine_old_elements(oldEl, el, k)
              current.outliers += ((el.id, newValue))
            }
          }
        }
      }
      state.update(current)

      var outliers = 0
      for (el <- current.outliers.values) {
        if (!el.safe_inlier) {
          val nnBefore = el.nn_before.count(_ >= window.getEnd - W)
          if (nnBefore + el.count_after < k) outliers += 1
        }
      }
      val tmpQuery = Query(query.R,query.k,query.W,query.S,outliers)
      out.collect((window.getEnd, tmpQuery))
    }
  }

  class Evict_before(time_slide: Int) extends Evictor[(Int, Data_naive), TimeWindow] {
    override def evictBefore(elements: lang.Iterable[TimestampedValue[(Int, Data_naive)]], size: Int, window: TimeWindow, evictorContext: Evictor.EvictorContext): Unit = {
      val iteratorEl = elements.iterator
      while (iteratorEl.hasNext) {
        val tmpNode = iteratorEl.next().getValue._2
        if (tmpNode.flag == 1 && tmpNode.arrival >= window.getStart && tmpNode.arrival < window.getEnd - time_slide) iteratorEl.remove()
      }
    }

    override def evictAfter(elements: lang.Iterable[TimestampedValue[(Int, Data_naive)]], size: Int, window: TimeWindow, evictorContext: Evictor.EvictorContext): Unit = {
    }
  }

  class PrintOutliers extends ProcessWindowFunction[(Long, Query), String, Long, TimeWindow] {
    override def process(key: Long, context: Context, elements: scala.Iterable[(Long, Query)], out: Collector[String]): Unit = {
      val group_outliers = elements
        .map(record => ((record._2.W,record._2.S,record._2.R,record._2.k), record._2.outliers))
        .foldLeft(Map[(Int,Int,Double,Int), Int]().withDefaultValue(0))((res, v) => {
          val key = v._1
          res + (key -> (res(key) + v._2))
        })
      group_outliers.foreach(record => out.collect(s"$key;${record._1};${record._2}"))
    }
  }

  class WriteOutliers extends ProcessWindowFunction[(Long, Query), InfluxDBPoint, Long, TimeWindow] {
    override def process(key: Long, context: Context, elements: scala.Iterable[(Long, Query)], out: Collector[InfluxDBPoint]): Unit = {
      val group_outliers = elements
        .map(record => ((record._2.W,record._2.S,record._2.R,record._2.k), record._2.outliers))
        .foldLeft(Map[(Int,Int,Double,Int), Int]().withDefaultValue(0))((res, v) => {
          val key = v._1
          res + (key -> (res(key) + v._2))
        })

      val measurement = "outliers"
      val timestamp = key
      group_outliers.foreach(record => {
        val tags = Map[String, String]("W"->record._1._1.toString, "S"->record._1._2.toString, "k"->record._1._4.toString, "R"->record._1._3.toString).asJava
        val fields = Map[String, Object]("Outliers"->record._2.asInstanceOf[Object]).asJava
        out.collect(new InfluxDBPoint(measurement, timestamp, tags, fields))
      })

    }
  }


}
