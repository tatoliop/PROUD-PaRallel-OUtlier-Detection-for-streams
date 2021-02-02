package utils

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.influxdb.InfluxDBPoint
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object Utils {

  case class Query(R: Double, k: Int, W: Int, S: Int, var outliers: Int)

  class PrintOutliers extends ProcessWindowFunction[(Long, Query, ListBuffer[Int]), String, Long, TimeWindow] {
    override def process(key: Long, context: Context, elements: scala.Iterable[(Long, Query, ListBuffer[Int])], out: Collector[String]): Unit = {
      val group_outliers = elements
        .map(record => ((record._2.W, record._2.S, record._2.R, record._2.k), (record._2.outliers, record._3)))
        .foldLeft(Map[(Int, Int, Double, Int), (Int, ListBuffer[Int])]().withDefaultValue((0, ListBuffer())))((res, v) => {
          val key = v._1
          res + (key -> ((res(key)._1 + v._2._1, res(key)._2 ++ v._2._2)))
        })
      group_outliers.foreach(record => out.collect(s"$key;${record._1};${record._2._1}"))
    }
  }

  class PrintOutliersNo extends ProcessWindowFunction[(Long, Query, ListBuffer[Int]), String, Long, TimeWindow] {
    override def process(key: Long, context: Context, elements: scala.Iterable[(Long, Query, ListBuffer[Int])], out: Collector[String]): Unit = {
      val group_outliers = elements
        .map(record => ((record._2.W, record._2.S, record._2.R, record._2.k), record._2.outliers))
        .foldLeft(Map[(Int, Int, Double, Int), Int]().withDefaultValue(0))((res, v) => {
          val key = v._1
          res + (key -> (res(key) + v._2))
        })
      group_outliers.foreach(record => out.collect(s"$key;${record._1};${record._2}"))
    }
  }

  class WriteOutliers extends ProcessWindowFunction[(Long, Query, ListBuffer[Int]), InfluxDBPoint, Long, TimeWindow] {
    override def process(key: Long, context: Context, elements: scala.Iterable[(Long, Query, ListBuffer[Int])], out: Collector[InfluxDBPoint]): Unit = {
      val group_outliers = elements
        .map(record => ((record._2.W, record._2.S, record._2.R, record._2.k), (record._2.outliers, record._3)))
        .foldLeft(Map[(Int, Int, Double, Int), (Int, ListBuffer[Int])]().withDefaultValue((0, ListBuffer())))((res, v) => {
          val key = v._1
          res + (key -> ((res(key)._1 + v._2._1, res(key)._2 ++ v._2._2)))
        })
      val measurement = "outliers"
      val timestamp = key
      group_outliers.foreach(record => {
        val tags = Map[String, String]("W" -> record._1._1.toString, "S" -> record._1._2.toString, "k" -> record._1._4.toString, "R" -> record._1._3.toString).asJava
        val fields = Map[String, Object]("Outliers" -> record._2._1.asInstanceOf[Object], "IDs" -> record._2._2.mkString(",").asInstanceOf[Object]).asJava
        out.collect(new InfluxDBPoint(measurement, timestamp, tags, fields))
      })
    }
  }

  class WriteOutliersNo extends ProcessWindowFunction[(Long, Query, ListBuffer[Int]), InfluxDBPoint, Long, TimeWindow] {
    override def process(key: Long, context: Context, elements: scala.Iterable[(Long, Query, ListBuffer[Int])], out: Collector[InfluxDBPoint]): Unit = {
      val group_outliers = elements
        .map(record => ((record._2.W, record._2.S, record._2.R, record._2.k), record._2.outliers))
        .foldLeft(Map[(Int, Int, Double, Int), Int]().withDefaultValue(0))((res, v) => {
          val key = v._1
          res + (key -> (res(key) + v._2))
        })
      val measurement = "outliersΝο"
      val timestamp = key
      group_outliers.foreach(record => {
        val tags = Map[String, String]("W" -> record._1._1.toString, "S" -> record._1._2.toString, "k" -> record._1._4.toString, "R" -> record._1._3.toString).asJava
        val fields = Map[String, Object]("Outliers" -> record._2.asInstanceOf[Object]).asJava
        out.collect(new InfluxDBPoint(measurement, timestamp, tags, fields))
      })
    }
  }

  class WriteAdaptations extends MapFunction[(Long, String, String), InfluxDBPoint] {
    override def map(value: (Long, String, String)): InfluxDBPoint = {
      val measurement = "adaptations"
      val timestamp = value._1
      val fields = Map[String, Object]("Overload" -> value._2.asInstanceOf[Object], "Underload" -> value._3.asInstanceOf[Object]).asJava
      new InfluxDBPoint(measurement, timestamp, null, fields)
    }
  }


  class GroupCostFunction extends ProcessWindowFunction[(Long, String), String, Long, TimeWindow] {
    override def process(key: Long, context: Context, elements: scala.Iterable[(Long, String)], out: Collector[String]): Unit = {
      var result = ""
      elements.foreach(r => result += (r._2 + ";"))
      result = result.substring(0, result.length - 1)
      out.collect(s"$key;$result")
    }
  }

}
