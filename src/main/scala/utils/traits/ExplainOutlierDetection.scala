package utils.traits

import models.Data_basis
import utils.Utils.Query

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

trait ExplainOutlierDetection extends Serializable {

  def new_slide(points: scala.Iterable[Data_basis], window_end: Long, window_start: Long): Unit

  def old_slide(points: scala.Iterable[Int], window_end: Long, window_start: Long): Unit

  def assess_outliers(window_end: Long, window_start: Long): (mutable.HashMap[Query, ListBuffer[(Int,Int)]], Int)

  protected def check_functionality(point: Data_basis, window_end: Long): Boolean = {
    if((point.countdown >= window_end && point.flag == 1) || (point.countdown < window_end && point.flag == 0)) true
    else false
  }

}
