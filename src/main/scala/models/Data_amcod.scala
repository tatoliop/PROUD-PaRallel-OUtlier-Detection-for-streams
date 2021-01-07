package models

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class Data_amcod(c_point: Data_basis) extends Data_basis(c_point.id, c_point.value, c_point.arrival, c_point.flag, c_point.countdown) {

  //Neighbor data
  var count_after: Int = 0
  var nn_before_set: ListBuffer[(Long, Double)] = ListBuffer[(Long,Double)]()
  var count_after_set: ListBuffer[Double] = ListBuffer[Double]()
  //Skip flag
  var safe_inlier: Boolean = false

  //Micro-cluster data
  var mc: Int = -1
  var Rmc: mutable.HashSet[Int] = mutable.HashSet[Int]()

  //Clear variables
  def clear(newMc: Int): Unit = {
    nn_before_set.clear()
    count_after_set.clear()
    count_after = 0
    mc = newMc
  }

}
