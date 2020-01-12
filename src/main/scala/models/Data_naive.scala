package models

import scala.collection.mutable.ListBuffer

class Data_naive(c_point: Data_basis) extends Data_basis(c_point.id, c_point.value, c_point.arrival, c_point.flag) {

  //Neighbor data
  var count_after: Int = 0
  var nn_before = ListBuffer[Long]()
  //Skip flag
  var safe_inlier: Boolean = false

  //Function to insert data as a preceding neighbor (max k neighbors)
  def insert_nn_before(el: Long, k: Int = 0): Unit = {
    if (k != 0 && nn_before.size == k) {
      val tmp = nn_before.min
      if (el > tmp) {
        nn_before.-=(tmp)
        nn_before.+=(el)
      }
    } else {
      nn_before.+=(el)
    }
  }

  //Get the minimum of preceding neighbors
  def get_min_nn_before(time: Long): Long = {
    if (nn_before.count(_ >= time) == 0) 0L
    else nn_before.filter(_ >= time).min
  }

  //Clear variables
   def clear(newMc: Int): Unit = {
    nn_before.clear()
    count_after = 0
  }

}
