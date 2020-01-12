package models

import scala.collection.mutable

class Data_mcod(c_point: Data_basis) extends Data_naive(c_point) {

  //Micro-cluster data
  var mc: Int = -1
  var Rmc = mutable.HashSet[Int]()

  //Clear variables
  override def clear(newMc: Int): Unit = {
    nn_before.clear()
    count_after = 0
    mc = newMc
  }

}
