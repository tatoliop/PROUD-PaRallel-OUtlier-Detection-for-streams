package models

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class Data_mcsky(c_point: Data_basis) extends Data_basis(c_point.id, c_point.value, c_point.arrival, c_point.flag, c_point.countdown) {

  //Neighbor data
  var lSky = mutable.HashMap[Int, ListBuffer[(Int,Long)]]()
  //Skip flag
  var safe_inlier: Boolean = false
  //Micro-cluster data
  var mc: Int = -1

  //Clear variables
   def clear(newMc: Int): Unit = {
     lSky.clear()
     mc = newMc
  }

}
