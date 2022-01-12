package models

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class Data_dode(c_point: Data_basis, c_R: Int, c_k: Int) extends Data_basis(c_point.id, c_point.value, c_point.arrival, c_point.flag, c_point.countdown) {

  //Neighbor data
  var neighbor = mutable.HashMap[Int, ListBuffer[(Int, Long)]]()
  //R-k table
  var rk_table = Array.ofDim[Boolean](c_R, c_k)
  //Skip flag
  var safe_inlier: Boolean = false

  //Clear variables
  def clear(): Unit = {
    neighbor.clear()
  }

  override def toString: String = {
    var out = s"id: $id & value: $value\n\t\t"
    for (i <- 1 to rk_table.length) out += s"|k$i\t\t"
    out += "\n"
    var count: Int = 0
    for (i <- 1 to rk_table.length) {
      count += neighbor.getOrElse(i, ListBuffer()).size
      out += s"R$i-${count}\t"
      for (y <- rk_table.indices) out += s"|${rk_table(i - 1)(y)}\t"
      out += "\n"
    }
    out
  }

}
