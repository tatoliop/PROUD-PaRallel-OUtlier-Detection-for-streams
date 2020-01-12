package models

import scala.collection.mutable.ListBuffer

class Data_basis(c_id: Int, c_val: ListBuffer[Double], c_arrival: Long, c_flag: Int) extends Serializable {

  val id: Int = c_id
  val value: ListBuffer[Double] = c_val
  val dimensions: Int = value.length
  val arrival: Long = c_arrival
  val flag: Int = c_flag
  val state: Seq[ListBuffer[Double]] = Seq(value)
  val hashcode: Int = state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)

  def this(point: Data_basis){
    this(point.id, point.value, point.arrival, point.flag)
  }


  def compareTo(t: Data_basis): Int = {
    val dim = Math.min(this.dimensions, t.dimensions)
    for (i <- 0 until dim) {
      if (this.value(i) > t.value(i)) +1
      else if (this.value(i) < t.value(i)) -1
      else 0
    }
    if (this.dimensions > dim) +1
    else -1
  }

  override def toString = s"Data_basis($id, $value, $arrival, $flag)"

}