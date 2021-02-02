package models

import models.NodeType.{NodeType, Value}
import mtree.DistanceFunctions.EuclideanCoordinate

object NodeType extends Enumeration with Serializable {
  type NodeType = Value
  val OUTLIER, INLIER, SAFE_INLIER = Value
}

class Data_cod(c_point: Data_basis) extends Data_naive(c_point: Data_basis) with EuclideanCoordinate with Comparable[Data_cod] with Ordered[Data_cod]  {

  var node_type: NodeType = null

  def canEqual(other: Any): Boolean = other.isInstanceOf[Data_cod]

  override def equals(other: Any): Boolean = other match {
    case that: Data_cod =>
      this.value.size == that.value.size &&
        this.value == that.value &&
        this.id == that.id
    case _ => false
  }

  override def hashCode(): Int = {
    hashcode
  }

  /**
    * A method to access the {@code index}-th component of the coordinate.
    *
    * @param index The index of the component. Must be less than { @link
    *              #dimensions()}.
    */
  override def get(index: Int): Double = value(index)

  override   def compareTo(t: Data_cod): Int = {
    var res = 0
    val dim = Math.min(this.dimensions, t.dimensions)
    for (i <- 0 until dim) {
      if (this.value(i) > t.value(i)) res+=1
      else if (this.value(i) < t.value(i)) res-=1
      else res+=0
    }
    if (this.dimensions > dim) res+=1
    else if(t.dimensions > dim)res-=1
    if(res >= 0) 1
    else -1
  }

  override def compare(that: Data_cod): Int = this.compareTo(that)

  override def contains(coord: Double): Boolean = value.contains(coord)
}
