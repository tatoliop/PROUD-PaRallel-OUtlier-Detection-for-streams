package models

import mtree.DistanceFunctions.EuclideanCoordinate

class Data_advanced(c_point: Data_basis) extends Data_naive(c_point: Data_basis) with EuclideanCoordinate with Comparable[Data_advanced] with Ordered[Data_advanced]  {

  def canEqual(other: Any): Boolean = other.isInstanceOf[Data_advanced]

  override def equals(other: Any): Boolean = other match {
    case that: Data_advanced =>
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

  override def compareTo(t: Data_advanced): Int = {
    val dim = Math.min(this.dimensions, t.dimensions)
    for (i <- 0 until dim) {
      if (this.value(i) > t.value(i)) +1
      else if (this.value(i) < t.value(i)) -1
      else 0
    }
    if (this.dimensions > dim) +1
    else -1
  }

  override def compare(that: Data_advanced): Int = this.compareTo(that)
}
