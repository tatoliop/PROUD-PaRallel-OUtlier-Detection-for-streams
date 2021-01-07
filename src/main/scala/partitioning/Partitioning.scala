package partitioning

import models.Data_basis

import scala.collection.mutable.ListBuffer

trait Partitioning {

  def get_partition( point: Data_basis, range: Double, temp: Boolean = false): (Int, ListBuffer[Int])
  def adapt_partition(overworked: List[Int], underworked: List[Int], range: Double, direction: Int = 0): Unit
  def reverse(): Unit
  def get_list_partitions(): List[Int]

}
