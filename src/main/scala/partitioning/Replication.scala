package partitioning

import models.Data_basis

import scala.collection.mutable.ListBuffer

object Replication {

  def replication_partitioning(partitions: Int,
                              point: Data_basis): ListBuffer[(Int, Data_basis)] = {
    var list = new ListBuffer[(Int, Data_basis)]
    for (i <- 0 until partitions) {
      if(point.id % partitions == i) list.+=((i, point))
      else list.+=((i, new Data_basis(point.id, point.value, point.arrival, 1)))
    }
    list
  }


}
