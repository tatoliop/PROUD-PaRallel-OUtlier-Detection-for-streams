package partitioning

import models.Data_basis

import scala.collection.mutable.ListBuffer

object Grid {

  private val delimiter = ";"
  //Hardcoded cell borders for 16 partitions
  private val spatial_tao: Map[Int, String] = Map[Int, String](1 -> "-0.01", 2 -> "79.23;82.47;85.77", 3 -> "26.932")
  private val spatial_stock: Map[Int, String] = Map[Int, String](
    1 -> "87.231;94.222;96.5;97.633;98.5;99.25;99.897;100.37;101.16;102.13;103.18;104.25;105.25;106.65;109.75")

  def grid_partitioning(partitions: Int,
                       point: Data_basis,
                       range: Double,
                       dataset: String): ListBuffer[(Int, Data_basis)] = {

    val res: (Int, ListBuffer[Int]) = dataset match {
      case "STK" => findPartSTOCK(point.value, range)
      case "TAO" => findPartTAO(point.value, range)
    }
    val belongs_to = res._1
    val neighbors = res._2
    var list = new ListBuffer[(Int, Data_basis)]
    list.+=((belongs_to, point))
    if (neighbors.nonEmpty) {
      neighbors.foreach(p => {
        list.+=((p, new Data_basis(point.id, point.value, point.arrival, 1)))
      })
    }
    list
  }

  //Hardcoded way to find cells for TAO
  private def findPartTAO(value: ListBuffer[Double], range: Double): (Int, ListBuffer[Int]) = {
    val points1d = spatial_tao(1).split(delimiter).map(_.toDouble).toList
    val points2d = spatial_tao(2).split(delimiter).map(_.toDouble).toList
    val points3d = spatial_tao(3).split(delimiter).map(_.toDouble).toList

    var belongs_to = ListBuffer[Int](1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)
    var neighbors = ListBuffer[Int](1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)
    //1st dimension=============================
    if (value.head <= points1d.head) { //it belongs to x1 (1,2,3,4,5,6,7,8)
      belongs_to.-=(9, 10, 11, 12, 13, 14, 15, 16)
      if (value.head >= points1d.head - range) { //belongs to x2 too
        //nothing to do
      } else { //does not belong to x2
        neighbors.-=(9, 10, 11, 12, 13, 14, 15, 16)
      }
    } else { //it belongs to x2 (9,10,11,12,13,14,15,16)
      belongs_to.-=(1, 2, 3, 4, 5, 6, 7, 8)
      if (value.head <= points1d.head + range) { //belongs to x1 too
        //nothing to do
      } else {
        //does not belong to x1
        neighbors.-=(1, 2, 3, 4, 5, 6, 7, 8)
      }
    }
    //2nd dimension=============================
    if (value(1) <= points2d.head) { //it belongs to y1 (1,5,9,13)
      belongs_to.-=(2, 6, 10, 14, 3, 7, 11, 15, 4, 8, 12, 16)
      neighbors.-=(3, 7, 11, 15, 4, 8, 12, 16) //y3 and y4 are not neighbors
      if (value(1) >= points2d.head - range) { //belongs to y2 too
        //nothing to do
      } else {
        neighbors.-=(2, 6, 10, 14)
      }
    } else if (value(1) <= points2d(1)) { //it belongs to y2 (2,6,10,14)
      belongs_to.-=(1, 5, 9, 13, 3, 7, 11, 15, 4, 8, 12, 16)
      neighbors.-=(4, 8, 12, 16) //y4 is not neighbor
      if (value(1) <= points2d.head + range) { //belongs to y1 too
        neighbors.-=(3, 7, 11, 15) //y3 is not neighbor
      } else if (value(1) >= points2d(1) - range) { //belongs to y3 too
        neighbors.-=(1, 5, 9, 13) //y1 is not neighbor
      } else {
        //y1 and y3 are not neighbors
        neighbors.-=(1, 5, 9, 13, 3, 7, 11, 15)
      }
    } else if (value(1) <= points2d(2)) { //it belongs to y3 (3,7,11,15)
      belongs_to.-=(1, 5, 9, 13, 2, 6, 10, 14, 4, 8, 12, 16)
      neighbors.-=(1, 5, 9, 13) //y1 is not neighbor
      if (value(1) <= points2d(1) + range) { //belongs to y2 too
        neighbors.-=(4, 8, 12, 16) //y4 is not neighbor
      } else if (value(1) >= points2d(2) - range) { //belongs to y4 too
        neighbors.-=(2, 6, 10, 14) //y2 is not neighbor
      } else {
        //y2 and y4 are not neighbors
        neighbors.-=(2, 6, 10, 14, 4, 8, 12, 16)
      }
    } else { //it belongs to y4 (4,8,12,16)
      belongs_to.-=(1, 5, 9, 13, 2, 6, 10, 14, 3, 7, 11, 15)
      neighbors.-=(1, 5, 9, 13, 2, 6, 10, 14) //y1 and y2 are not neighbors
      if (value(1) <= points2d(2) + range) { //belongs to y3 too
        //nothing to do
      } else { //does not belong to y3
        neighbors.-=(3, 7, 11, 15)
      }
    }
    //3rd dimension=============================
    if (value(2) <= points3d.head) { //it belongs to z1 (5,6,7,8,9,10,11,12)
      belongs_to.-=(1, 2, 3, 4, 13, 14, 15, 16)
      if (value(2) >= points3d.head - range) { //belongs to z2 too
        //nothing to do
      } else { //does not belong to z2
        neighbors.-=(1, 2, 3, 4, 13, 14, 15, 16)
      }
    } else { //it belongs to z2 (1,2,3,4,13,14,15,16)
      belongs_to.-=(5, 6, 7, 8, 9, 10, 11, 12)
      if (value(2) <= points3d.head + range) { //belongs to z1 too
        //nothing to do
      } else {
        //does not belong to z1
        neighbors.-=(5, 6, 7, 8, 9, 10, 11, 12)
      }
    }
    val partition = belongs_to.head
    neighbors.-=(partition)
    (partition, neighbors)
  }

  //Hardcoded way to find cells for STK
  private def findPartSTOCK(value: ListBuffer[Double], range: Double): (Int, ListBuffer[Int]) = {

    val points = spatial_stock(1).split(delimiter).map(_.toDouble).toList
    val parallelism = 16
    var neighbors = ListBuffer[Int]()

    var i = 0
    var break = false
    var belongs_to, previous, next = -1
    do {
      if (value.head <= points(i)) {
        belongs_to = i //belongs to the current partition
        break = true
        if (i != 0) {
          //check if it is near the previous partition
          if (value.head <= points(i - 1) + range) {
            previous = i - 1
          }
        } //check if it is near the next partition
        if (value.head >= points(i) - range) {
          next = i + 1
        }
      }
      i += 1
    } while (i <= parallelism - 2 && !break)
    if (!break) {
      // it belongs to the last partition
      belongs_to = parallelism - 1
      if (value.head <= points(parallelism - 2) + range) {
        previous = parallelism - 2
      }
    }

    val partition = belongs_to
    if (next != -1) neighbors.+=(next)
    if (previous != -1) neighbors.+=(previous)
    (partition, neighbors)
  }


}
