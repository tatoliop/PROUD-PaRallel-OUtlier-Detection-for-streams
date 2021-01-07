package partitioning

import jvptree.VPTree
import models.Data_basis

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.io.Source

object Tree {

  class Tree_partitioning(elements: Int, partitions: Int, dataPath: String) extends Serializable {

    private val my_vp_tree: VPTree[Data_basis, Data_basis] = {
      val myFile = Source.fromFile(dataPath)
      val mySample: Iterator[String] = myFile.getLines()
      val finalSample =
        mySample
          .take(elements)
          .toList
          .map(p => {
            val value = p.split(",").to[ListBuffer].map(_.toDouble)
            new Data_basis(0, value, 0L, 0)
          })
          .asJava
      myFile.close()
      new VPTree[Data_basis, Data_basis](new VP_euclidean_distance, finalSample)
    }
    my_vp_tree.createPartitions(partitions)

    def tree_partitioning(partitions: Int,
                          point: Data_basis,
                          range: Double
                         ): ListBuffer[(Int, Data_basis)] = {
      val neighbors: ListBuffer[Int] = {
        val javaList = my_vp_tree.findPartitions(point, range, partitions)
          .asScala.to[ListBuffer]
        var res = ListBuffer[Int]()
        if (javaList.count(_.contains("true")) != 1) System.exit(1)
        res.+=(javaList.filter(_.contains("true")).head.split("&")(0).toInt)
        javaList
          .filter(_.contains("false"))
          .foreach(p => res.+=(p.split("&")(0).toInt))
        res
      }
      var list = new ListBuffer[(Int, Data_basis)]
      list.+=((neighbors.head, point))
      if (neighbors.size > 1) {
        for (i <- 1 until neighbors.size) {
          list.+=((neighbors(i), new Data_basis(point.id, point.value, point.arrival, 1)))
        }
      }
      list
    }
  }


  private class VP_euclidean_distance extends jvptree.DistanceFunction[Data_basis] with Serializable {
    override def getDistance(xs: Data_basis, ys: Data_basis): Double = {
      val min = Math.min(xs.dimensions, ys.dimensions)
      var value: Double = 0
      for (i <- 0 until min) {
        value += scala.math.pow(xs.value(i) - ys.value(i), 2)
      }
      val res = scala.math.sqrt(value)
      res
    }
  }

}