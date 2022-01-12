package partitioning

import models.Data_basis
import utils.traits.Partitioning

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object Grid_test {

  class Grid_test_partitioning(c_cuts: List[Int], c_sample: Array[Array[Double]]) extends Partitioning with Serializable {

    private val delimiterIn = "&"
    private val delimiterOut = " "
    private val cuts = c_cuts

    //Hash that holds the trees nodes with VP and threshold
    private val myGrid: mutable.HashMap[Int, ListBuffer[Double]] = createGrid(cuts, c_sample)
    private val totalDims = myGrid.keySet.size
    //Partition inverted index
    private var myHashcodes: mutable.HashMap[Int, String] = createHashcodes()

    override def get_partition(point: Data_basis, range: Double, temp: Boolean = false): (Int, ListBuffer[Int]) = {
      val value = point.value
      var resultString = ""
      var nnList = ListBuffer[String]()
      for (i <- 1 to totalDims) {
        var tmpFound = false
        val neighbors: ListBuffer[String] = ListBuffer()
        //If dimension does not have cuts
        if(myGrid(i).isEmpty) {
          tmpFound = true
          //Found where it belongs in the dimension
          resultString = if (i < totalDims) resultString + s"$i${delimiterIn}0$delimiterOut" else resultString + s"$i${delimiterIn}0"
          //Also add it to the neighbors
          neighbors += s"$i${delimiterIn}0"
        }
        for (y <- myGrid(i).indices) {
          if (!tmpFound && value(i - 1) <= myGrid(i)(y)) { //We found it!
            tmpFound = true
            //Found where it belongs in the dimension
            resultString = if (i < totalDims) resultString + s"$i$delimiterIn$y$delimiterOut" else resultString + s"$i$delimiterIn$y"
            //Also add it to the neighbors
            neighbors += s"$i$delimiterIn$y"
            //Check for neighbors
            if (value(i - 1) >= myGrid(i)(y) - range) neighbors += s"$i$delimiterIn${y + 1}"
            if (y != 0 && value(i - 1) <= myGrid(i)(y - 1) + range) neighbors += s"$i$delimiterIn${y - 1}"
          }
        }
        if (!tmpFound) {
          resultString = if (i < totalDims) resultString + s"$i$delimiterIn${myGrid(i).size}$delimiterOut" else resultString + s"$i$delimiterIn${myGrid(i).size}"
          neighbors += s"$i$delimiterIn${myGrid(i).size}"
          if (value(i - 1) <= myGrid(i).last + range) neighbors += s"$i$delimiterIn${myGrid(i).size - 1}"
        }
        //Create neighbor permutations
        if (i == 1) nnList.appendAll(neighbors)
        else nnList = for (x <- nnList; y <- neighbors) yield s"$x$delimiterOut$y"
      }
      nnList -= resultString
      (resultString.hashCode, nnList.map(_.hashCode))
    }

    override def adapt_partition(overworked: List[Int], underworked: List[Int], range: Double, direction: Int = 1): Unit = {
      println("Grid can't work with adaptations")
      System.exit(1)
    }

    override def reverse(): Unit = {
      println("Grid can't work with adaptations")
      System.exit(1)
    }

    override def get_list_partitions(): List[Int] = {
      myHashcodes.keys.toList
    }

    private def createGrid(cuts: List[Int], sample: Array[Array[Double]]): mutable.HashMap[Int, ListBuffer[Double]] = {
      //Create the grid hashmap
      val dimensionNo = sample.head.length
      val sampleSize = sample.length
      val myGrid: mutable.HashMap[Int, ListBuffer[Double]] = mutable.HashMap()
      for (i <- 1 to dimensionNo) {
        val tmpList = sample.map(arr => arr(i - 1)).toList.sorted
        val splitNo = cuts(i - 1)
        val split = sampleSize / splitNo
        val splitList = (for (y <- 1 until splitNo) yield tmpList(y * split)).to[ListBuffer]
        myGrid.update(i, splitList)
      }
      myGrid
    }

    private def createHashcodes(): mutable.HashMap[Int, String] = {
      //Create all permutations
      var permutations = for (x <- 0 to myGrid(1).size) yield s"1$delimiterIn$x"
      for (i <- 2 to myGrid.keySet.size) {
        val tmpList = for (x <- 0 to myGrid(i).size) yield s"$i$delimiterIn$x"
        permutations = for (x <- permutations; y <- tmpList) yield s"$x$delimiterOut$y"
      }
      //Transform permutations into integers
      val result: mutable.HashMap[Int, String] = mutable.HashMap().++=(permutations.map(r => (r.hashCode, r)))
      result
    }

  }

}
