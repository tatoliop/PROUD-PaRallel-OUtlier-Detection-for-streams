package partitioning

import models.Data_basis

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source


object Grid_adapt {

  class Grid_partitioning(c_elements: Int, c_cuts: List[Int], c_dataPath: String, file_delimiter: String) extends Partitioning with Serializable {

    private val delimiterIn = "&"
    private val delimiterOut = " "
    private val cuts = c_cuts

    //Hash that holds the trees nodes with VP and threshold
    private val myGrid: mutable.HashMap[Int, ListBuffer[Double]] = createGrid(cuts, c_elements, c_dataPath)
    private val totalDims = myGrid.keySet.size
    //Temp borders
    private var myTmpGrid: mutable.HashMap[Int, ListBuffer[Double]] = null
    //Partition inverted index
    private var myHashcodes: mutable.HashMap[Int, String] = createHashcodes()

    override def get_partition(point: Data_basis, range: Double, temp: Boolean = false): (Int, ListBuffer[Int]) = {
      val value = point.value
      val temporary = if (temp && myTmpGrid != null) myTmpGrid else myGrid
      var resultString = ""
      var nnList = ListBuffer[String]()
      for (i <- 1 to totalDims) {
        var tmpFound = false
        val neighbors: ListBuffer[String] = ListBuffer()
        for (y <- temporary(i).indices) {
          if (!tmpFound && value(i - 1) <= temporary(i)(y)) { //We found it!
            tmpFound = true
            //Found where it belongs in the dimension
            resultString = if (i < totalDims) resultString + s"$i$delimiterIn$y$delimiterOut" else resultString + s"$i$delimiterIn$y"
            //Also add it to the neighbors
            neighbors += s"$i$delimiterIn$y"
            //Check for neighbors
            if (value(i - 1) >= temporary(i)(y) - range) neighbors += s"$i$delimiterIn${y + 1}"
            if (y != 0 && value(i - 1) <= temporary(i)(y - 1) + range) neighbors += s"$i$delimiterIn${y - 1}"
          }
        }
        if (!tmpFound) {
          resultString = if (i < totalDims) resultString + s"$i$delimiterIn${temporary(i).size}$delimiterOut" else resultString + s"$i$delimiterIn${temporary(i).size}"
          neighbors += s"$i$delimiterIn${temporary(i).size}"
          if (value(i - 1) <= temporary(i).last + range) neighbors += s"$i$delimiterIn${temporary(i).size - 1}"
        }
        //Create neighbor permutations
        if (i == 1) nnList.appendAll(neighbors)
        else nnList = for (x <- nnList; y <- neighbors) yield s"$x$delimiterOut$y"
      }
      nnList -= resultString
      (resultString.hashCode, nnList.map(_.hashCode))
    }

    override def adapt_partition(overworked: List[Int], underworked: List[Int], range: Double, direction: Int = 1): Unit = {
      myTmpGrid = mutable.HashMap[Int, ListBuffer[Double]]()
      myTmpGrid.++=(myGrid)

      val cellString = myHashcodes(overworked.head)
      val cellDims = cellString.split(delimiterOut).toList
      cellDims.foreach(r => {
        val dimensions = r.split(delimiterIn)(0).toInt
        val cellIndex = r.split(delimiterIn)(1).toInt
        val tmpList = ListBuffer[Double]()
        tmpList.appendAll(myTmpGrid(dimensions))
        if (cellIndex == 0) tmpList(0) = tmpList.head - (2 * range)
        else if (cellIndex == tmpList.size) tmpList(tmpList.size - 1) = tmpList.last + (2 * range)
        else {
          if (tmpList(cellIndex) - tmpList(cellIndex - 1) > 3 * range) {
            tmpList(cellIndex - 1) = tmpList(cellIndex - 1) + range
            tmpList(cellIndex) = tmpList(cellIndex) - range
          }
        }
        myTmpGrid.update(dimensions, tmpList)
      })
    }

    override def reverse(): Unit = {
      if (myTmpGrid != null) {
        myGrid.clear()
        myGrid.++=(myTmpGrid)
        myTmpGrid = null
      }
    }

    override def get_list_partitions(): List[Int] = {
      myHashcodes.keys.toList
    }

    private def createGrid(cuts: List[Int], elements: Int, datapath: String): mutable.HashMap[Int, ListBuffer[Double]] = {
      val myFile = Source.fromFile(datapath)
      val mySample: Iterator[String] = myFile.getLines()
      val finalSample =
        mySample
          .take(elements)
          .toArray[String]
          .map(_.split(file_delimiter).map(_.toDouble))
      myFile.close()
      //Create the grid hashmap
      val dimensionNo = finalSample.head.length
      val sampleSize = finalSample.length
      val myGrid: mutable.HashMap[Int, ListBuffer[Double]] = mutable.HashMap()
      for (i <- 1 to dimensionNo) {
        val tmpList = finalSample.map(arr => arr(i - 1)).toList.sorted
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
