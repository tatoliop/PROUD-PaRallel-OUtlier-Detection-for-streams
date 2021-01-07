package partitioning

import jvptree.VPTree
import jvptree.util.SamplingMedianDistanceThresholdSelectionStrategy
import models.Data_basis

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

object Tree_adapt {

  class Tree_partitioning(c_range: Double, c_elements: Int, c_height: List[Int], c_dataPath: String, file_delimiter: String, distance_type: String = "euclidean") extends Partitioning with Serializable {

    private val myDistance = new VP_distance(distance_type)
    private val height: Int = c_height.head
    private val necessaryRange: Double = c_range

    //Hash that holds the trees nodes with VP and threshold
    private val myHash = createHash(c_elements, c_dataPath)
    //Temp borders
    private var myTmpHash: mutable.HashMap[MyId, MyNode] = null

    override def get_partition(point: Data_basis, range: Double, temp: Boolean = false): (Int, ListBuffer[Int]) = {
      val height = myHash.keySet.maxBy(_.height).height

      val toSearch = if (temp && myTmpHash != null) myTmpHash else myHash

      var belongsTo = MyId(0, 1) //root
      val neighbors: ListBuffer[MyId] = ListBuffer()
      while (belongsTo.height <= height) {
        val newNN = ListBuffer[MyId]() //tmp list for neighbor traverse
        val node = toSearch(belongsTo)
        val distance = myDistance.getDistance(point, node.point)
        //Find the partition that the data point belongs to
        if (distance <= node.threshold) belongsTo = node.closer
        else if (distance <= node.threshold + range) newNN += node.closer
        if (distance > node.threshold) belongsTo = node.farther
        else if (distance > node.threshold - range) newNN += node.farther
        //Find the neighbor partitions
        neighbors.foreach(nn => {
          val tmpNN = toSearch(nn)
          val distance = myDistance.getDistance(point, tmpNN.point)
          //Find the partition that the data point belongs to
          if (distance <= tmpNN.threshold + range) newNN += tmpNN.closer
          if (distance > tmpNN.threshold - range) newNN += tmpNN.farther
        })
        neighbors.clear()
        neighbors.++=(newNN.distinct)
      }
      var result = new ListBuffer[Int]()
      if (neighbors.nonEmpty) {
        for (i <- neighbors.indices) {
          result.+=(neighbors(i).id)
        }
      }
      (belongsTo.id, result)
    }

    override def adapt_partition(overworked: List[Int], underworked: List[Int], range: Double, direction: Int = 1): Unit = {
      myTmpHash = mutable.HashMap()
      myTmpHash.++=(myHash)

      val changes = mutable.HashMap[MyId, Int]()
      val startHeight = myTmpHash.keys.maxBy(_.height).height
      //Start by finding overworked parents
      overworked.foreach { r =>
        val parent = if (r % 2 == 0) MyId(startHeight, r / 2) else MyId(startHeight, (r + 1) / 2)
        val change = if (r % 2 == 1) changes.getOrElse(parent, 0) - 1 else changes.getOrElse(parent, 0) + 1
        if (change == 0) changes += ((parent, 5))
        else changes += ((parent, change))
      }
      //Continue by finding underworked parents
      underworked.foreach { r =>
        val parent = if (r % 2 == 0) MyId(startHeight, r / 2) else MyId(startHeight, (r + 1) / 2)
        val change = if (r % 2 == 1) changes.getOrElse(parent, 0) + 1 else changes.getOrElse(parent, 0) - 1
        if (change == 0) changes += ((parent, -5))
        else changes += ((parent, change))
      }
      //Iteration over changes
      while (changes.nonEmpty) {
        val tmpChanges = mutable.HashMap[MyId, Int]() //Temp list for parents
        val currentHeight = changes.keys.head.height
        if (currentHeight == 0) { //This is the root
          val node = myTmpHash(MyId(0,1))
          val change = if (changes.head._2 % 2 == 0) changes.head._2 / 2 else changes.head._2
          if (change == 5) {
            //Both kids are overworked
            //Probably nothing to do here
          } else if (change == -5) {
            //Both of the kids are underworked
            //Probably nothing to do here
          } else {
            //Change the node --- no need to go to node's parent
            if (node.threshold + change * range > necessaryRange)
              myTmpHash.update(MyId(0,1), MyNode(node.threshold + change * range, node.point, node.closer, node.farther, node.parent))
          }
        } else { //Below the root
          changes.foreach { r =>
            val node = myTmpHash(r._1)
            val parent = node.parent
            val change = if (r._2 % 2 == 0) r._2 / 2 else r._2
            if (change == 5) { //Both kids are overworked so we need to go higher
              //First change the closer kid
              if (node.threshold - range > necessaryRange)
                myTmpHash.update(r._1, MyNode(node.threshold - range, node.point, node.closer, node.farther, node.parent))
              //Then insert the parent for change
              val newChange = if (r._1.id % 2 == 1) tmpChanges.getOrElse(parent, 0) - 1 else tmpChanges.getOrElse(parent, 0) + 1
              if (newChange == 0) tmpChanges += ((parent, 5))
              else tmpChanges += ((parent, newChange))
            } else if (change == -5) { //Both kids are underworked
              //First change the closer kid
              myTmpHash.update(r._1, MyNode(node.threshold + range, node.point, node.closer, node.farther, node.parent))
              //Then insert the parent for change
              val newChange = if (r._1.id % 2 == 1) tmpChanges.getOrElse(parent, 0) + 1 else tmpChanges.getOrElse(parent, 0) - 1
              if (newChange == 0) tmpChanges += ((parent, -5))
              else tmpChanges += ((parent, newChange))
            } else { //Only one kid is problematic so change the threshold
              if (node.threshold + change * range > necessaryRange)
                myTmpHash.update(r._1, MyNode(node.threshold + change * range, node.point, node.closer, node.farther, node.parent))
            }
          }
        }
        changes.clear()
        changes ++= tmpChanges
      }
    }

    override def reverse(): Unit = {
      if (myTmpHash != null) {
        myHash.clear()
        myHash.++=(myTmpHash)
        myTmpHash = null
      }
    }

    override def get_list_partitions(): List[Int] = {
      val height = myHash.keySet.maxBy(_.height).height
      myHash.filter(_._1.height == height).flatMap(r => List(r._2.closer.id, r._2.farther.id)).toList
    }

    private def createHash(elements: Int, path: String): mutable.HashMap[MyId, MyNode] = {
      val delimiter = ";"
      //Create tree
      val my_vp_tree = createVPtree(path, elements)
      //Get thresholds
      val thresholds = my_vp_tree.getThresholds(height - 1, delimiter)
      //Get VPs
      val vps = my_vp_tree.getVPs(height - 1, delimiter)
      //Create hashmap
      val myHash: mutable.HashMap[MyId, MyNode] = new mutable.HashMap()
      thresholds.keySet().asScala.foreach(key => { //Thresholds and vps should have the same keys
        //Get info
        val threshold: Double = if(thresholds.get(key) > necessaryRange) thresholds.get(key) else necessaryRange
        val vp = vps.get(key).asInstanceOf[Data_basis]
        val splitKey = key.split(delimiter)
        val tmp_height = splitKey(0).toInt
        val id = splitKey(1).toInt
        //Get parent if not root
        val parent = if (tmp_height == 0) {
          null
        } else {
          val parent_id = if (id % 2 == 0) id / 2 else (id + 1) / 2
          MyId(tmp_height - 1, parent_id)
        }
        //Get children if available
        val (closer, farther) = if (tmp_height >= height) {
          (null, null)
        } else {
          val farther_id = id * 2
          val closer_id = farther_id - 1
          (MyId(tmp_height + 1, closer_id), MyId(tmp_height + 1, farther_id))
        }
        myHash.put(MyId(tmp_height, id), MyNode(threshold, vp, closer, farther, parent))
      })
      myHash
    }

    private def createVPtree(datapath: String, elements: Int): VPTree[Data_basis, Data_basis] = {
      val myFile = Source.fromFile(datapath)
      val mySample: Iterator[String] = myFile.getLines()
      val finalSample =
        mySample
          .take(elements)
          .toList
          .map(p => {
            val value = p.split(file_delimiter).to[ListBuffer].map(_.toDouble)
            new Data_basis(0, value, 0L, 0)
          })
          .asJava
      myFile.close()
      new VPTree[Data_basis, Data_basis](myDistance, new SamplingMedianDistanceThresholdSelectionStrategy[Data_basis, Data_basis](elements), finalSample)
    }

  }

  private class VP_distance(distance_type: String) extends jvptree.DistanceFunction[Data_basis] with Serializable {
    override def getDistance(xs: Data_basis, ys: Data_basis): Double = {
      utils.Helpers.distance(xs.value.toArray, ys.value.toArray, distance_type)
    }
  }

  private case class MyNode(threshold: Double, point: Data_basis, closer: MyId, farther: MyId, parent: MyId)

  private case class MyId(height: Int, id: Int)

}
