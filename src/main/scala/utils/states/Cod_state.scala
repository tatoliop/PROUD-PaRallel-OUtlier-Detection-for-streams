package utils.states

import models.Data_cod
import mtree.{ComposedSplitFunction, DistanceFunction, DistanceFunctions, MTree, PartitionFunctions, PromotionFunction, utils}
import _root_.utils.traits.State

import scala.collection.mutable

object Cod_state {

  class Cod_EventQElement(myId: Int, myTime: Long) extends Comparable[Cod_EventQElement] {
    val id: Int = myId
    val time: Long = myTime

    override def toString = s"EventQElement($id, $time)"

    override def compareTo(t: Cod_EventQElement): Int = {
      if (this.time > t.time) return 1
      else if (this.time < t.time) return -1
      else {
        if (this.id > t.id) return 1
        else if (this.id < t.id) return -1
      }
      return 0
    }
  }

  class Cod_state(distance: String, bound: Int) extends State {

    private val nonRandomPromotion = new PromotionFunction[Data_cod] {
      /**
        * Chooses (promotes) a pair of objects according to some criteria that is
        * suitable for the application using the M-Tree.
        *
        * @param dataSet          The set of objects to choose a pair from.
        * @param distanceFunction A function that can be used for choosing the
        *                         promoted objects.
        * @return A pair of chosen objects.
        */
      override def process(dataSet: java.util.Set[Data_cod], distanceFunction: DistanceFunction[_ >: Data_cod]): utils.Pair[Data_cod] = {
        utils.Utils.minMax[Data_cod](dataSet)
      }
    }
    private val mySplit = new ComposedSplitFunction[Data_cod](nonRandomPromotion, new PartitionFunctions.BalancedPartition[Data_cod])

    val tree: MTree[Data_cod] = if (distance == "euclidean") new MTree[Data_cod](bound, DistanceFunctions.EUCLIDEAN, mySplit)
    else new MTree[Data_cod](bound, DistanceFunctions.JACCARD, mySplit)

    val hashMap: mutable.HashMap[Int, Data_cod] = new mutable.HashMap[Int, Data_cod]()
    val eventQ: mutable.TreeSet[Cod_EventQElement] = new mutable.TreeSet[Cod_EventQElement]()
  }

}