package utils.states

import _root_.utils.traits.State
import models.Data_advanced
import mtree._

import scala.collection.mutable

class Advanced_state(distance: String, bound: Int) extends State {

  private val nonRandomPromotion = new PromotionFunction[Data_advanced] {
    /**
      * Chooses (promotes) a pair of objects according to some criteria that is
      * suitable for the application using the M-Tree.
      *
      * @param dataSet          The set of objects to choose a pair from.
      * @param distanceFunction A function that can be used for choosing the
      *                         promoted objects.
      * @return A pair of chosen objects.
      */
    override def process(dataSet: java.util.Set[Data_advanced], distanceFunction: DistanceFunction[_ >: Data_advanced]): utils.Pair[Data_advanced] = {
      utils.Utils.minMax[Data_advanced](dataSet)
    }
  }
  private val mySplit = new ComposedSplitFunction[Data_advanced](nonRandomPromotion, new PartitionFunctions.BalancedPartition[Data_advanced])

  val tree: MTree[Data_advanced] = if (distance == "euclidean") new MTree[Data_advanced](bound, DistanceFunctions.EUCLIDEAN, mySplit)
  else new MTree[Data_advanced](bound, DistanceFunctions.JACCARD, mySplit)
  val hashMap: mutable.HashMap[Int, Data_advanced] = new mutable.HashMap[Int, Data_advanced]()
}
