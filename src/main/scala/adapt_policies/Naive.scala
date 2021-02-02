package adapt_policies

import utils.traits.Adaptivity

import scala.collection.mutable

object Naive extends Adaptivity {

  override def decidePolicy(allMetrics: mutable.Map[Int, mutable.Queue[Double]], upperThreshold: Double, lowerThreshold: Double, similarityThreshold: Double, queueSize: Int): (mutable.Map[Int,Double], mutable.Map[Int,Double]) = {
    val metrics = allMetrics.map(r => (r._1, r._2.last))
    val totalCost = metrics.values.sum
    val expectedCost = totalCost / metrics.size
    //Get the overworked node
    val overworked = metrics.maxBy(_._2)
    if(overworked._2 * 100 / expectedCost >= upperThreshold) (mutable.Map[Int,Double](overworked), mutable.Map[Int,Double]())
    else (mutable.Map[Int,Double](), mutable.Map[Int,Double]())
  }

}
