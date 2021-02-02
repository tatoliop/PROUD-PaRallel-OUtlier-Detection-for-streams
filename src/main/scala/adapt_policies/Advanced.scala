package adapt_policies

import utils.traits.Adaptivity

import scala.collection.mutable

object Advanced extends Adaptivity {

  override def decidePolicy(allMetrics: mutable.Map[Int, mutable.Queue[Double]], upperThreshold: Double, lowerThreshold: Double, similarityThreshold: Double, queueSize: Int): (mutable.Map[Int,Double], mutable.Map[Int,Double]) = {
    //Here we check the history of the changes on te processing tasks and decide whether to update the borders or not
    val latestMetrics = allMetrics.map(r => (r._1, r._2.last))
    val totalCost = latestMetrics.values.sum
    val expectedCost = totalCost / latestMetrics.keys.size
    //Get the overworked nodes
    val overworked = latestMetrics.filter( r => r._2 * 100 / expectedCost >= upperThreshold)
    val chronicOverworked = overworked.filter { r =>
      //Get the difference with the current cost after dividing them
      val keyHistory = allMetrics(r._1).filter(e => Math.abs((e * 100 / expectedCost) - (r._2 * 100 / expectedCost)) <= similarityThreshold)
      keyHistory.size == queueSize
    }
    //Get the underworked nodes
    val underworked = latestMetrics.filter( r => r._2 * 100 / expectedCost < lowerThreshold)
    val chronicUnderworked = underworked.filter { r =>
      //Get the difference with the current cost after dividing them
      val keyHistory = allMetrics(r._1).filter(e => Math.abs((e * 100 / expectedCost) - (r._2 * 100 / expectedCost)) <= similarityThreshold)
      keyHistory.size == queueSize
    }
    (chronicOverworked,chronicUnderworked)
  }

}
