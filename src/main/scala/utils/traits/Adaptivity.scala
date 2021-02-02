package utils.traits

import scala.collection.mutable

trait Adaptivity extends Serializable {

  def decidePolicy(allMetrics: mutable.Map[Int, mutable.Queue[Double]], upperThreshold: Double, lowerThreshold: Double, similarityThreshold: Double, queueSize: Int): (mutable.Map[Int,Double], mutable.Map[Int,Double])

}
