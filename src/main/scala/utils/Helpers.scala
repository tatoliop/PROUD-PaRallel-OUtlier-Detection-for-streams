package utils

import scala.collection.mutable.ListBuffer

object Helpers {

  def distance(xs: Array[Double], ys: Array[Double], distance_type: String = "euclidean"): Double ={
    val res = distance_type match {
      case "euclidean" => euclidean_distance(xs, ys)
      case "jaccard" => jaccard_distance(xs, ys)
    }
    res
  }

  private def euclidean_distance(xs: Array[Double], ys: Array[Double]): Double ={
    val min = Math.min(xs.length, ys.length)
    var value: Double = 0
    for (i <- 0 until min) {
      value += scala.math.pow(xs(i) - ys(i), 2)
    }
    val res = scala.math.sqrt(value)
    res
  }

  private def jaccard_distance(xs: Array[Double], ys: Array[Double]): Double ={
    val xSet: Set[Double] = xs.toSet
    val ySet: Set[Double] = ys.toSet
    val union = xSet.union(ySet).size.toDouble
    val intersect = xSet.intersect(ySet).size
    val res = 1 - (intersect / union)
    res
  }

  @scala.annotation.tailrec
  private def gcd(a: Int, b: Int): Int= {
    if(a==0) return b
    gcd(b%a,a)
  }

  def find_gcd(myList: ListBuffer[Int]): Int = {
    var result = myList.head
    for (i <- 1 until myList.size){
      result = gcd(myList(i), result)
    }
    result
  }

  /**
    * Method to read environment variables
    * @param key The key of the variable
    * @return The variable
    */
  @throws[NullPointerException]
  def readEnvVariable(key: String): String = {
    val envVariable = System.getenv(key)
    if (envVariable == null) throw new NullPointerException("Error! Environment variable " + key + " is missing")
    envVariable
  }

  def IsPowerOfTwo(x: Int): Boolean = (x & (x - 1)) == 0

}
