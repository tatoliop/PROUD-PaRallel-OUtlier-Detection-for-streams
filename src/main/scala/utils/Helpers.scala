package utils

import models.{Data_advanced, Data_naive}
import scala.collection.mutable.ListBuffer

object Helpers {

  private val distance_type = "euclidean"

  def distance(xs: Array[Double], ys: Array[Double]): Double ={
    val res = distance_type match {
      case "euclidean" => euclidean_distance(xs, ys)
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

  def combine_elements(el1: Data_naive, el2: Data_naive, k: Int): Data_naive = {
    for (elem <- el2.nn_before) {
      el1.insert_nn_before(elem, k)
    }
    el1
  }

  def combine_new_elements(el1: Data_advanced, el2: Data_advanced, k: Int): Data_advanced = {
    if (el1 == null) {
      el2
    } else if (el2 == null) {
      el1
    } else if (el1.flag == 1 && el2.flag == 1) {
      for (elem <- el2.nn_before) {
        el1.insert_nn_before(elem, k)
      }
      el1
    } else if (el2.flag == 0) {
      for (elem <- el1.nn_before) {
        el2.insert_nn_before(elem, k)
      }
      el2
    } else if (el1.flag == 0) {
      for (elem <- el2.nn_before) {
        el1.insert_nn_before(elem, k)
      }
      el1
    } else {
      null
    }
  }

  def combine_old_elements(el1: Data_advanced, el2: Data_advanced, k: Int): Data_advanced = {
    el1.count_after = el2.count_after
    el1.safe_inlier = el2.safe_inlier
    el1
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

}
