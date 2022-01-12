package utils.states

import models.Data_dode
import utils.traits.State

import scala.collection.mutable

class Dode_state() extends State {
  val index = mutable.LinkedHashMap[Int, Data_dode]()

  val predictions = Array.fill[Int](8,8)(0)

  var rk_stats = mutable.HashMap[Int, Array[Array[Int]]]()
  for (z <- 0 to 6) {
    val tmpArray = Array.ofDim[Int](5,5)
    for (i <- 0 to 4) {
      for (y <- 0 to 4) {
        tmpArray(i)(y) = 0
      }
    }
    rk_stats.put(z,tmpArray)
  }
}

