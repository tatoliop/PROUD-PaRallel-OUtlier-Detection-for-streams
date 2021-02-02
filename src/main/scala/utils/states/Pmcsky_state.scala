package utils.states

import models.Data_mcsky
import utils.traits.State

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class Pmcsky_microcluster(var center: ListBuffer[Double], var points: ListBuffer[Int])

class Pmcsky_state() extends State {
  val index = mutable.LinkedHashMap[Int, Data_mcsky]()
  val MC = mutable.HashMap[Int, Pmcsky_microcluster]()
  val PD = mutable.HashSet[Int]()
  var slide_count: Long = 0
  var cluster_id: Int = 1
}

