package utils.states

import models.Data_amcod
import utils.traits.State

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class Amcod_microcluster(var center: ListBuffer[Double], var points: mutable.HashMap[Int, Data_amcod])

class Amcod_state() extends State {
  val PD = mutable.HashMap[Int, Data_amcod]()
  val MC = mutable.HashMap[Int, Amcod_microcluster]()
  var cluster_id: Int = 1
}

