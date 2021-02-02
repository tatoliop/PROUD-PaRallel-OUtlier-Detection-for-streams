package utils.states

import models.Data_mcod
import utils.traits.State

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class Pmcod_microcluster(var center: ListBuffer[Double], var points: mutable.HashMap[Int, Data_mcod])

class Pmcod_state() extends State {
  val PD = mutable.HashMap[Int, Data_mcod]()
  val MC = mutable.HashMap[Int, Pmcod_microcluster]()
}

