package utils.states

import utils.traits.State

import scala.collection.mutable

class Explain_state() extends State {

  val netChange: mutable.Map[Int, (Int, Int)] = mutable.HashMap[Int, (Int,Int)]()

  def resetNet(id: Int, category: Int = -1): Unit = {
    netChange.update(id, (category,0))
  }

  def addNet(id: Int, count: Int = 1): Unit = {
    netChange.update(id, (netChange(id)._1, netChange(id)._2 + count))
  }

  def reduceNet(id: Int, count: Int = 1): Unit = {
    netChange.update(id, (netChange(id)._1, netChange(id)._2 - count))
  }

}

