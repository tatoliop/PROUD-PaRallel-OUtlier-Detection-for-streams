package utils.states

import models.Data_slicing
import mtree.MTree
import utils.traits.State

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class Slicing_state() extends State {
  val trees: mutable.HashMap[Long, MTree[Data_slicing]] = mutable.HashMap()
  var triggers: mutable.HashMap[Long, mutable.Set[Int]] = mutable.HashMap()
  val elements: ListBuffer[Data_slicing] = ListBuffer()
}

