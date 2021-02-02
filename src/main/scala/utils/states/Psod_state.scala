package utils.states

import models.Data_lsky
import utils.traits.State

import scala.collection.mutable

class Psod_state() extends State {
  val index = mutable.LinkedHashMap[Int, Data_lsky]()
  var slide_count: Long = 0
  var last_window: Long = 0
}

