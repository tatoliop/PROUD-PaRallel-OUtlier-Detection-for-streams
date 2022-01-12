package utils

import java.io.FileWriter
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

object Helpers {

  def getSampleData(sample_size: Int, partitioning_file: String, file_delimiter: String): (Array[Array[Double]], Int) = {
    val myFile = Source.fromFile(partitioning_file)
    val mySample: Iterator[String] = myFile.getLines()
    val finalSample =
      mySample
        .take(sample_size)
        .toArray[String]
        .map(_.split(file_delimiter).map(_.toDouble))
    myFile.close()
    (finalSample, finalSample.head.length)
  }

  def getCombinations(no: Int, size: Int): List[List[Int]] = {
    val combinations =
      if (size == 3)
        (for {
          i <- 1 until no - 1
          j <- i + 1 until no
          z <- j + 1 to no
        } yield List[Int](i, j, z)).toList
      else
        (for {
          i <- 1 until no
          j <- i + 1 to no
        } yield List[Int](i, j)).toList
    combinations
  }

  //------------------- Classification block --------------------

  def writeML(el: Array[Array[Boolean]], neighbors: mutable.HashMap[Int, Int], id: Int, category: Int, dataset: String, status: Int, k: Double): Unit = {
    val filename = dataset + "_" + status + "_outliers" + ".csv"
    val fw = new FileWriter("ml_explain/data/" + filename, true)
    if (status == 1) {
      //Get NN
      val norm_nn = getNNwk(el, neighbors, k)
      //Write header
      if (id == 0) fw.write("R1;R2;R3;R4;R5;label\n")
      //Write the point
      fw.write(s"${norm_nn.mkString(";")};$category\n")
    } else if (status == 2) {
      //Prepare the array
      val outlierness = getStatusTable(el)
      //Write header
      if (id == 0) {
        for (i <- 1 to 25)
          fw.write(s"In$i;")
        fw.write("label\n")
      }
      //Write the point
      fw.write(s"${outlierness.mkString(";")};$category\n")
    } else if (status == 3) {
      //Get NN
      val norm_nn = getNNwk(el, neighbors, k)
      //Prepare the array
      val outlierness = getStatusTable(el)
      //Write header
      if (id == 0) {
        fw.write("R1;R2;R3;R4;R5;")
        for (i <- 1 to 25)
          fw.write(s"In$i;")
        fw.write("label\n")
      }
      //Write the point
      fw.write(s"${norm_nn.mkString(";")};${outlierness.mkString(";")};$category\n")
    }
    //Close
    fw.close()
  }

  def explain(el: Array[Array[Boolean]], neighbors: mutable.HashMap[Int, Int], status: Int = 1, k: Double): Int = {
    var result = 7
    if (status == 1) { //We need to take into account only the normalized NN values
      result = explainNN(el, neighbors, k)
    } else if (status == 2) { //We take into account only the status table
      result = explainStatus(el)
    } else if (status == 3) { //We take into account the combination of the normalized NN and the status table
      //Start by getting the result from the NN
      val nnResult = explainNN(el, neighbors, k)
      //Get the result from the status table
      val statusResult = explainStatus(el)
      //Combine results
      if (statusResult == nnResult) result = statusResult
      else result = getStrictCategory(statusResult, nnResult)
    }
    //Return the result
    result
  }

  private def getNNwk(el: Array[Array[Boolean]], neighbors: mutable.HashMap[Int, Int], k: Double): Array[Double] = {
    //Prepare the NN parameter
    val norm_nn = Array.ofDim[Double](el.length)
    var count = 0
    for (i <- 1 to 5) {
      count += neighbors.getOrElse(i, 0)
      norm_nn(i - 1) = count / k
    }
    norm_nn
  }

  private def getStatusTable(el: Array[Array[Boolean]]): Array[Int] = {
    //Prepare the array
    val outlierness = Array.ofDim[Int](el.length * el.length)
    var z = 0
    for (i <- el.indices) {
      for (y <- el.indices) {
        outlierness(z) = if (el(i)(y)) 1 else 0
        z += 1
      }
    }
    outlierness
  }

  private def explainNN(el: Array[Array[Boolean]], neighbors: mutable.HashMap[Int, Int], k: Double): Int = {
    var result = 7
    val nnk = getNNwk(el, neighbors, k)
    if (nnk(4) == 0) result = 0
    else if (nnk(3) == 0) result = 5
    else if (nnk(2) == 0) result = 5
    else if (nnk(1) == 0) { //Something sparse
      if (nnk(4) < 2.5) result = 2
      else result = 4
    }
    else if (nnk(2) < 1) { //Something sparse
      if (nnk(4) < 2.5) result = 2
      else result = 4
    }
    else {
      if (nnk(3) <= 2.5) result = 1
      else result = 3
    }
    result
  }

  private def explainStatus(el: Array[Array[Boolean]]): Int = {
    var result = 7
    //Preprocess table for easier lookup
    val first_outlier = mutable.HashMap[Int, Int]()
    for (range <- el.indices) { //For every row (R value) starting with smallest
      val tmp_pos = el(range).takeWhile(p => p).length //Find the position of the first outlier of the row
      first_outlier.put(range, tmp_pos)
    }
    //From ground dataset
    if (first_outlier(4) == 0) result = 0
    else if (first_outlier(3) == 0) result = 5
    else if (first_outlier(2) == 0) //Something sparse
      if (first_outlier(4) <= 3) result = 2
      else result = 4
    else if (first_outlier(1) == 0) //Something sparse or dense MC
      if (first_outlier(2) >= 2) result = 1 //Dense MC
      else if (first_outlier(4) <= 3) result = 2
      else result = 4
    else if (first_outlier(0) == 0) //Something dense
      if (first_outlier(4) < 5) result = 1
      else result = 3 //Cluster
    else result = 3 //Dense cluster

    result
  }

  private def getStrictCategory(catA: Int, catB: Int): Int = {
    val orderStrict = mutable.HashMap[Int, Int](0 -> 6, 1 -> 3, 2 -> 4, 3 -> 1, 4 -> 2, 5 -> 5, 6 -> 0, 7 -> 7)
    //Order of strictness is Fridge > Dense Cl > Sparse Cl > Dense MC > Sparse MC > Near Cl > Isolated > Unknown
    if (orderStrict.contains(catA) && orderStrict.contains(catB))
      if (orderStrict(catA) < orderStrict(catB)) catA
      else catB
    else 7
  }

  def categoryString(no: Int): String = {
    no match {
      case -1 => "Inlier"
      case 0 => "Isolated"
      case 1 => "Dense microcluster"
      case 2 => "Sparse microcluster"
      case 3 => "Dense cluster"
      case 4 => "Sparse cluster"
      case 5 => "Near cluster"
      case 6 => "Fridge point"
      case _ => "Unknown"
    }
  }

  def categoryInteger(category: String): Int = {
    category match {
      case "Inlier" => -1
      case "Isolated" => 0
      case "Dense microcluster" => 1
      case "Sparse microcluster" => 2
      case "Dense cluster" => 3
      case "Sparse cluster" => 4
      case "Near cluster" => 5
      case "Fridge point" => 6
      case _ => 7
    }
  }

  //------------------- END Classification block --------------------

  def distance(xs: Array[Double], ys: Array[Double], distance_type: String = "euclidean"): Double = {
    val res = distance_type match {
      case "euclidean" => euclidean_distance(xs, ys)
      case "jaccard" => jaccard_distance(xs, ys)
    }
    res
  }

  private def euclidean_distance(xs: Array[Double], ys: Array[Double]): Double = {
    val min = Math.min(xs.length, ys.length)
    var value: Double = 0
    for (i <- 0 until min) {
      value += scala.math.pow(xs(i) - ys(i), 2)
    }
    val res = scala.math.sqrt(value)
    res
  }

  private def jaccard_distance(xs: Array[Double], ys: Array[Double]): Double = {
    val xSet: Set[Double] = xs.toSet
    val ySet: Set[Double] = ys.toSet
    val union = xSet.union(ySet).size.toDouble
    val intersect = xSet.intersect(ySet).size
    val res = 1 - (intersect / union)
    res
  }

  @scala.annotation.tailrec
  private def gcd(a: Int, b: Int): Int = {
    if (a == 0) return b
    gcd(b % a, a)
  }

  def find_gcd(myList: ListBuffer[Int]): Int = {
    var result = myList.head
    for (i <- 1 until myList.size) {
      result = gcd(myList(i), result)
    }
    result
  }

  /**
    * Method to read environment variables
    *
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
