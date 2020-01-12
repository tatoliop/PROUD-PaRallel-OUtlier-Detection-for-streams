package custom_source

import scala.collection.mutable.ListBuffer
import scala.util.Random

//Class that creates random values for each dimension of the dataset
//Just like the VP-tree, it needs to read a data file in order to get stats on the dataset
class Json_source(c_dataset: String) extends Serializable {

  private val dataset: String = c_dataset
  private val jsonTemplate: String =
    """
      |{
      |"id":"{myid}",
      |"timestamp":"{mytime}",
      |"value":[{myvalues}]
      |}""".stripMargin

  private val dimensions: Int =
    if (dataset == "STK") 1
    else if (dataset == "TAO") 3
    else 0

  private val distribution: Map[Int, Map[(Int, Int), Double]] =
    if (dataset == "STK")
      Map(
        1 -> Map[(Int, Int), Double]((0, 50) -> 0.10, (50, 30) -> 0.15, (80, 20) -> 0.325, (100, 20) -> 0.325, (120, 80) -> 0.0995, (200, 9730) -> 0.0005)
      )
    else if (dataset == "TAO")
      Map(
        1 -> Map[(Int, Int), Double]((-10, 7) -> 0.0005, (-3, 2) -> 0.20, (-1, 2) -> 0.40, (1, 19) -> 0.20, (20, 20) -> 0.10, (40, 36) -> 0.0995),
        2 -> Map[(Int, Int), Double]((-10, 70) -> 0.0005, (60, 15) -> 0.15, (75, 15) -> 0.60, (90, 9) -> 0.20, (99, 3) -> 0.4995),
        3 -> Map[(Int, Int), Double]((-10, 32) -> 0.0005, (22, 3) -> 0.20, (25, 2) -> 0.25, (27, 2) -> 0.40, (29, 2) -> 0.1495)
      )
    else Map(0 -> Map[(Int, Int), Double]())
  private val rnd = new Random()
  private var counter: Long = 0

  def get_next(): String = {
    //Create random values
    val value = ListBuffer[String]()
    for (dim <- 1 to dimensions) {
      val limits = sample(distribution(dim))
      val tmp = limits._1 + rnd.nextInt(limits._2) + rnd.nextDouble()
      value += f"$tmp%1.2f"
    }
    //Update result
    val res = jsonTemplate
      .replace("{myid}", counter.toString)
      .replace("{mytime}", System.currentTimeMillis.toString)
      .replace("{myvalues}", value.mkString(","))
    counter += 1
    res
  }

  private final def sample[A](dist: Map[A, Double]): A = {
    val p = rnd.nextDouble
    val it = dist.iterator
    var accum = 0.0
    while (it.hasNext) {
      val (item, itemProb) = it.next
      accum += itemProb
      if (accum >= p)
        return item // return so that we don't have to search through the whole distribution
    }
    sys.error(f"this should never happen") // needed so it will compile
  }

}
