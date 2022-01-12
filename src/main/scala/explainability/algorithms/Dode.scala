package explainability.algorithms

import models.{Data_basis, Data_dode}
import utils.Helpers.{distance, explain, writeML}
import utils.Utils.Query
import utils.states.Dode_state
import utils.traits.ExplainOutlierDetection

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

//DUMMY ALGORITHM for testing purposes
class Dode(c_query: Query, c_distance: String) extends ExplainOutlierDetection {

  val state: Dode_state = new Dode_state
  val distribution: List[Double] = List(0.25, 0.5, 1, 2, 4)
  //OD vars
  val query: Query = c_query
  val k: Int = query.k
  val R: Double = query.R
  val slide: Int = query.S
  val distance_type: String = c_distance
  //Multi-params vars
  val R_distinct_list: List[Double] = for (distr <- distribution) yield R * distr
  val k_distinct_list: List[Double] = for (distr <- distribution) yield k * distr
  val R_max: Double = R_distinct_list.max
  val R_min: Double = R_distinct_list.min
  val R_size: Int = R_distinct_list.size
  val k_max: Double = k_distinct_list.max
  val k_min: Double = k_distinct_list.min
  val k_size: Int = k_distinct_list.size

  override def new_slide(points: scala.Iterable[Data_basis], window_end: Long, window_start: Long): Unit = {
    //Insert new elements to state
    points
      .map(p => new Data_dode(p, R_size, k_size))
      //Sort is needed when each point has a different timestamp
      //In our case every point in the same slide has the same timestamp
      .toList
      .sortBy(_.arrival)
      .foreach(p => state.index += ((p.id, p)))
  }

  override def old_slide(points: scala.Iterable[Int], window_end: Long, window_start: Long): Unit = {
    //Remove points
    points.foreach { p =>
      //Remove old points
      deletePoint(state.index(p))
    }
  }

  override def assess_outliers(window_end: Long, window_start: Long): (mutable.HashMap[Query, ListBuffer[(Int, Int)]], Int) = {

    var debug_string_out = ""
    var debug_string_all = ""
    var outlier_category_f1 = ""
    var outlier_truth_f1 = ""

    //Hardcoded variables for input type and dataset name
    val status = 2
    //val dataset = "custom_debug"
    //val dataset = "custom_big"
    //val dataset = "custom_medium"
    //val dataset = "custom_small"
    val dataset = "ground"

    val outliers = ListBuffer[(Int, Int)]()
    //Update elements
    state.index.values.foreach(p => {
      checkPoint(p, window_end, window_start)
      if (check_functionality(p, window_end)) {
        var i, y: Int = 0
        var count = p.neighbor.getOrElse(i + 1, ListBuffer()).count(_._2 >= window_start)
        do {
          if (count >= k_distinct_list(y)) { //inlier for all y
            for (z <- i until R_size) p.rk_table(z)(y) = true
            y += 1
          } else { //outlier for all i
            for (z <- y until k_size) p.rk_table(i)(z) = false
            i += 1
            count += p.neighbor.getOrElse(i + 1, ListBuffer()).count(_._2 >= window_start)
          }
        } while (i < R_size && y < k_size)
        //Get true category
        val trueCategory: Int = getTrueCategory(p.id, dataset)
        //Get rules category
        val category = explain(p.rk_table, p.neighbor.map(el => (el._1, el._2.size)), status, k)
        //Store confusion matrix for every point
        //state.predictions(trueCategory)(category) += 1
        //Output outlier
        if (!p.rk_table(2)(2)) {
          //Store confusion matrix for outliers
          outliers += ((p.id, category))
          writeML(p.rk_table, p.neighbor.map(el => (el._1, el._2.size)), p.id, trueCategory, dataset, status, k)
          debug_string_out += s"${p.value.mkString(",")},${category}\n"
          outlier_category_f1 += s"$category, "
          outlier_truth_f1 += s"$trueCategory, "
        }
        state.predictions(trueCategory)(category) += 1
        debug_string_all += s"${p.value.mkString(",")},${category}\n"

        //Write inputs for use in the classification models of python
        //writeML(p.rk_table, p.neighbor.map(el => (el._1, el._2.size)), p.id, trueCategory, dataset, status, k)
      }
    })


    //Print confusion matrix
    var out = ""
    out += "True/Prediction\t\t"
    for (z <- 0 to 5) {
      out += s"|Predict: ${z}\t\t\t"
    }
    out += "\n"
    for (z <- 0 to 5) {
      out += s"True: ${z}\t\t\t\t"
      for (i <- 0 to 5) {
        out += s"|${state.predictions(z)(i)}\t\t\t\t"
      }
      out += "\n"
    }
    println(out)
    println(outlier_category_f1)
    println(outlier_truth_f1)

    /*
    //Write meta for glass
    import java.io._
    val pw = new PrintWriter(new File("glass_full_" + R + "-" + k + "_" + status + ".csv"))
    pw.write(debug_string_all)
    pw.close()
    val pw2 = new PrintWriter(new File("glass_outliers_" + R + "-" + k + "_" + status + ".csv"))
    pw2.write(debug_string_out)
    pw2.close()
    */
    //Output results
    (mutable.HashMap[Query, ListBuffer[(Int, Int)]](query -> outliers), 0)
  }

  //Ground truth for annotated datasets
  private def getTrueCategory(id: Int, dataset: String): Int = {
    if (dataset == "custom_debug") {
      if (id <= 3) 0
      else if (id <= 63) 1
      else if (id <= 123) 2
      else if (id <= 323) 3
      else if (id <= 523) 4
      else 7
    } else if (dataset == "custom_medium") {
      if (id <= 2) 0
      else if (id <= 3) 5
      else if (id <= 23) 1
      else if (id <= 43) 2
      else if (id <= 243) 3
      else if (id <= 443) 4
      else 7
    } else if (dataset == "custom_small") {
      if (id <= 1) 0
      else if (id <= 4) 1
      else if (id <= 5) 5
      else if (id <= 21) 3
      else if (id <= 57) 4
      else 7
    } else if (dataset == "custom_big") {
      if (id <= 0) 0
      else if (id <= 2) 5
      else if (id <= 3) 0
      else if (id <= 203) 1
      else if (id <= 403) 2
      else if (id <= 2403) 3
      else if (id <= 4403) 4
      else 7
    } else if (dataset == "ground") {
      if (id <= 3) 0
      else if (id <= 7) 5
      else if (id <= 37) 1
      else if (id <= 67) 2
      else if (id <= 367) 3
      else if (id <= 667) 4
      else 7
    }
    else 7

  }

  private def checkPoint(el: Data_dode, window_end: Long, window_start: Long): Unit = {
    if (el.neighbor.isEmpty) { //It's a new point!
      insertPoint(el)
    } else { //It's an old point
      updatePoint(el, window_end, window_start)
    }
  }

  private def insertPoint(el: Data_dode): Unit = {
    state.index.values.toList.reverse //get the points so far from latest to earliest
      .foreach(p => {
        if (p.id != el.id) {
          val thisDistance = distance(el.value.toArray, p.value.toArray, distance_type)
          if (thisDistance <= R_max) {
            val norm_dist = normalizeDistance(thisDistance)
            el.neighbor.update(norm_dist, el.neighbor.getOrElse(norm_dist, ListBuffer[(Int, Long)]()) += ((p.id, p.arrival)))
          }
        }
      })
  }

  private def updatePoint(el: Data_dode, window_end: Long, window_start: Long): Unit = {
    //Remove old points from lSky
    el.neighbor.keySet.foreach(p => el.neighbor.update(p, el.neighbor(p).filter(_._2 >= window_start)))
    //Check against newer points
    state.index.values.toList.reverse //Check new points
      .takeWhile(p => {
        var tmpRes = true
        if (p.arrival >= window_end - slide) {
          val thisDistance = distance(el.value.toArray, p.value.toArray, distance_type)
          if (thisDistance <= R_max) {
            val norm_dist = normalizeDistance(thisDistance)
            el.neighbor.update(norm_dist, el.neighbor.getOrElse(norm_dist, ListBuffer[(Int, Long)]()) += ((p.id, p.arrival)))
          }
        } else tmpRes = false
        tmpRes
      })
  }

  private def deletePoint(el: Data_dode): Unit = {
    state.index.remove(el.id)
  }

  private def normalizeDistance(distance: Double): Int = {
    var res, i = 0
    do {
      if (distance <= R_distinct_list(i)) res = i + 1
      i += 1
    } while (i < R_distinct_list.size && res == 0)
    res
  }

}

/*        //|INFO
          //|CUSTOM_DEBUG --DEBUG true --policy static --algorithm dode --W 524 --S 524 --dataset EXPLAIN/custom --partitioning grid --partitions 1;1 --R 18 --k 10
          //|==========|================|==================|===========|
          //|ID        |TYPE            |CENTRE            |VARIANCE   |
          //|==========|================|==================|===========|
          //|0-3       |ISOLATED        |edges (0-1000)    |           |
          //|4-63      |DENSE MC        |(200,200)         |50         |
          //|64-123    |SPARSE MC       |(200,700)         |150        |
          //|124-323   |DENSE CLUSTER   |(700,200)         |50         |
          //|324-523   |SPARSE CLUSTER  |(700,700)         |150        |
          //|==========|================|==================|===========|

          //|CUSTOM_MEDIUM --DEBUG true --policy static --algorithm dode --W 444 --S 444 --dataset EXPLAIN/custom_hardcoded --partitioning grid --partitions 1;1 --R 50 --k 10
          //|==========|=====================|==================|===========|
          //|ID        |TYPE                 |CENTRE            |VARIANCE   |
          //|==========|=====================|==================|===========|
          //|0-2       |ISOLATED             |                  |           |
          //|3         |NEAR DENSE CLUSTER   |                  |           |
          //|4-23      |DENSE MC             |                  |           |
          //|24-43     |SPARSE MC            |                  |           |
          //|44-243    |DENSE CLUSTER        |                  |           |
          //|244-443   |SPARSE CLUSTER       |                  |           |
          //|==========|=====================|==================|===========|

          //|CUSTOM_SMALL --DEBUG true --policy static --algorithm dode --W 58 --S 58 --dataset EXPLAIN/custom_small_hardcoded --partitioning grid --partitions 1;1 --R 15 --k 2
          //|==========|=====================|==================|===========|
          //|ID        |TYPE                 |CENTRE            |VARIANCE   |
          //|==========|=====================|==================|===========|
          //|0-1       |ISOLATED             |                  |           |
          //|2-4       |DENSE MC             |                  |           |
          //|5         |NEAR DENSE CLUSTER   |                  |           |
          //|6-21      |DENSE CLUSTER(16)    |                  |           |
          //|22-57     |SPARSE CLUSTER(36)   |                  |           |
          //|==========|=====================|==================|===========|

          //|CUSTOM_BIG --DEBUG true --policy static --algorithm dode --W 4404 --S 4404 --dataset EXPLAIN/custom_big --partitioning grid --partitions 1;1 --R 700 --k 100
          //|==========|================|==================|===========|
          //|ID        |TYPE            |CENTRE            |VARIANCE   |
          //|==========|================|==================|===========|
          //|0         |ISOLATED        |                  |           |
          //|1-2       |NEAR CL (?)     |                  |           |
          //|3         |ISOLATED        |                  |           |
          //|4-203     |DENSE MC        |                  |2000       |
          //|204-403   |SPARSE MC       |                  |4000       |
          //|404-2403  |DENSE CLUSTER   |                  |2000       |
          //|2404-4403 |SPARSE CLUSTER  |                  |4000       |
          //|==========|================|==================|===========|

          //|GROUND --DEBUG true --policy static --algorithm dode --W 668 --S 668 --dataset EXPLAIN/ground --partitioning grid --partitions 1;1 --R 50 --k 15
          //|==========|================|==================|===========|
          //|ID        |TYPE            |CENTRE            |VARIANCE   |
          //|==========|================|==================|===========|
          //|0-3       |ISOLATED        |                  |           |
          //|4-7       |NEAR CL         |                  |           |
          //|8-37      |DENSE MC        |                  |2000       |
          //|38-67     |SPARSE MC       |                  |4000       |
          //|68-367    |DENSE CLUSTER   |                  |2000       |
          //|368-667   |SPARSE CLUSTER  |                  |4000       |
          //|==========|================|==================|===========|
*/