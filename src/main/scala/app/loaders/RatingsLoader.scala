package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


import scala.util.Try

/**
 * Helper class for loading the input
 *
 * @param sc   The Spark context for the given application
 * @param path The path for the input file
 */
class RatingsLoader(sc: SparkContext, path: String) extends Serializable {

  /**
   * Read the rating file in the given path and convert it into an RDD
   *
   * @return The RDD for the given ratings
   */
  def load(): RDD[(Int, Int, Option[Double], Double, Int)] =
    sc.textFile(getClass.getResource(path).getPath).map(line => {
      val col = line.split("\\|")
      if (col.length == 4)
        (Try(col(0).toInt).getOrElse(-1),
          Try(col(1).toInt).getOrElse(-1),
          None:Option[Double],
          Try(col(2).toDouble).getOrElse(-1.0:Double),
          Try(col(3).toInt).getOrElse(-1))
      else if(col.length == 5)
        (Try(col(0).toInt).getOrElse(-1),
          Try(col(1).toInt).getOrElse(-1),
          Option(Try(col(2).toDouble).getOrElse(-1:Double)),
          Try(col(3).toDouble).getOrElse(-1:Double),
          Try(col(4).toInt).getOrElse(-1))
      else (-1,-1,None:Option[Double],-1.0:Double,-1)

    }).persist()
}