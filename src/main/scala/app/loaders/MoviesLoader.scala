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
class MoviesLoader(sc: SparkContext, path: String) extends Serializable {

  /**
   * Read the title file in the given path and convert it into an RDD
   *
   * @return The RDD for the given titles
   */
  def load(): RDD[(Int, String, List[String])] =
    sc.textFile(getClass.getResource(path).getPath)
      .map(line => line.split("\\|", 3))
      .map(col => (
        Try(col(0).toInt).getOrElse(-1),
        col(1).replaceAll("\"", ""),
        col(2).split("\\|").toList.map(_.replaceAll("\"", ""))))
      .persist()

}

