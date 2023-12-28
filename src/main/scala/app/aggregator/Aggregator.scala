package app.aggregator

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD._

/**
 * Class for computing the aggregates
 *
 * @param sc The Spark context for the given application
 */
class Aggregator(sc: SparkContext) extends Serializable {

  private val hashPartitions = 100
  private val partitioner: HashPartitioner = new HashPartitioner(hashPartitions)
  private var titlesbyID: RDD[(Int, (List[String], String))] = _
  private var averagedRatingsById:
    RDD[(Int, (Double, List[String], String, Int))] = _

  /**
   * Use the initial ratings and titles to compute the average rating for each title.
   * The average rating for unrated titles is 0.0
   *
   * @param ratings The RDD of ratings in the file
   *                format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   * @param title   The RDD of titles in the file
   */
  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            title: RDD[(Int, String, List[String])]
          ): Unit = {
    titlesbyID = title.map { case (id, title, genres) => (id, (genres, title)) }
    averagedRatingsById = ratings
      .map { case (uid, tid, _, r, t) => ((tid, uid), (r, t)) }
      .reduceByKey(partitioner, (a, b) => if (a._2 > b._2) a else b)
      .map { case ((tid, _), (r, _)) => (tid, r) }
      .groupByKey(partitioner).mapValues(x => (x.sum / x.size, x.size))
      .rightOuterJoin(titlesbyID, partitioner)
      .mapValues {
        case (Some((avg, count)), (genres, title)) => (avg, genres, title, count)
        case (None, (genres, title)) => (0.0, genres, title, 0)
      }.persist()


  }

  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titles and ratings
   */
  def getResult(): RDD[(String, Double)] = averagedRatingsById.map(x => (x._2._3, x._2._1))

  /**
   * Compute the average rating across all (rated titles) that contain the
   * given keywords.
   *
   * @param keywords A list of keywords. The aggregate is computed across
   *                 titles that contain all the given keywords
   * @return The average rating for the given keywords. Return 0.0 if no
   *         such titles are rated and -1.0 if no such titles exist.
   */
  def getKeywordQueryResult(keywords: List[String]): Double = {
    val avgList = averagedRatingsById.map(x => (x._2._2, x._2._1))
      .filter(x => keywords.forall(x._1.contains))
    if (avgList.isEmpty()) -1.0
    else {
      val nonZero = avgList.map(x => x._2).filter(_ != 0.0)
      nonZero.sum() / nonZero.count()
    }
  }

  /**
   * Use the "delta"-ratings to incrementally maintain the aggregate ratings
   *
   * @param delta Delta ratings that haven't been included previously in aggregates
   *              format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   */
  def updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)]): Unit = {

    averagedRatingsById = sc.parallelize(delta_)
      .map { case (uid, tid, r_old, r_new, t) => ((uid, tid), (r_old, r_new, t)) }
      .reduceByKey(partitioner, (a, b) => if (a._2 > b._2) a else b) // Keep latest rating by (userid,titleID)
      .map {
        case ((_, tid), (Some(r_old), r_new, _)) => (tid, (r_old, r_new, 0))
        case ((_, tid), (None, r_new, _)) => (tid, (0.0, r_new, 1))
      }
      .reduceByKey {
        case ((r_old1, r_new1, c1), (r_old2, r_new2, c2)) => (r_old1 + r_old2, r_new1 + r_new2, c1 + c2)
      }
      .join(titlesbyID)
      .fullOuterJoin(averagedRatingsById)
      .mapValues {
        case (Some(((r_old, r_new, c_new), _)), Some((avg, genres, title, c_tot))) =>
          ((avg * c_tot - r_old + r_new) / (c_new + c_tot), genres, title, c_new + c_tot)
        case (Some(((_, r_new, count), (genres, title))), None) => (r_new / count, genres, title, count)
        case (None, Some(a)) => a
      }.persist()


  }
}
