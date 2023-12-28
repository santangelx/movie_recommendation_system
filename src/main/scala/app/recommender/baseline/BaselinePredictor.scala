package app.recommender.baseline

import org.apache.spark.rdd.RDD

class BaselinePredictor() extends Serializable {

  private var averageRatingByUser: RDD[(Int, Double)] = _
  private var globalAverageDeviationByMovie: RDD[(Int, Double)] = _

  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {
    //ratingsRDD = [userID | movieID | r0 | r1 | timestamp]
    averageRatingByUser = ratingsRDD
      .map { case (uid, _, _, r, _) => (uid, (r, 1)) }
      .reduceByKey { case (a, b) => (a._1 + b._1, a._2 + b._2) }
      .mapValues { case (sum, count) => sum / count }
      .persist()

    // Pre-process ratings by user to see how much they differ from average by user.
    globalAverageDeviationByMovie = ratingsRDD
      .map { case (uid, movieID, _, r, _) => (uid, (movieID, r)) }
      .join(averageRatingByUser)
      .map { case (_, ((movieID, r), av)) => (movieID, ((r - av) / scale(r, av), 1)) }
      .reduceByKey { case (a, b) => (a._1 + b._1, a._2 + b._2) }
      .mapValues { case (sum, count) => sum / count }

  }

  def predict(userId: Int, movieId: Int): Double = {
    val userAverageRating = averageRatingByUser.lookup(userId).headOption
    val movieDeviation = globalAverageDeviationByMovie.lookup(movieId).headOption

    (userAverageRating, movieDeviation) match {
      case (None, _) => averageRatingByUser.map(_._2).sum() / averageRatingByUser.count() // If user has no rating, use global average
      case (_, None) => userAverageRating.get // If movie has no rating, use user's average rating
      case (Some(r_u), Some(r_m)) => r_u + r_m * scale(r_u + r_m, r_u) // Compute the prediction using both user's average and movie's deviation
    }


  }

  private def scale(x: Double, av: Double): Double = {
    if (x > av) 5 - av
    else if (x < av) av - 1
    else 1
  }
}

