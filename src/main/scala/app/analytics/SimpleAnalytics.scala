package app.analytics

import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import java.time.{Instant, ZonedDateTime, ZoneOffset}

class SimpleAnalytics() extends Serializable {

  private val hashPartitions = 100
  private val ratingsPartitioner: HashPartitioner = new HashPartitioner(hashPartitions)
  private val moviesPartitioner: HashPartitioner = new HashPartitioner(hashPartitions)

  private var titlesGroupedById:
    RDD[(Int, Iterable[(Int, String, List[String])])] = _
  private var ratingsGroupedByYearByTitle:
    RDD[(Int, Iterable[(Int, List[(Int, Int, Option[Double], Double, Int)])])] = _

  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            movie: RDD[(Int, String, List[String])]
          ): Unit = {
    titlesGroupedById = movie.map(m => (m._1, m))
      .groupByKey(moviesPartitioner).persist()
    ratingsGroupedByYearByTitle = ratings
      .map {
        case (user, movie, r1, r2, time) =>
          ((ZonedDateTime.ofInstant(Instant.ofEpochSecond(time), ZoneOffset.UTC).getYear, movie),
            List((user, movie, r1, r2, time)))
      }
      .reduceByKey(ratingsPartitioner,_ ++ _)
      .map {
        case ((year, movie), values) => (year, (movie, values))
      }
      .groupByKey(ratingsPartitioner)
      .persist()
  }

  def getNumberOfMoviesRatedEachYear: RDD[(Int, Int)] = ratingsGroupedByYearByTitle
    .map {
      case (year, ratingsByMovie) => (year, ratingsByMovie.size)
    }

  def getMostRatedMovieEachYear: RDD[(Int, String)] = ratingsGroupedByYearByTitle
    .map {
      case (year, ratingsByMovie) => (year,
        ratingsByMovie.map { case (movieID, ratings) => (movieID, ratings.size) }
          .reduce((a, b) =>
            if (a._2 == b._2)
              if (a._1 > b._1) a else b
            else if (a._2 > b._2) a else b)
      )
    }.map { case (year, (id, _)) => (id, year) }
    .join(titlesGroupedById)
    .map { case (_, (year, values)) => (year, values.head._2) }

  def getMostRatedGenreEachYear: RDD[(Int, List[String])] = ratingsGroupedByYearByTitle
    .map {
      case (year, ratingsByMovie) => (year,
        ratingsByMovie.map { case (movieID, ratings) => (movieID, ratings.size) }
          .reduce((a, b) =>
            if (a._2 == b._2)
              if (a._1 > b._1) a else b
            else if (a._2 > b._2) a else b)
      )
    }.map { case (year, (id, _)) => (id, year) }
    .join(titlesGroupedById)
    .map { case (_, (year, values)) => (year, values.head._3) }

  // Note: if two genre has the same number of rating, return the first one based on lexicographical sorting on genre.
  def getMostAndLeastRatedGenreAllTime: ((String, Int), (String, Int)) = {
    val tmp = ratingsGroupedByYearByTitle
    .map {
      case (year, ratingsByMovie) => (year,
        ratingsByMovie.map { case (movieID, ratings) => (movieID, ratings.size) }
          .reduce((a, b) =>
            if (a._2 == b._2)
              if (a._1 > b._1) a else b
            else if (a._2 > b._2) a else b)
      )
    }.map { case (year, (id, _)) => (id, year) }
    .join(titlesGroupedById)
    .map { case (_, (year, values)) => values.head._3 }
    .flatMap(identity).groupBy(identity).mapValues(_.size)

    val max = tmp.reduce( (a,b) =>
      if(a._2==b._2)
        if(a._1 > b._1) a else b
      else if (a._2>b._2) a else b
    )
    val min = tmp.reduce((a, b) =>
      if (a._2 == b._2)
        if (a._1 < b._1) a else b
      else if (a._2 < b._2) a else b
    )
    (min,max)
  }

  /**
   * Filter the movies RDD having the required genres
   *
   * @param movies         RDD of movies dataset
   * @param requiredGenres RDD of genres to filter movies
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre(movies: RDD[(Int, String, List[String])],
                          requiredGenres: RDD[String]): RDD[String] =
    movies.filter(_._3.contains(requiredGenres)).map(_._2)

  /**
   * Filter the movies RDD having the required genres
   * HINT: use the broadcast callback to broadcast requiresGenres to all Spark executors
   *
   * @param movies            RDD of movies dataset
   * @param requiredGenres    List of genres to filter movies
   * @param broadcastCallback Callback function to broadcast variables to all Spark executors
   *                          (https://spark.apache.org/docs/2.4.8/rdd-programming-guide.html#broadcast-variables)
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre_usingBroadcast(movies: RDD[(Int, String, List[String])],
                                         requiredGenres: List[String],
                                         broadcastCallback: List[String] => Broadcast[List[String]]): RDD[String] = {
    val readOnly = broadcastCallback(requiredGenres)
    movies.filter(_._3.contains(readOnly.value)).map(_._2)

  }



}

