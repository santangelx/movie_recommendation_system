package app.recommender.collaborativeFiltering


import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

class CollaborativeFiltering(rank: Int,
                             regularizationParameter: Double,
                             seed: Long,
                             n_parallel: Int) extends Serializable {

  // NOTE: set the parameters according to the project description to get reproducible (deterministic) results.
  private val maxIterations = 20
  private var model : MatrixFactorizationModel = _
  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {
    val als = new ALS()
    als.setRank(rank)
    als.setLambda(regularizationParameter)
    als.setSeed(seed)
    als.setBlocks(n_parallel)
    als.setIterations(maxIterations)
    // Model is ready to use
    model = als.run(ratingsRDD.map{case (uid,mid,_,r,_) => Rating(uid,mid,r)})
  }

  def predict(userId: Int, movieId: Int): Double = model.predict(userId,movieId)

}
