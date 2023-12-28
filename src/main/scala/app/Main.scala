package app


import org.apache.spark.{SparkConf, SparkContext}


object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)




    }




}
