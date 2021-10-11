import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object PageRank {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Usage: InputLocation Iterations OutputLocation")
    }
    //fetching spark config objects and sql contexts
    val conf = new SparkConf().setAppName("PageRank")
    val sqlContext = new SparkSession.Builder()
      .config(conf)
      .getOrCreate()

    import sqlContext.implicits._
    // fetching input arguments
    val iterationCount = args(1).toInt
    val inLoc = args(0)
    val outLoc = args(2)

    //loading the input file
    val input = sqlContext.read.option("header",true).csv(inLoc)
    val df = input.drop("ORIGIN_CITY_NAME", "DEST_CITY_NAME")
    val routes = df.map(x => (x.getString(0), x.getString(1))).rdd
    val airports = routes.distinct.groupByKey()

    //initializing the rank
    var ranks = airports.mapValues(r => 10.0)
    val total = airports.count()
    //setting alpha value
    val alpha = 0.15
    // iterating
    for( iter <- 1 to iterationCount) {
      println("Iteration " + iter)
      var tmpRanks = airports.join(ranks).values.flatMap {case (stations, currRank) =>
        stations.map(airport => (airport, currRank/stations.size))
      }
      //applying page rank formula
      ranks = tmpRanks.reduceByKey(_ + _).mapValues(a => (alpha/total) + (1-alpha)*a)

    }
    //sorting ranks into descending order and saving into textfile
    ranks.sortBy(_._2, false).saveAsTextFile(outLoc)
  }
}