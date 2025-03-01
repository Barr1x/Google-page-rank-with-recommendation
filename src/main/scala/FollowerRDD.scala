import org.apache.spark.SparkContext

object FollowerRDD {

  /**
    * This function should first read the graph located at the input path, it should compute the
    * follower count, and save the top 100 users to the output path with userID and
    * count **tab separated**.
    *
    * It must be done using the RDD API.
    *
    * @param inputPath the path to the graph.
    * @param outputPath the output path.
    * @param sc the SparkContext.
    */
  def computeFollowerCountRDD(inputPath: String, outputPath: String, sc: SparkContext): Unit = {
    // TODO: Calculate the follower count for each user
    // TODO: Write the top 100 users to the outputPath with userID and count **tab separated**
    val graphRDD = sc.textFile(inputPath)
    val edgeRDD = graphRDD.map(line => {
      val fields = line.split("\t")
      (fields(0), fields(1))
    }
    )
    val uniqueEdgeRDD = edgeRDD.distinct()
    val top100 = uniqueEdgeRDD.map(pair => (pair._2, 1)).reduceByKey(_+_).sortBy(_._2, ascending = false).take(100)
    val top100Output = top100.map(pair => pair._1 + "\t" + pair._2)
    sc.parallelize(top100Output).saveAsTextFile(outputPath)
  }

  /**
    * @param args it should be called with two arguments, the input path, and the output path.
    */
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.sparkSession()
    val sc = spark.sparkContext

    val inputGraph = args(0)
    val followerRDDOutputPath = args(1)

    computeFollowerCountRDD(inputGraph, followerRDDOutputPath, sc)
  }
}
