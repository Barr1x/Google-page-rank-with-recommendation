import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.functions.{array, array_max, coalesce, col, collect_list, concat, explode_outer, expr, greatest, lit, max, size, sum, when}

object PageRank {

  // Do not modify
  val PageRankIterations = 10

  /**
    * Input graph is a plain text file of the following format:
    *
    *   follower  followee
    *   follower  followee
    *   follower  followee
    *   ...
    *
    * where the follower and followee are separated by `\t`.
    *
    * After calculating the page ranks of all the nodes in the graph,
    * the output should be written to `outputPath` in the following format:
    *
    *   node  rank
    *   node  rank
    *   node  rank
    *
    * where node and rank are separated by `\t`.
    *
    * @param inputGraphPath path of the input graph.
    * @param outputPath path of the output of page rank.
    * @param iterations number of iterations to run on the PageRank.
    * @param spark the SparkSession.
    */
  def calculatePageRank(
      inputGraphPath: String,
      graphTopicsPath: String,
      pageRankOutputPath: String,
      recsOutputPath: String,
      iterations: Int,
      spark: SparkSession): Unit = {
    val sc = spark.sparkContext

    // TODO - Your code here
    val schema = StructType(Array(
      StructField("follower", LongType),
      StructField("followee", LongType)
    ))
    val df = spark.read.option("delimiter", "\t").schema(schema).csv(inputGraphPath)
    val schema2 = StructType(Array(
      StructField("user", LongType),
      StructField("games", DoubleType),
      StructField("movies", DoubleType),
      StructField("music", DoubleType)
    ))
    val df_topics = spark.read.option("header", "true").option("delimiter", "\t").schema(schema2).csv(graphTopicsPath)

    val num_vertex = df_topics.count().toDouble

    val df_following = df.groupBy("follower")
      .agg(collect_list("followee") as "followees")
      .withColumn("numfollowing", size(col("followees")))

    val df_following_repart = df_following.repartition(col("follower"))
    val df_topics_repart = df_topics.repartition(col("user"))
    val statics = df_following_repart.join(df_topics_repart, df_following_repart("follower") === df_topics_repart("user"), "right")

    val init_rank_value = 1.0 / num_vertex
    var vars = df_topics
      .withColumn("rank", lit(init_rank_value))

      .withColumn("games_rec", array(df_topics("games"), df_topics("user")))
      .withColumn("movies_rec", array(df_topics("movies"), df_topics("user")))
      .withColumn("music_rec", array(df_topics("music"), df_topics("user")))

      .select("user", "rank", "games_rec", "movies_rec", "music_rec")

    for (i <- 1 to 10){
      val statics_part = statics.repartition(col("user"))
      val vars_part = vars.repartition(col("user"))
      val joined = statics_part.join(vars_part, "user")

      val contribs = joined
        .withColumn("followee", explode_outer(concat(col("followees"), array(col("user")))))
        .withColumn("contrib", when(col("user") !== col("followee"), joined("rank") / joined("numfollowing"))
          .otherwise(0))

        .withColumn("games_rec", expr("IF(games < 3, array(0, games_rec[1]), games_rec)"))

        .withColumn("movies_rec", expr("IF(movies < 3, array(0, movies_rec[1]), movies_rec)"))

        .withColumn("music_rec", expr("IF(music < 3, array(0, music_rec[1]), music_rec)"))

        // keep selected values
        // * replace null followee fields with user (from explode_outer on rows with null followee list)
        .select(col("user"), coalesce(col("followee"), col("user")).alias("followee"), col("contrib"), col("games_rec"), col("movies_rec"), col("music_rec"))

        // replace null rank contributions with 0 so that they sum properly
        .na.fill(0, Seq("contrib"))

      val total_dangling_rank = 1.0 - contribs.agg(sum("contrib")).first().getDouble(0)

      vars = contribs.groupBy("followee").agg(
          sum("contrib").as("rank"),
          max("games_rec").as("games_rec"),
          max("movies_rec").as("movies_rec"),
          max("music_rec").as("music_rec")
        )
        .withColumn("rank", col("rank") * lit(0.85) + lit(0.85 * total_dangling_rank + 0.15) / lit(num_vertex))
        .withColumnRenamed("followee", "user")

    }

    val rank_output = vars.select("user", "rank")
    rank_output.write.option("delimiter", "\t").option("header", "false").csv(pageRankOutputPath)

    val joined_output = df_topics.join(vars, "user")
    val recommender_output = joined_output
      .withColumn("games_rec", expr("IF(games < 3, array(0, games_rec[1]), games_rec)"))

      .withColumn("movies_rec", expr("IF(movies < 3, array(0, movies_rec[1]), movies_rec)"))

      .withColumn("music_rec", expr("IF(music < 3, array(0, music_rec[1]), music_rec)"))
      .withColumn("max_rec", greatest(col("games_rec"), col("movies_rec"), col("music_rec")))
      .withColumn("recommended_user_id", col("max_rec")(1).cast("int"))
      .withColumn("post_frequency", col("max_rec")(0))
      .select("user", "recommended_user_id", "post_frequency")

    recommender_output.write
      .option("delimiter", "\t")
      .option("header", "false")
      .csv(recsOutputPath)
  }


  /**
    * @param args it should be called with two arguments, the input path, and the output path.
    */
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.sparkSession()

    val inputGraph = args(0)
    val graphTopics = args(1)
    val pageRankOutputPath = args(2)
    val recsOutputPath = args(3)

    calculatePageRank(inputGraph, graphTopics, pageRankOutputPath, recsOutputPath, PageRankIterations, spark)
  }
}
