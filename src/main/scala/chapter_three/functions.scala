import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "./README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))



    val rawUserArtistData = sc.textFile("./profiledata_06-May-2005/user_artist_data.txt")

    val rawArtistData = sc.textFile("./profiledata_06-May-2005/artist_data.txt")

    val rawArtistAlias = sc.textFile("./profiledata_06-May-2005/artist_alias.txt")

    val artistById = rawArtistData.flatMap { line =>
      val (id, name) = line.span(_ != '\t')
      if (name.isEmpty) {
        None
      }  else {
        try {
          Some((id.toInt, name.trim))
        } catch {
          case e: NumberFormatException => None
        }
      }
    }

    val artistAlias = rawArtistAlias.flatMap { line =>
      val tokens = line.split('\t')
      if (tokens(0).isEmpty) {
        None
      } else {
        Some((tokens(0).toInt, tokens(1).toInt))
      }
    }.collectAsMap()

    import org.apache.spark.mllib.recommendation._
    import org.apache.spark.mllib.recommendation.Rating



    val bArtistAlias = sc.broadcast(artistAlias)

    val trainData = rawUserArtistData.map { line =>
      val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
      val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
      Rating(userID, finalArtistID, count)
    }.cache()

    val model = ALS.trainImplicit(trainData, 10, 5, .01, 1.0)

    model.userFeatures.mapValues(x => x.mkString(", ")).first()

    val rawArtistsForUser = rawUserArtistData.map(_.split(' ')).
      filter { case Array(user,_,_) => user.toInt == 2093760}.collect().toSet

    val existingProducts = rawArtistsForUser.map{case Array(_, artist, _) => artist.toInt}.toSet

    artistById.filter { case (id, name) => existingProducts.contains(id)}.values.collect().foreach(println)

    val recommendations = model.recommendProducts(2093760, 5)
    recommendations.foreach(println)

    val recommendedProductIds = recommendations.map(p => p.product).toSet

    artistById.filter { case (id, name) => recommendedProductIds.contains(id)}.values.collect().foreach(println)
  }
}
