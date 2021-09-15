package spark

import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}

object OccupationRecommendation {

  case class Occupation(industry_id:Int, occupation_id:Int,skills_id:Int )
  case class OccNames(occupation_id:Int,occupation: String )
  case class OccPairs( occupation1: Int, occupation2: Int, skill1:Int, skill2:Int)
  case class OccPairsSim(occupation1: Int, occupation2: Int, score: Double,numPairs:Long)


  def computeCosineSimilarity(spark: SparkSession, data:  Dataset[OccPairs]): Dataset[OccPairsSim] = {
    // Compute xx, xy and yy columns
    val pairScores = data

      .withColumn("xx", col("skill1") * col("skill1"))
      .withColumn("yy", col("skill2") * col("skill2"))
      .withColumn("xy", col("skill1") * col("skill2"))

    // Compute numerator, denominator and numPairs columns
    val calculateSimilarity = pairScores
      .groupBy("occupation1","occupation2")
      .agg(
        sum(col("xy")).alias("numerator"),
        (sqrt(sum(col("xx"))) * sqrt(sum(col("yy")))).alias("denominator"),
        count(col("xy")).alias("numPairs")
      )

    // Calculate score and select only needed columns (movie1, movie2, score, numPairs)
    import spark.implicits._
    val result = calculateSimilarity
      .withColumn("score",
        when(col("denominator") =!= 0, col("numerator") / col("denominator"))
          .otherwise(null)
      ).select("occupation1","occupation2","score","numPairs").as[OccPairsSim]

    result
  }
  def getOccNames(occNames:  Dataset[OccNames], occupationID: Int): String = {
    val result= occNames.filter(col("occupation_id")=== occupationID)
      .select("occupation").collect()(0)

    result(0).toString
  }

  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("MovieSimilarities")
      .master("local[*]")
      .getOrCreate()

    // Create schemas

    val OccNamesSchema = new StructType()
      .add("occupation_id",IntegerType,nullable = true)
      .add("occupation",StringType,nullable = true)

    val OccupationSchema = new StructType()
      .add("industry_id",IntegerType,nullable = true)
      .add("occupation_id",IntegerType,nullable = true)
      .add("skills_id",IntegerType,nullable = true)

    println("\nLoading Occupation Names...")

    import spark.implicits._
    // Load up movie data as dataset
    val occNames=spark.read
      .option("sep",",")
      .schema(OccNamesSchema)
      .csv("C:\\Adith\\MIS 730 A\\Assignment 3 Datasets\\Dataset1Test.csv")
      .as[OccNames]

    val occupations= spark.read
      .option("sep",",")
      .schema(OccupationSchema)
      .csv("C:\\Adith\\MIS 730 A\\Assignment 3 Datasets\\FinalTest.csv")
      .as[Occupation]

    val occPairs = occupations.as("occ1")
      .join(occupations.as("occ2"),$"occ1.industry_id" ===$"occ2.industry_id" && $"occ1.occupation_id"< $"occ2.occupation_id" )
      .select($"occ1.occupation_id".as("occupation1"),
        $"occ2.occupation_id".as("occupation2"),
        $"occ1.skills_id".as("skill1"),
        $"occ2.skills_id".as("skill2")
      ).as[OccPairs]

    val occPairsSim = computeCosineSimilarity(spark,occPairs ).cache()

    if (args.length > 0) {
      val scoreThreshold = 0.97
      val OccurenceThreshold = 50

      val occupationID : Int= args (0).toInt

      val filteredResults= occPairsSim.filter(
        (col("occupation1")===occupationID || col("occupation2")=== occupationID )
          && col("score")> scoreThreshold && col("numPairs")>OccurenceThreshold )

      val results = filteredResults.sort(col("score").desc).take(3)

      println("\nTop 3 Similar Occupations for " + getOccNames(occNames, occupationID))

      for (item<-results){
        var SimilarOccId = item.occupation1
        if(SimilarOccId==occupationID){
          SimilarOccId= item.occupation2
        }
        //val formattedTemp = f"$temp%.2f F"
        val formattedScore= f"${item.score}%.2f"
        println(getOccNames(occNames, SimilarOccId) + ",\tScore: " + formattedScore + "\tStrength: " + item.numPairs)
      }
    }
  }
}
// Web Development - 764 ; Information Management - 2549 ; SDLC - 2130 ;