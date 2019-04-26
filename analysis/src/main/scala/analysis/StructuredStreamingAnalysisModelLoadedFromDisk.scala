package analysis

import java.util.concurrent.TimeUnit.MILLISECONDS

import org.apache.spark.SparkConf
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.ProcessingTime

import scala.concurrent.duration.Duration


object StructuredStreamingAnalysisModelLoadedFromDisk {

  val KafkaStreamingFormat = "kafka"
  val KafkaBrokers = "localhost:9092"
  val MeetupTopic = "meetupTopic"

  val RegressionModelURI = "/home/robertsteele/GradleProjects/" +
    "StreamingApplication/analysis_tier/src/main/resources/" +
    "LogisticRegressionModels.model"
  val ModelPredictionsOutputDir = "/home/robertsteele/GradleProjects/" +
    "StreamingApplication/analysis_tier/src/main/resources/results"

  def main(agrs: Array[String]) {
    val conf = new SparkConf()
      .setAppName("StreamingApp")
      .setMaster("local[*]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val pipelineModel = PipelineModel.load(RegressionModelURI)

    val meetupStreamDF = spark.readStream
      .format(KafkaStreamingFormat)
      .option("kafka.bootstrap.servers", KafkaBrokers)
      .option("subscribe", MeetupTopic)
      .option("startingOffsets", "latest")
      .load()

    val meetupStreamStringDF = meetupStreamDF.select(
      from_json(col("value").cast("string"), RSVPmodel.MeetupSchema)
        .alias("rsvp")).select("rsvp.*")

    meetupStreamStringDF.printSchema()

    val filteredDF = meetupStreamStringDF.filter(!_.anyNull)

    val featuresDF = filteredDF.select("group.group_city",
      "group.group_lat", "group.group_lon",
      "response")

    featuresDF.printSchema()

    val predictionsDF = pipelineModel.transform(featuresDF)

    predictionsDF.printSchema()

    val query = predictionsDF.writeStream
      .format("json")
      //.format("console")
      .option("path",ModelPredictionsOutputDir)
      .trigger(ProcessingTime(Duration(3000, MILLISECONDS)))
      .option("truncate", false)
      .option("checkpointLocation", ModelPredictionsOutputDir)
      .start()

    query.awaitTermination()

  }

}