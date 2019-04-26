package analysis

import java.util
import java.util.logging.{Level, Logger}

import org.apache.kafka.clients.consumer
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, OffsetCommitCallback}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.mllib.classification.StreamingLogisticRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.json.simple.JSONObject
import org.json.simple.parser._


object KafkaMeetupStreamTrainingAndPrediction {

  val KafkaBrokers = "localhost:9092"
  val KafkaGroup = "meetupGroup"
  val KafkaTopic = "meetupTopic"
  val KafkaOffsetResetType = "latest"
  val MongoOutputURI = "mongodb://localhost:27017/meetupDB.rsvps"
  val predictionValuesOutput = "/home/robertsteele/GradleProjects/StreamingApplication/" +
    "analysis_tier/src/main/resources/results"
  val CaseSensitive = "false"
  val TimeInterval = 5000

  val topics = Array(KafkaTopic)

  val kafkaParams = Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> KafkaBrokers,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.GROUP_ID_CONFIG -> KafkaGroup,
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> KafkaOffsetResetType,
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean))

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("KafkaMeetupStreamTrainingAndPrediction")
      .set("spark.sql.caseSensitive", CaseSensitive)

    val streamingContext = new StreamingContext(conf, new Duration(TimeInterval))

    val dataStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val streamDataValues = dataStream.map { consumerRecord =>
      consumerRecord.value()
    }

    val trainingData = streamDataValues.map { value =>

      val jsonParser = new JSONParser()
      val jsonValue = jsonParser.parse(value).asInstanceOf[JSONObject]

      val stringValue = {
        val responseString =
          if (String.valueOf(jsonValue.get("response")).equals("yes")) "1.0,[" else "0.0,["
        "(" +
          responseString + jsonValue.get("group").asInstanceOf[JSONObject].get("group_lat") + "," +
          jsonValue.get("group").asInstanceOf[JSONObject].get("group_lon") + "])"
      }
      stringValue
    }

    trainingData.print()

    val labeledPoints = trainingData.map(data => LabeledPoint.parse(data))

    val streamingLogisticRegressionWithSGD = new StreamingLogisticRegressionWithSGD()
      .setInitialWeights(Vectors.zeros(2))

    streamingLogisticRegressionWithSGD.trainOn(labeledPoints)


    val labelWithFeatures = labeledPoints.map { labeledPoints =>
      (labeledPoints.label, labeledPoints.features)

    }

    dataStream.foreachRDD { meetupRecord =>
      val offsetRanges = meetupRecord.asInstanceOf[HasOffsetRanges].offsetRanges
      dataStream.asInstanceOf[CanCommitOffsets]
        .commitAsync(offsetRanges, new RSVPsOffsetCommitCallback)
    }

    streamingLogisticRegressionWithSGD.predictOnValues(labelWithFeatures)
      .saveAsTextFiles(predictionValuesOutput)
    streamingContext.start()
    streamingContext.awaitTermination()
  }

  object RSVPsOffsetCommitCallback {
    private val log = Logger.getLogger(classOf[RSVPsOffsetCommitCallback].toString)
  }

  class RSVPsOffsetCommitCallback extends OffsetCommitCallback {
    override def onComplete(offsets: util.Map[TopicPartition, consumer.OffsetAndMetadata], exception: Exception): Unit = {
      RSVPsOffsetCommitCallback.log.info("---------------------------------------------------")
      RSVPsOffsetCommitCallback.log.log(Level.INFO, "{0} | {1}", Array[AnyRef](offsets, exception))
      RSVPsOffsetCommitCallback.log.info("---------------------------------------------------")
    }
  }

}