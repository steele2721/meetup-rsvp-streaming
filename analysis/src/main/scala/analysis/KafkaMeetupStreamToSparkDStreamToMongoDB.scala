package analysis

import java.util
import java.util.logging.{Level, Logger}

import com.mongodb.spark.MongoSpark
import org.apache.kafka.clients.consumer
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, OffsetCommitCallback}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.bson.Document

object KafkaMeetupStreamToSparkDStreamToMongoDB {

  val KafkaBrokers = "localhost:9092"
  val KafkaGroup = "meetupGroup"
  val KafkaTopic = "meetupTopic"
  val KafkaOffsetResetType = "latest"
  val MongoOutputURI = "mongodb://localhost:27017/meetupDB.rsvps"

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
      .setAppName("StreamingApp")
      .setMaster("local[*]")
      .set("spark.mongodb.output.uri", MongoOutputURI)

    val streamingContext = new StreamingContext(conf, new Duration(5000))

    val dataStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    dataStream.foreachRDD { consumerRecord =>
      MongoSpark.save(
        consumerRecord.map(record =>
          Document.parse(record.value())))
    }

    dataStream.foreachRDD { meetupRecord =>
      val offsetRanges = meetupRecord.asInstanceOf[HasOffsetRanges].offsetRanges
      dataStream.asInstanceOf[CanCommitOffsets]
        .commitAsync(offsetRanges, new RSVPsOffsetCommitCallback)
    }

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
